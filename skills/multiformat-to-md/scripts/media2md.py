#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import List, Optional

import requests

from multiformat_gateway import (
    MEDIA_EXTENSIONS,
    ProgressCallback,
    ProgressPrinter,
    _make_progress_callback,
    build_markdown,
    resolve_inputs,
    write_output,
)

_SENTENCE_SPLIT_RE = re.compile(r"(?<=[。！？!?])")


def _get_media_duration_seconds(path: Path) -> Optional[float]:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(path),
    ]
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, encoding="utf-8", errors="ignore")
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    raw = (result.stdout or "").strip()
    try:
        duration = float(raw)
        return duration if duration > 0 else None
    except Exception:
        return None


def _faster_whisper_available() -> bool:
    try:
        import faster_whisper  # type: ignore  # noqa: F401
        return True
    except Exception:
        return False


def _select_whisper_engine() -> str:
    engine = (os.getenv("WHISPER_ENGINE") or "").strip().lower()
    if engine in {"faster", "faster-whisper"}:
        return "faster"
    if engine in {"openai", "openai-whisper", "whisper"}:
        return "openai"
    return "faster" if _faster_whisper_available() else "openai"


def _torch_cuda_available() -> bool:
    try:
        import torch  # type: ignore
        return bool(torch.cuda.is_available())
    except Exception:
        return False


def _transcribe_with_faster_whisper(
    path: Path,
    model_name: str = "base",
    progress_callback: Optional[ProgressCallback] = None,
    duration_seconds: Optional[float] = None,
) -> str:
    try:
        from faster_whisper import WhisperModel  # type: ignore
    except Exception as exc:
        raise RuntimeError("Missing dependency: faster-whisper") from exc

    device = "cuda" if _torch_cuda_available() else "cpu"
    compute_type = "float16" if device == "cuda" else "int8"
    model = WhisperModel(model_name, device=device, compute_type=compute_type)
    segments, _ = model.transcribe(str(path))

    texts = []
    last_report = -1
    for seg in segments:
        if seg.text:
            texts.append(seg.text)
        if progress_callback and duration_seconds:
            current = min(duration_seconds, float(getattr(seg, "end", 0.0)))
            percent = int((current / duration_seconds) * 100)
            if percent != last_report:
                last_report = percent
                progress_callback(current, duration_seconds, "Transcribing")
    return "".join(texts).strip()


def _transcribe_with_openai_whisper(
    path: Path,
    model_name: str = "base",
    progress_callback: Optional[ProgressCallback] = None,
) -> str:
    try:
        import whisper  # type: ignore
    except Exception as exc:
        raise RuntimeError("Missing dependency: openai-whisper") from exc

    if progress_callback:
        progress_callback(0.1, 1.0, "Transcribing")
    model = whisper.load_model(model_name)
    result = model.transcribe(str(path), fp16=_torch_cuda_available())
    if progress_callback:
        progress_callback(1.0, 1.0, "Transcription done")
    return (result.get("text") or "").strip()


def transcribe_audio(
    path: Path,
    model_name: str = "base",
    progress_callback: Optional[ProgressCallback] = None,
    duration_seconds: Optional[float] = None,
) -> str:
    engine = _select_whisper_engine()
    if engine == "faster":
        return _transcribe_with_faster_whisper(
            path,
            model_name=model_name,
            progress_callback=progress_callback,
            duration_seconds=duration_seconds,
        )
    return _transcribe_with_openai_whisper(path, model_name=model_name, progress_callback=progress_callback)


def transcribe_video(
    path: Path,
    model_name: str = "base",
    progress_callback: Optional[ProgressCallback] = None,
) -> str:
    duration = _get_media_duration_seconds(path)
    with tempfile.TemporaryDirectory() as tmp:
        audio_path = Path(tmp) / "audio.wav"
        cmd = ["ffmpeg", "-y", "-i", str(path), "-vn", "-ac", "1", "-ar", "16000", str(audio_path)]
        try:
            if progress_callback:
                progress_callback(0.01, 1.0, "Extracting audio")
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except FileNotFoundError as exc:
            raise RuntimeError("Missing system tool: ffmpeg (for .mp4)") from exc
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(f"ffmpeg failed: {(exc.stderr or '').strip()}") from exc
        return transcribe_audio(audio_path, model_name=model_name, progress_callback=progress_callback, duration_seconds=duration)


def convert_media(path: Path, whisper_model: str, progress_callback: Optional[ProgressCallback] = None) -> str:
    ext = path.suffix.lower()
    if ext == ".mp3":
        duration = _get_media_duration_seconds(path)
        return transcribe_audio(path, model_name=whisper_model, progress_callback=progress_callback, duration_seconds=duration)
    if ext == ".mp4":
        return transcribe_video(path, model_name=whisper_model, progress_callback=progress_callback)
    raise RuntimeError(f"Unsupported extension for media converter: {ext}")


def _load_env_files() -> None:
    base_dir = Path(__file__).resolve().parents[3]
    for filename in (".env", "env"):
        env_path = base_dir / filename
        if not env_path.exists() or not env_path.is_file():
            continue
        try:
            lines = env_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            continue
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if key.startswith("export "):
                key = key.replace("export ", "", 1).strip()
            if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
                value = value[1:-1]
            if key and key not in os.environ:
                os.environ[key] = value


def _parse_frontmatter(text: str) -> tuple[str, str]:
    if text.startswith("---"):
        parts = text.split("---", 2)
        if len(parts) >= 3:
            front = "---" + parts[1] + "---"
            rest = parts[2]
            return front.strip(), rest.lstrip("\n")
    return "", text


def _extract_content(body: str) -> tuple[str, str]:
    marker = "\n## Content\n"
    if marker in body:
        pre, content = body.split(marker, 1)
        return pre.rstrip(), content.lstrip("\n")
    return body.rstrip(), ""


def _split_paragraphs(content: str) -> List[str]:
    content = re.sub(r"\n{3,}", "\n\n", content.strip())
    return [] if not content else content.split("\n\n")


def _split_long_paragraph(text: str, max_len: int) -> List[str]:
    text = (text or "").strip()
    if not text:
        return []
    if max_len <= 0 or len(text) <= max_len:
        return [text]

    def _hard_split(unit: str) -> List[str]:
        unit = unit.strip()
        return [] if not unit else [unit[i : i + max_len] for i in range(0, len(unit), max_len)]

    sentences = [s for s in _SENTENCE_SPLIT_RE.split(text) if s and s.strip()]
    if len(sentences) <= 1:
        return _hard_split(text)

    chunks: List[str] = []
    buf = ""
    for s in sentences:
        s = s.strip()
        if len(s) > max_len:
            if buf:
                chunks.append(buf)
                buf = ""
            chunks.extend(_hard_split(s))
            continue
        if len(buf) + len(s) <= max_len:
            buf += s
        else:
            if buf:
                chunks.append(buf)
            buf = s
    if buf:
        chunks.append(buf)
    return chunks


def _format_readable_lines(text: str) -> str:
    text = (text or "").replace("\r\n", "\n").strip()
    if not text:
        return ""
    # Keep chunk/paragraph readability by inserting line breaks after sentence-ending punctuation.
    text = re.sub(r"([。！？!?；;])(?=[^\n])", r"\1\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _build_rewrite_prompt(paragraph: str) -> str:
    return (
        "下面是一段音视频转写文本，请在不丢失信息的前提下做文字整理。\n"
        "要求：去口语化、去重复、不删减事实与观点、可调整句式和标点。"
        "只输出重写后的文本。\n\n待重写段落：\n" + paragraph.strip()
    )


def _build_video_summary_prompt(full_markdown: str) -> str:
    return (
        "下面给你一整篇 Markdown 文本（可能来自视频转写或文档抽取），请基于全文输出中文总结。\n"
        "要求：\n"
        "1) 先给出 5-10 行核心结论要点；\n"
        "2) 再给出结构化分节总结（按主题归纳）；\n"
        "3) 最后给出“可执行建议/行动项”；\n"
        "4) 不要编造原文没有的信息，结论需可追溯到原文。\n"
        "只输出总结正文，不要输出额外说明。\n\n"
        "原始 Markdown 全文如下：\n"
        f"{full_markdown.strip()}"
    )


def _resolve_llm_config(default_model: str, model_env_vars: Optional[List[str]] = None) -> tuple[str, str, str, int]:
    _load_env_files()
    api_key = (os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY / LLM_API_KEY is not configured.")

    candidate_models: List[str] = []
    for env_name in model_env_vars or []:
        value = (os.getenv(env_name) or "").strip()
        if value:
            candidate_models.append(value)
    candidate_models.append((os.getenv("OPENAI_MODEL") or "").strip())
    candidate_models.append(default_model)
    model = next((m for m in candidate_models if m), default_model)

    base_url = (os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").strip()
    timeout = int(os.getenv("OPENAI_TIMEOUT_SECONDS", "180"))
    return api_key, model, base_url, timeout


def _call_llm(messages: List[dict], model: str, base_url: str, api_key: str, timeout: int) -> str:
    response = requests.post(
        f"{base_url.rstrip('/')}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"model": model, "messages": messages, "temperature": 0.2},
        timeout=timeout,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"LLM interface error {response.status_code}: {response.text[:400]}")
    data = response.json()
    choices = data.get("choices") or []
    if not choices:
        raise RuntimeError("LLM returned empty choices.")
    message = (choices[0] or {}).get("message") or {}
    content = message.get("content", "")
    if isinstance(content, list):
        parts = [(item.get("text") if isinstance(item, dict) else str(item)) for item in content]
        content = "".join(parts)
    return (str(content) or "").strip()


def rewrite_markdown_with_llm(input_path: Path, output_path: Path, max_chunk_len: int = 1000, sleep_ms: int = 0) -> None:
    api_key, model, base_url, timeout = _resolve_llm_config(
        default_model="gpt-4o-mini",
        model_env_vars=["OPENAI_REWRITE_MODEL"],
    )

    raw = input_path.read_text(encoding="utf-8", errors="ignore")
    front, rest = _parse_frontmatter(raw)
    pre, content = _extract_content(rest)

    rewritten_parts: List[str] = []
    for para in _split_paragraphs(content):
        chunks = _split_long_paragraph(para, max_chunk_len)
        rewritten_chunks: List[str] = []
        for chunk in chunks:
            rewritten = _call_llm(
                messages=[
                    {"role": "system", "content": "你是一个细心的中文文本纠正员，请在不丢失信息的前提下做文字整理。请用简体中文回答。"},
                    {"role": "user", "content": _build_rewrite_prompt(chunk)},
                ],
                model=model,
                base_url=base_url,
                api_key=api_key,
                timeout=timeout,
            )
            rewritten_chunks.append(rewritten)
            if sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)
        merged_chunks = [_format_readable_lines(item) for item in rewritten_chunks if (item or "").strip()]
        merged = "\n\n".join(merged_chunks).strip()
        rewritten_parts.append(merged if merged else _format_readable_lines(para.strip()))

    new_content = "\n\n".join([p for p in rewritten_parts if p]).strip()
    src_len = len(content.strip())
    dst_len = len(new_content)
    min_ratio = float(os.getenv("AUDIO_REWRITE_MIN_RATIO", "0.85"))
    if src_len > 0 and (dst_len / src_len) < min_ratio:
        new_content = content.strip()
    new_content += "\n"

    output_text = (front + "\n" if front else "") + pre.rstrip() + "\n## Content\n" + new_content
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output_text, encoding="utf-8")


def summarize_video_markdown_with_llm(input_path: Path, output_path: Path) -> None:
    api_key, model, base_url, timeout = _resolve_llm_config(
        default_model="dsv3.2",
        model_env_vars=["OPENAI_VIDEO_SUMMARY_MODEL", "VIDEO_SUMMARY_MODEL"],
    )

    raw = input_path.read_text(encoding="utf-8", errors="ignore")
    front, rest = _parse_frontmatter(raw)
    pre, content = _extract_content(rest)
    if not content.strip():
        raise RuntimeError("Input markdown has empty content, skipped video summary.")

    summary = _call_llm(
        messages=[
            {"role": "system", "content": "你是一个严谨的中文内容总结助手，请只基于用户提供的全文进行总结。"},
            {"role": "user", "content": _build_video_summary_prompt(raw)},
        ],
        model=model,
        base_url=base_url,
        api_key=api_key,
        timeout=timeout,
    )
    summary = _format_readable_lines(summary) or summary.strip()

    prefix = pre.rstrip() if pre.strip() else f"# {input_path.name}"
    output_text = (
        (front + "\n" if front else "")
        + prefix
        + "\n## AI Summary\n"
        + summary.strip()
        + "\n"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output_text, encoding="utf-8")


def maybe_rewrite_media_markdown(output_markdown: Path, progress_callback: Optional[ProgressCallback] = None) -> None:
    raw_backup_path = output_markdown.with_name(f"{output_markdown.stem}__raw.md")
    try:
        shutil.copyfile(output_markdown, raw_backup_path)
    except Exception:
        pass

    if progress_callback:
        progress_callback(95, 100, "LLM rewriting")
    rewrite_markdown_with_llm(output_markdown, output_markdown, max_chunk_len=800, sleep_ms=0)


def maybe_generate_video_summary_markdown(
    output_markdown: Path,
    progress_callback: Optional[ProgressCallback] = None,
) -> Path:
    if progress_callback:
        progress_callback(98, 100, "LLM summarizing")
    summary_path = output_markdown.with_name(f"{output_markdown.stem}__ai_summary.md")
    summarize_video_markdown_with_llm(output_markdown, summary_path)
    return summary_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert audio/video to Markdown for LLM ingestion.")
    parser.add_argument("--input", nargs="+", required=True, help="Input files and/or directories.")
    parser.add_argument("--output", required=True, help="Output markdown directory.")
    parser.add_argument("--whisper-model", default="base", help="Whisper model for mp3/mp4.")
    parser.add_argument("--rewrite-with-llm", action="store_true", help="Rewrite media transcripts with LLM.")
    args = parser.parse_args()

    files = resolve_inputs(args.input, allowed_exts=MEDIA_EXTENSIONS)
    if not files:
        print("No supported audio/video files found.", file=sys.stderr)
        return 1

    output_dir = Path(args.output).expanduser().resolve()
    failed = 0
    printer = ProgressPrinter(total_files=len(files))

    for f in files:
        printer.start_file(f)
        progress_cb = _make_progress_callback(printer, f"Processing: {f.name}")
        summary_output: Optional[Path] = None
        try:
            content = convert_media(f, whisper_model=args.whisper_model, progress_callback=progress_cb)
            md = build_markdown(f, content=content, status="ok")
        except Exception as exc:
            failed += 1
            md = build_markdown(f, content="", status="failed", error=str(exc))

        printer.update(100, "Writing markdown")
        out = write_output(f, output_dir, md)
        if args.rewrite_with_llm and "status: ok" in md[:1200]:
            try:
                maybe_rewrite_media_markdown(out, progress_callback=progress_cb)
            except Exception as exc:
                failed += 1
                print(f"[rewrite failed] {out}: {exc}", file=sys.stderr)
        if f.suffix.lower() == ".mp4" and "status: ok" in md[:1200]:
            try:
                summary_output = maybe_generate_video_summary_markdown(out, progress_callback=progress_cb)
            except Exception as exc:
                print(f"[video summary failed] {out}: {exc}", file=sys.stderr)

        status = "ok" if "status: ok" in md[:1200] else "failed"
        printer.finish_file(status)
        print(f"[written] {out}")
        if summary_output:
            print(f"[written] {summary_output}")

    print(f"Processed {len(files)} files; failed: {failed}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
