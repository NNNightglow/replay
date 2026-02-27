#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import requests


BASE_DIR = Path(__file__).resolve().parent.parent.parent
SKILL_CONVERTER = (
    BASE_DIR
    / "skills"
    / "multiformat-to-md"
    / "scripts"
    / "convert_to_markdown.py"
)

SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".doc", ".png", ".jpg", ".jpeg", ".mp3", ".mp4"}

ProgressCallback = Callable[[int, int, str], None]

_PROGRESS_PATTERN = re.compile(r"\b(\d{1,3})%\s*(.*)")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[。！？!?；;])")


def _strip_likely_page_headings(text: str) -> str:
    lines = text.splitlines()
    kept = []
    for raw in lines:
        line = raw.strip()
        compact = re.sub(r"\s+", "", line)
        # 过滤类似“## 第 1 页 / ## 1 page / ## <乱码> 1 <乱码>”的短页码标题
        if line.startswith("##") and re.search(r"\d+", line) and len(compact) <= 20:
            continue
        kept.append(raw)
    return "\n".join(kept)


def _parse_markdown_status(markdown_path: Path) -> Tuple[str, str]:
    if not markdown_path.exists():
        return "failed", "Markdown output file not found."

    text = markdown_path.read_text(encoding="utf-8", errors="ignore")
    head = text[:1200]
    status = "ok"
    if "status: failed" in head:
        status = "failed"

    error_message = ""
    marker = "## Error"
    if marker in text:
        after = text.split(marker, 1)[1]
        if "## Content" in after:
            after = after.split("## Content", 1)[0]
        error_message = after.strip()

    return status, error_message


def _load_env_files() -> None:
    for filename in (".env", "env"):
        env_path = BASE_DIR / filename
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


def _parse_frontmatter(text: str) -> Tuple[str, str]:
    if text.startswith("---"):
        parts = text.split("---", 2)
        if len(parts) >= 3:
            front = "---" + parts[1] + "---"
            rest = parts[2]
            return front.strip(), rest.lstrip("\n")
    return "", text


def _extract_content(body: str) -> Tuple[str, str]:
    marker = "\n## Content\n"
    if marker in body:
        pre, content = body.split(marker, 1)
        return pre.rstrip(), content.lstrip("\n")
    return body.rstrip(), ""


def _pdf_markdown_quality_ok(markdown_path: Path, min_chars: int = 20) -> bool:
    text = markdown_path.read_text(encoding="utf-8", errors="ignore")
    _, body = _parse_frontmatter(text)
    _, content = _extract_content(body)
    if not content.strip():
        return False
    without_page_titles = _strip_likely_page_headings(content)
    compact = re.sub(r"\s+", "", without_page_titles)
    meaningful = re.sub(r"[^\u4e00-\u9fffA-Za-z0-9]", "", compact)
    if len(meaningful) < min_chars:
        return False

    # 检查按页覆盖率，防止“仅少数页提取到文本”仍被判定为 ok。
    page_parts = re.split(r"(?m)^##\s*第\s*\d+\s*页\s*$", content)
    if len(page_parts) >= 3:
        page_texts = page_parts[1:]
        total_pages = len(page_texts)
        non_empty_pages = 0
        for page_text in page_texts:
            page_compact = re.sub(r"\s+", "", page_text)
            page_meaningful = re.sub(r"[^\u4e00-\u9fffA-Za-z0-9]", "", page_compact)
            if len(page_meaningful) >= 10:
                non_empty_pages += 1

        if total_pages <= 3:
            coverage_ok = non_empty_pages >= 1
        elif total_pages <= 8:
            coverage_ok = non_empty_pages >= 2 and (non_empty_pages / total_pages) >= 0.25
        else:
            coverage_ok = non_empty_pages >= 3 and (non_empty_pages / total_pages) >= 0.30
        if not coverage_ok:
            return False

    # 防止“提取成功但实为乱码”误判为可用内容。
    if len(compact) >= 80:
        cjk_chars = len(re.findall(r"[\u4e00-\u9fff]", without_page_titles))
        alpha_tokens = re.findall(r"[A-Za-z]+", without_page_titles)
        alpha_chars = sum(len(token) for token in alpha_tokens)
        upper_chars = len(re.findall(r"[A-Z]", without_page_titles))
        cjk_ratio = cjk_chars / max(len(compact), 1)
        short_ratio = (
            sum(1 for token in alpha_tokens if len(token) <= 2) / max(len(alpha_tokens), 1)
            if alpha_tokens
            else 0.0
        )
        upper_ratio = upper_chars / max(alpha_chars, 1) if alpha_chars else 0.0
        if cjk_ratio < 0.05 and alpha_chars >= 120 and upper_ratio >= 0.75:
            return False
        if cjk_ratio < 0.05 and alpha_chars >= 60 and upper_ratio >= 0.58 and short_ratio >= 0.50:
            return False
    return True


def _split_paragraphs(content: str) -> List[str]:
    content = re.sub(r"\n{3,}", "\n\n", content.strip())
    if not content:
        return []
    return content.split("\n\n")


def _split_long_paragraph(text: str, max_len: int) -> List[str]:
    if len(text) <= max_len:
        return [text]
    sentences = [s for s in _SENTENCE_SPLIT_RE.split(text) if s]
    chunks: List[str] = []
    buf = ""
    for s in sentences:
        if len(buf) + len(s) <= max_len:
            buf += s
        else:
            if buf:
                chunks.append(buf)
            buf = s
    if buf:
        chunks.append(buf)
    return chunks


def _build_rewrite_prompt(paragraph: str) -> str:
    rules = (
        "请逐段重写，要求：\n"
        "1) 去口语化。\n"
        "2) 去重复。\n"
        "3) 去无意义、对主旨理解无帮助的内容。\n"
        "4) 不缩短真实内容，不删事实与观点。\n"
        "5) 允许轻微句式调整、断句、补标点。\n"
        "6) 只输出重写后的该段文字，不要额外说明。\n"
    )
    return rules + "\n待重写段落：\n" + paragraph.strip()


def _call_llm(messages: List[dict], model: str, base_url: str, api_key: str, timeout: int) -> str:
    response = requests.post(
        f"{base_url.rstrip('/')}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": messages,
            "temperature": 0.2,
        },
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
        parts = []
        for item in content:
            if isinstance(item, dict):
                parts.append(item.get("text") or "")
            else:
                parts.append(str(item))
        content = "".join(parts)
    return (str(content) or "").strip()


def rewrite_markdown_with_llm(
    input_path: Path,
    output_path: Path,
    max_chunk_len: int = 1000,
    sleep_ms: int = 0,
) -> None:
    _load_env_files()

    api_key = (os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY / LLM_API_KEY is not configured.")
    model = (os.getenv("OPENAI_MODEL") or "deepseek-v3.2").strip()
    base_url = (os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").strip()
    timeout = int(os.getenv("OPENAI_TIMEOUT_SECONDS", "180"))

    raw = input_path.read_text(encoding="utf-8", errors="ignore")
    front, rest = _parse_frontmatter(raw)
    pre, content = _extract_content(rest)

    paragraphs = _split_paragraphs(content)
    rewritten_parts: List[str] = []

    for para in paragraphs:
        chunks = _split_long_paragraph(para, max_chunk_len)
        rewritten_chunks = []
        for chunk in chunks:
            prompt = _build_rewrite_prompt(chunk)
            rewritten = _call_llm(
                messages=[
                    {"role": "system", "content": "你是一个严谨的中文文字整理助手。"},
                    {"role": "user", "content": prompt},
                ],
                model=model,
                base_url=base_url,
                api_key=api_key,
                timeout=timeout,
            )
            rewritten_chunks.append(rewritten)
            if sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)
        merged = "".join(rewritten_chunks).strip()
        merged = re.sub(r"\n{2,}", "\n", merged)
        rewritten_parts.append(merged)

    new_content = "\n".join([p for p in rewritten_parts if p]).strip() + "\n"
    output_text = ""
    if front:
        output_text += front + "\n"
    output_text += pre.rstrip() + "\n## Content\n" + new_content
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output_text, encoding="utf-8")


def convert_file_to_markdown_via_skill(
    input_file: Path,
    output_markdown: Path,
    whisper_model: str = "tiny",
    progress_callback: Optional[ProgressCallback] = None,
) -> Tuple[str, str]:
    """
    Convert one source file to markdown by invoking the skill script.
    Returns: (status, error_message)
    """
    if not SKILL_CONVERTER.exists():
        return "failed", f"Skill converter not found: {SKILL_CONVERTER}"

    input_file = input_file.resolve()
    output_markdown = output_markdown.resolve()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_out = Path(tmpdir)
        cmd = [
            sys.executable,
            "-u",
            str(SKILL_CONVERTER),
            "--input",
            str(input_file),
            "--output",
            str(tmp_out),
            "--whisper-model",
            whisper_model,
        ]
        output_lines = []
        if progress_callback:
            progress_callback(0, 100, "开始转换")

        env = os.environ.copy()
        env.setdefault("PYTHONIOENCODING", "utf-8")
        env.setdefault("PYTHONUTF8", "1")
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="ignore",
            env=env,
        )

        if proc.stdout:
            for raw in proc.stdout:
                line = raw.strip()
                if not line:
                    continue
                output_lines.append(line)
                if progress_callback:
                    match = _PROGRESS_PATTERN.search(line)
                    if match:
                        percent = int(match.group(1))
                        message = (match.group(2) or "").strip()
                        progress_callback(percent, 100, message)

        returncode = proc.wait()

        generated = list(tmp_out.glob("*.md"))
        if not generated:
            detail = " ".join(output_lines).strip()
            detail = detail or f"converter exit code: {returncode}"
            return "failed", f"No markdown generated. {detail}"

        output_markdown.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(generated[0], output_markdown)

        status, err = _parse_markdown_status(output_markdown)
        if returncode not in (0, 2) and status == "ok":
            extra = " ".join(output_lines).strip() or f"exit code: {returncode}"
            return "failed", extra
        if status == "ok" and input_file.suffix.lower() == ".pdf":
            if not _pdf_markdown_quality_ok(output_markdown):
                return "failed", "PDF 转换结果为空或信息量过低（疑似扫描版）。"

        if status == "ok" and input_file.suffix.lower() in {".mp3", ".mp4"}:
            if progress_callback:
                progress_callback(95, 100, "LLM 修正中")
            try:
                rewrite_markdown_with_llm(
                    input_path=output_markdown,
                    output_path=output_markdown,
                    max_chunk_len=800,
                    sleep_ms=0,
                )
            except Exception as exc:
                return "failed", f"LLM rewrite failed: {exc}"

        if progress_callback:
            progress_callback(100, 100, "完成")
        return status, err
