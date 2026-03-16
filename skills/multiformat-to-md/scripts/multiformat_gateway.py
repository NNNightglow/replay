#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime as dt
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Tuple

SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".doc", ".png", ".jpg", ".jpeg", ".mp3", ".mp4"}
DOC_IMAGE_EXTENSIONS = {".pdf", ".docx", ".doc", ".png", ".jpg", ".jpeg"}
MEDIA_EXTENSIONS = {".mp3", ".mp4"}
ProgressCallback = Callable[[float, float, str], None]
BridgeProgressCallback = Callable[[int, int, str], None]
_PROGRESS_PATTERN = re.compile(r"\b(\d{1,3})%\s*(.*)")
_DATE_YMD_SEP_PATTERN = re.compile(r"(?<!\d)(20\d{2})[-./年](\d{1,2})[-./月](\d{1,2})(?:日)?(?!\d)")
_DATE_YMD_COMPACT_PATTERN = re.compile(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)")
_DATE_YYMD_COMPACT_PATTERN = re.compile(r"(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)")


class ProgressPrinter:
    def __init__(self, total_files: int) -> None:
        self.total_files = max(1, total_files)
        self.current_index = 0
        self._last_percent = -1
        self._last_line_len = 0
        self._use_inline = sys.stdout.isatty()

    def start_file(self, path: Path) -> None:
        self.current_index += 1
        print(f"[{self.current_index}/{self.total_files}] start: {path.name}")

    def update(self, percent: float, message: str = "") -> None:
        if percent is None:
            return
        clamped = max(0, min(100, int(percent)))
        if clamped == self._last_percent:
            return
        self._last_percent = clamped
        bar = self._render_bar(clamped)
        line = f"  {bar} {clamped:3d}% {message}".rstrip()
        if self._use_inline:
            pad = max(0, self._last_line_len - len(line))
            sys.stdout.write("\r" + line + (" " * pad))
            sys.stdout.flush()
            self._last_line_len = len(line)
        else:
            print(line)

    def finish_file(self, status: str) -> None:
        if self._use_inline and self._last_line_len:
            sys.stdout.write("\r" + (" " * self._last_line_len) + "\r")
            sys.stdout.flush()
        self._last_percent = -1
        self._last_line_len = 0
        print(f"[{self.current_index}/{self.total_files}] done: {status}")

    @staticmethod
    def _render_bar(percent: int, width: int = 24) -> str:
        filled = int(width * percent / 100)
        return "[" + ("=" * filled) + ("-" * (width - filled)) + "]"


def _make_progress_callback(printer: ProgressPrinter, label: str) -> ProgressCallback:
    def _callback(current: float, total: float, message: str = "") -> None:
        if total <= 0:
            return
        percent = (current / total) * 100
        printer.update(percent, message or label)

    return _callback


def resolve_inputs(inputs: Iterable[str], allowed_exts: Optional[set[str]] = None) -> List[Path]:
    exts = allowed_exts or SUPPORTED_EXTENSIONS
    files: List[Path] = []
    for item in inputs:
        p = Path(item).expanduser().resolve()
        if p.is_dir():
            for child in p.rglob("*"):
                if child.is_file() and child.suffix.lower() in exts:
                    files.append(child)
        elif p.is_file() and p.suffix.lower() in exts:
            files.append(p)
    return sorted(set(files))


def build_markdown(path: Path, content: str, status: str, error: str = "") -> str:
    ts = dt.datetime.now(dt.timezone.utc).isoformat()
    content_time, content_time_type, content_time_evidence = _infer_content_time(path, content)
    header = [
        "---",
        f'source_file: "{path.as_posix()}"',
        f"converted_at_utc: {ts}",
        f"status: {status}",
        f"content_time: {content_time}",
        f"content_time_type: {content_time_type}",
        f'content_time_evidence: "{content_time_evidence}"',
        "---",
        "",
        f"# {path.name}",
        "",
    ]
    if error:
        header.extend(["## Error", "", error.strip(), ""])
    header.extend(["## Content", "", content.strip(), ""])
    return "\n".join(header)


def write_output(input_file: Path, output_dir: Path, markdown: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{input_file.stem}.md"
    target.write_text(markdown, encoding="utf-8")
    return target


def _parse_markdown_status(markdown_path: Path) -> Tuple[str, str]:
    if not markdown_path.exists():
        return "failed", "Markdown output file not found."

    text = markdown_path.read_text(encoding="utf-8", errors="ignore")
    head = text[:1200]
    status = "failed" if "status: failed" in head else "ok"

    error_message = ""
    marker = "## Error"
    if marker in text:
        after = text.split(marker, 1)[1]
        if "## Content" in after:
            after = after.split("## Content", 1)[0]
        error_message = after.strip()
    return status, error_message


def _safe_date_str(year: int, month: int, day: int) -> str:
    try:
        return dt.date(year, month, day).isoformat()
    except Exception:
        return ""


def _extract_date_from_text(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""

    m = _DATE_YMD_SEP_PATTERN.search(text)
    if m:
        y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        date_str = _safe_date_str(y, mth, d)
        if date_str:
            return date_str, m.group(0)

    m = _DATE_YMD_COMPACT_PATTERN.search(text)
    if m:
        y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        date_str = _safe_date_str(y, mth, d)
        if date_str:
            return date_str, m.group(0)

    m = _DATE_YYMD_COMPACT_PATTERN.search(text)
    if m:
        yy, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        y = 2000 + yy if yy <= 69 else 1900 + yy
        date_str = _safe_date_str(y, mth, d)
        if date_str:
            return date_str, m.group(0)

    return "", ""


def _infer_content_time(path: Path, content: str) -> Tuple[str, str, str]:
    from_filename, filename_hit = _extract_date_from_text(path.name)
    if from_filename:
        return from_filename, "inferred_filename", f"filename:{filename_hit}"

    sample = (content or "")[:6000]
    from_content, content_hit = _extract_date_from_text(sample)
    if from_content:
        return from_content, "inferred_text", f"content:{content_hit}"

    return "", "unknown", ""


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


def _strip_likely_page_headings(text: str) -> str:
    lines = text.splitlines()
    kept = []
    for raw in lines:
        line = raw.strip()
        compact = re.sub(r"\s+", "", line)
        if line.startswith("##") and re.search(r"\d+", line) and len(compact) <= 20:
            continue
        kept.append(raw)
    return "\n".join(kept)


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


def _resolve_converter_for_file(input_file: Path) -> Path:
    scripts_dir = Path(__file__).resolve().parent
    ext = input_file.suffix.lower()
    if ext in DOC_IMAGE_EXTENSIONS:
        return scripts_dir / "docs_images2md.py"
    if ext in MEDIA_EXTENSIONS:
        return scripts_dir / "media2md.py"
    return scripts_dir / "docs_images2md.py"


def convert_file_to_markdown_via_skill(
    input_file: Path,
    output_markdown: Path,
    whisper_model: str = "tiny",
    progress_callback: Optional[BridgeProgressCallback] = None,
) -> Tuple[str, str]:
    input_file = input_file.resolve()
    output_markdown = output_markdown.resolve()
    converter = _resolve_converter_for_file(input_file)
    if not converter.exists():
        return "failed", f"Skill converter not found: {converter}"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_out = Path(tmpdir)
        cmd = [sys.executable, "-u", str(converter), "--input", str(input_file), "--output", str(tmp_out)]
        if input_file.suffix.lower() in MEDIA_EXTENSIONS:
            cmd.extend(["--whisper-model", whisper_model])
            if (os.getenv("STRATEGY_WATCH_AUDIO_LLM_REWRITE", "").strip().lower() in {"1", "true", "yes", "on"}):
                cmd.append("--rewrite-with-llm")

        output_lines: List[str] = []
        if progress_callback:
            progress_callback(0, 100, "Starting conversion")

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
                        progress_callback(int(match.group(1)), 100, (match.group(2) or "").strip())

        returncode = proc.wait()
        generated = list(tmp_out.glob("*.md"))
        if not generated:
            detail = " ".join(output_lines).strip() or f"converter exit code: {returncode}"
            return "failed", f"No markdown generated. {detail}"

        output_markdown.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(generated[0], output_markdown)

        status, err = _parse_markdown_status(output_markdown)
        if returncode not in (0, 2) and status == "ok":
            return "failed", " ".join(output_lines).strip() or f"exit code: {returncode}"
        if status == "ok" and input_file.suffix.lower() == ".pdf" and not _pdf_markdown_quality_ok(output_markdown):
            return "failed", "PDF conversion output is empty or too low quality."

        if progress_callback:
            progress_callback(100, 100, "Done")
        return status, err


def rewrite_markdown_with_llm(
    input_path: Path,
    output_path: Path,
    max_chunk_len: int = 1000,
    sleep_ms: int = 0,
) -> None:
    # Compatibility export for existing callers through utils.agents.dataloaders.
    scripts_dir = Path(__file__).resolve().parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    from media2md import rewrite_markdown_with_llm as _rewrite  # local import to avoid cycles

    _rewrite(
        input_path=input_path,
        output_path=output_path,
        max_chunk_len=max_chunk_len,
        sleep_ms=sleep_ms,
    )
