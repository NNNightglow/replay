---
name: multiformat-to-md
description: "Convert source files into Markdown for LLM ingestion. Use this skill for PDF/Word/images (.pdf/.docx/.doc/.png/.jpg/.jpeg) and audio/video transcription (.mp3/.mp4)."
---

# Multiformat To Markdown

## Script Architecture

- `scripts/multiformat_gateway.py`
- Shared/common layer for this skill.
- Provides shared helpers (`ProgressPrinter`, `resolve_inputs`, markdown builder/writer).
- Provides runtime bridge API `convert_file_to_markdown_via_skill` for `utils/agents/dataloaders.py`.

- `scripts/docs_images2md.py`
- Handles document/image conversion only.
- Includes doc/image-specific extraction functions (PDF/DOCX/DOC/OCR).

- `scripts/media2md.py`
- Handles media transcription only.
- Includes media-specific functions (audio/video transcription + optional LLM rewrite).

## Usage

Document/image to markdown:

```powershell
python skills/multiformat-to-md/scripts/docs_images2md.py `
  --input "E:/data/raw_docs" `
  --output "E:/data/md_output"
```

Media to markdown:

```powershell
python skills/multiformat-to-md/scripts/media2md.py `
  --input "E:/data/raw_media" `
  --output "E:/data/md_output" `
  --whisper-model "base"
```

Enable LLM rewrite for media:

```powershell
python skills/multiformat-to-md/scripts/media2md.py `
  --input "E:/data/raw_media" `
  --output "E:/data/md_output" `
  --whisper-model "base" `
  --rewrite-with-llm
```

## Dependencies

```powershell
pip install pypdf python-docx pillow pytesseract openai-whisper faster-whisper pypdfium2 pymupdf requests
```

System tools when needed:

- `ffmpeg` for `.mp4`
- `tesseract-ocr` for OCR
- `antiword` for `.doc` fallback
