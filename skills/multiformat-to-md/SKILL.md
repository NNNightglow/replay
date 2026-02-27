<!-- 此段说明。 -->

---

<!-- 名称 -->
# 名称

name: multiformat-to-md

<!-- 描述 -->
# # 描述

description: 将多种文件格式转换为便于 LLM 摄取的 Markdown。用于将 PDF、Word（.docx/.doc）、图片（.png/.jpg/.jpeg）、音频（.mp3）或视频（.mp4）转换为干净、可上传的 .md 文件，并包含文本提取或转写结果。

---

<!-- 多格式转 Markdown -->
# # 多格式转 Markdown

将每个输入文件转换为独立的 Markdown 文件，包含：

- 来源元数据
- 提取文本或转写内容
- 稳定的 UTF-8 输出，便于直接上传

<!-- 输出说明 -->
> 生成的 `.md` 可直接用于 LLM 上传。

<!-- 快速开始 -->
# # 快速开始

> 下面示例展示批量转换与指定文件转换。

<!-- 运行命令 -->
# # 运行命令

运行：

```powershell

python skills/multiformat-to-md/scripts/convert_to_markdown.py `
  --input "E:/data/raw_docs" `
  --output "E:/data/md_output"

```

转换指定文件：

```powershell

python skills/multiformat-to-md/scripts/convert_to_markdown.py `
  --input "a.pdf" "b.docx" "c.jpg" "d.mp3" "e.mp4" `
  --output "./out-md"

```

<!-- 支持类型 -->
# # 支持类型

> 支持以下输入类型。

- `.pdf`：使用 `pypdf` 按页提取文本
- `.docx`：使用 `python-docx` 提取段落与表格文本
- `.doc`：如安装 `antiword` 则使用其转换
- `.png/.jpg/.jpeg`：使用 `pytesseract` 做 OCR
- `.mp3`：使用 `openai-whisper` 转写
- `.mp4`：使用 `ffmpeg` 提取音频后再用 `openai-whisper` 转写

<!-- 依赖安装 -->
# # 依赖安装

> 先安装 Python 依赖。
安装 Python 依赖：

```powershell

pip install pypdf python-docx pillow pytesseract openai-whisper

```

安装系统工具：

- `ffmpeg`（用于 `.mp4`）
- `tesseract-ocr`（用于图片 OCR）
- `antiword`（用于旧 `.doc`，可选）

> 如果缺少 `ffmpeg` / `tesseract-ocr` / `antiword`，脚本会在输出 `.md` 中写明错误原因，而不是中断整个批次。

<!-- 输出规则 -->
# # 输出规则

> 输出规则适用于 LLM 上传场景。

- 每个源文件生成一个 `.md`，使用相同基名。
- 在 frontmatter 中保留源路径与转换时间戳。
- 输出使用 UTF-8，且不带 BOM。
- 即使单个文件失败，也继续处理其他文件。
