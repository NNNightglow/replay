#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent.parent
BRIDGE_PATH = BASE_DIR / "skills" / "multiformat-to-md" / "scripts" / "multiformat_gateway.py"


def _load_bridge_module() -> ModuleType:
    if not BRIDGE_PATH.exists():
        raise FileNotFoundError(f"Bridge module not found: {BRIDGE_PATH}")
    spec = importlib.util.spec_from_file_location("multiformat_gateway", BRIDGE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module spec from: {BRIDGE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_BRIDGE = _load_bridge_module()

SUPPORTED_EXTENSIONS = _BRIDGE.SUPPORTED_EXTENSIONS
ProgressCallback = _BRIDGE.ProgressCallback


def rewrite_markdown_with_llm(
    input_path: Path,
    output_path: Path,
    max_chunk_len: int = 1000,
    sleep_ms: int = 0,
) -> None:
    return _BRIDGE.rewrite_markdown_with_llm(
        input_path=input_path,
        output_path=output_path,
        max_chunk_len=max_chunk_len,
        sleep_ms=sleep_ms,
    )


def convert_file_to_markdown_via_skill(
    input_file: Path,
    output_markdown: Path,
    whisper_model: str = "tiny",
    progress_callback: Optional[ProgressCallback] = None,
) -> Tuple[str, str]:
    return _BRIDGE.convert_file_to_markdown_via_skill(
        input_file=input_file,
        output_markdown=output_markdown,
        whisper_model=whisper_model,
        progress_callback=progress_callback,
    )
