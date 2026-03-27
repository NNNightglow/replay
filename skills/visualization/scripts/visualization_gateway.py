#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Dict, List

SCRIPT_DIR = Path(__file__).resolve().parent

_CHART_REGISTRY: Dict[str, Dict[str, Any]] = {}
_LOADED_MODULES: Dict[str, ModuleType] = {}


def _register_chart(
    chart_id: str,
    func: Callable[..., Any],
    description: str = "",
    output_type: str = "",
    module_name: str = "",
) -> None:
    cid = str(chart_id or "").strip()
    if not cid:
        raise ValueError("chart_id can not be empty.")
    if not callable(func):
        raise ValueError(f"chart function is not callable: {cid}")
    _CHART_REGISTRY[cid] = {
        "id": cid,
        "func": func,
        "description": (description or "").strip(),
        "output_type": (output_type or "").strip(),
        "module": module_name or "",
    }


def _load_module_by_path(path: Path) -> ModuleType:
    module_name = f"visualization_skill_{path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module spec: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _register_from_module(module: ModuleType, module_name: str) -> int:
    registered = 0
    specs = getattr(module, "CHART_SPECS", None)
    if isinstance(specs, list):
        for item in specs:
            if not isinstance(item, dict):
                continue
            chart_id = str(item.get("id") or "").strip()
            func_ref = item.get("func")
            func = None
            if callable(func_ref):
                func = func_ref
            elif isinstance(func_ref, str) and hasattr(module, func_ref):
                func = getattr(module, func_ref)
            if not chart_id or not callable(func):
                continue
            _register_chart(
                chart_id=chart_id,
                func=func,
                description=str(item.get("description") or ""),
                output_type=str(item.get("output_type") or ""),
                module_name=module_name,
            )
            registered += 1

    if registered > 0:
        return registered

    for name in dir(module):
        if not name.startswith("plot_"):
            continue
        func = getattr(module, name)
        if not callable(func):
            continue
        _register_chart(
            chart_id=name,
            func=func,
            description=f"Auto-registered function from {module_name}",
            output_type="",
            module_name=module_name,
        )
        registered += 1
    return registered


def load_chart_modules(force_reload: bool = False) -> Dict[str, Any]:
    if force_reload:
        _CHART_REGISTRY.clear()
        _LOADED_MODULES.clear()

    loaded_count = 0
    registered_count = 0
    for path in sorted(SCRIPT_DIR.glob("chart_*.py")):
        module_key = path.as_posix()
        if module_key in _LOADED_MODULES and not force_reload:
            continue
        module = _load_module_by_path(path)
        _LOADED_MODULES[module_key] = module
        loaded_count += 1
        registered_count += _register_from_module(module, path.name)

    return {
        "loaded_modules": loaded_count,
        "registered_charts": registered_count,
        "total_charts": len(_CHART_REGISTRY),
    }


def list_charts(force_reload: bool = False) -> List[Dict[str, Any]]:
    load_chart_modules(force_reload=force_reload)
    rows: List[Dict[str, Any]] = []
    for item in _CHART_REGISTRY.values():
        rows.append(
            {
                "id": item.get("id"),
                "description": item.get("description", ""),
                "output_type": item.get("output_type", ""),
                "module": item.get("module", ""),
            }
        )
    return sorted(rows, key=lambda x: str(x.get("id") or ""))


def render_chart(chart_id: str, **kwargs) -> Dict[str, Any]:
    load_chart_modules(force_reload=False)
    cid = str(chart_id or "").strip()
    item = _CHART_REGISTRY.get(cid)
    if not item:
        return {
            "ok": False,
            "error": f"chart not found: {cid}",
            "available": [x.get("id") for x in list_charts()],
        }
    func = item.get("func")
    try:
        output = func(**kwargs)
        return {
            "ok": True,
            "chart_id": cid,
            "output_type": item.get("output_type", ""),
            "result": output,
        }
    except Exception as exc:
        return {
            "ok": False,
            "chart_id": cid,
            "error": str(exc),
        }
