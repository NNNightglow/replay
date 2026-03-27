#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import asyncio
import inspect
import importlib.util
import json
import re
import time
import threading
import contextvars
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Tuple, Callable, Optional

import requests

import agentscope  # type: ignore
from agentscope.message import Msg  # type: ignore
from agentscope.agent import ReActAgent  # type: ignore

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_CACHE_GATEWAY_PATH = BASE_DIR / "skills" / "data-cache-inspector" / "scripts" / "data_cache_gateway.py"
_AGENTSCOPE_READY = False
_RUNTIME_EVENT_HOOK_LOCK = threading.Lock()
_RUNTIME_EVENT_HOOK: Optional[Callable[[Dict[str, Any]], None]] = None
_TRACE_CONTEXT: contextvars.ContextVar = contextvars.ContextVar("agent_trace_context", default={})
_DATA_CACHE_GATEWAY_MODULE: Optional[ModuleType] = None
_DATA_CACHE_GATEWAY_LOCK = threading.Lock()

AGENT_RESOURCE_DIR = Path(__file__).resolve().parent / "resource"
AGENT_PROMPT_ALT_SUFFIX = "_prompt.md"


def _load_data_cache_gateway_module() -> ModuleType:
    global _DATA_CACHE_GATEWAY_MODULE
    with _DATA_CACHE_GATEWAY_LOCK:
        if _DATA_CACHE_GATEWAY_MODULE is not None:
            return _DATA_CACHE_GATEWAY_MODULE
        if not DATA_CACHE_GATEWAY_PATH.exists():
            raise FileNotFoundError(f"Gateway module not found: {DATA_CACHE_GATEWAY_PATH}")
        spec = importlib.util.spec_from_file_location("data_cache_gateway", DATA_CACHE_GATEWAY_PATH)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Failed to load module spec from: {DATA_CACHE_GATEWAY_PATH}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _DATA_CACHE_GATEWAY_MODULE = module
        return module


def query_data_cache_table_via_skill(**kwargs) -> Dict[str, Any]:
    module = _load_data_cache_gateway_module()
    return module.query_data_cache_table(**kwargs)


def compare_market_turnover_dates_via_skill(date_a: str, date_b: str) -> Dict[str, Any]:
    module = _load_data_cache_gateway_module()
    return module.compare_market_turnover_dates(date_a=date_a, date_b=date_b)

DEFAULT_AGENT_PROMPTS: Dict[str, str] = {
    "dialog_agent": (
        "你是A股复盘系统中的通用对话助手。优先进行自然、清晰、直接的问答沟通，"
        "不做多角色流程编排。需要查看数据可用 data-cache-inspector，先说明你基于哪些数据得出结论。"
    ),
    "pm_agent": (
        "你是产品经理（PM）。先理解用户真实需求，再定义范围、输入输出、验收标准与优先级。"
        "若需求含糊，先提澄清问题；若信息足够，输出结构化需求说明。"
    ),
    "architect_agent": (
        "你是架构师（Architect）。基于 PM 需求输出 TODO List，包含任务分层、依赖关系、执行顺序、"
        "里程碑、风险点和回滚方案。优先给出最小可交付路径（MVP）。"
    ),
    "engineer_agent": (
        "你是工程师（Engineer）。负责调用现有能力、编写新 skill/功能并落地实现。"
        "输出时说明：复用了哪些能力、需要新增哪些能力、实现步骤、接口与验证方式。"
    ),
    "analyst_agent": (
        "你是分析师（Analyst）。优先使用 data-cache-inspector 盘点已有数据、图表与可用证据，"
        "再输出结构化分析：事实、信号、推断、风险与置信度。"
    ),
    "visualization_master_agent": (
        "你是可视化大师。负责将分析结论转成图表与看板方案。明确图表类型、字段映射、配色和交互，"
        "并指出潜在误读风险与展示限制。"
    ),
}


def _load_split_agent_prompts() -> Dict[str, str]:
    def _resolve_agent_name(raw_key: str) -> str:
        key = (raw_key or "").strip().lower()
        if not key:
            return ""

        alias_map = {
            "dialog": "dialog_agent",
            "strategy_coordinator": "architect_agent",
            "pm": "pm_agent",
            "architect": "architect_agent",
            "engineer": "engineer_agent",
            "analyst": "analyst_agent",
            "visualization": "visualization_master_agent",
            "visualization_master": "visualization_master_agent",
        }
        if key in alias_map:
            return alias_map[key]

        if key in DEFAULT_AGENT_PROMPTS:
            return key
        if key.endswith("_prompt"):
            key = key[: -len("_prompt")].strip()
        if key in DEFAULT_AGENT_PROMPTS:
            return key
        if key.endswith("_agent"):
            if key in DEFAULT_AGENT_PROMPTS:
                return key
            base = key[: -len("_agent")].strip()
            return alias_map.get(base, key)
        if f"{key}_agent" in DEFAULT_AGENT_PROMPTS:
            return f"{key}_agent"
        return alias_map.get(key, key)

    out: Dict[str, str] = {}
    if not AGENT_RESOURCE_DIR.exists():
        return out
    for path in AGENT_RESOURCE_DIR.glob(f"*{AGENT_PROMPT_ALT_SUFFIX}"):
        filename = path.name
        if not filename.endswith(AGENT_PROMPT_ALT_SUFFIX):
            continue
        raw_key = filename[: -len(AGENT_PROMPT_ALT_SUFFIX)].strip()
        if not raw_key:
            continue
        agent_name = _resolve_agent_name(raw_key)
        if not agent_name:
            continue
        try:
            content = path.read_text(encoding="utf-8-sig").strip()
        except Exception:
            continue
        if content:
            out[agent_name] = content
    return out


def _load_agent_prompts() -> Dict[str, str]:
    prompts = dict(DEFAULT_AGENT_PROMPTS)
    prompts.update(_load_split_agent_prompts())
    return prompts


def _inject_sys_prompts(agent_profiles: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    prompt_map = _load_agent_prompts()
    result: Dict[str, Dict[str, Any]] = {}
    for agent_name, profile in agent_profiles.items():
        cloned = dict(profile)
        cloned["sys_prompt"] = prompt_map.get(agent_name) or DEFAULT_AGENT_PROMPTS.get(agent_name, "")
        result[agent_name] = cloned
    return result


AGENT_PROFILES: Dict[str, Dict[str, Any]] = _inject_sys_prompts(
    {
        # Stage 0: 对话入口（非编排）
        "dialog_agent": {
            "display_name": "Dialog Agent",
            "description": "通用对话助手，支持调用 data-cache-inspector 查看数据。",
            "stage": 0,
            "upstream_agents": [],
            "downstream_agents": [],
            "skill_ids": ["data-cache-inspector"],
        },
        # Stage 1: 需求澄清
        "pm_agent": {
            "display_name": "PM Agent",
            "description": "产品经理，负责理解用户需求并定义验收标准。",
            "stage": 1,
            "upstream_agents": ["architect_agent"],
            "downstream_agents": [],
            "skill_ids": [],
        },
        # Stage 2: 架构拆解
        "architect_agent": {
            "display_name": "Architect Agent",
            "description": "架构师，负责输出 TODO List、任务依赖与落地路径。",
            "stage": 0,
            "upstream_agents": [],
            "downstream_agents": ["pm_agent", "engineer_agent", "analyst_agent", "visualization_master_agent"],
            "skill_ids": ["orchestration"],
        },
        # Stage 3: 工程实现
        "engineer_agent": {
            "display_name": "Engineer Agent",
            "description": "工程师，负责调用/编写 skill 或功能并交付实现结果。",
            "stage": 3,
            "upstream_agents": ["architect_agent"],
            "downstream_agents": [],
            "skill_ids": [],
        },
        # Stage 4: 数据分析
        "analyst_agent": {
            "display_name": "Analyst Agent",
            "description": "分析师，基于 data-cache-inspector 与现有数据产出策略分析。",
            "stage": 4,
            "upstream_agents": ["architect_agent"],
            "downstream_agents": [],
            "skill_ids": ["data-cache-inspector"],
        },
        # Stage 5: 可视化表达
        "visualization_master_agent": {
            "display_name": "Visualization Master",
            "description": "可视化大师，负责绘图与图表叙事表达。",
            "stage": 5,
            "upstream_agents": ["architect_agent"],
            "downstream_agents": [],
            "skill_ids": ["visualization"],
        },
    }
)

AGENT_ROOT = "architect_agent"
AGENT_PROFILE_FALLBACK = AGENT_ROOT

SKILL_REGISTRY: Dict[str, Dict[str, str]] = {
    "orchestration": {
        "display_name": "Orchestration",
        "description": "跨角色协同编排与任务拆解，输出可执行计划与阶段交接。",
    },
    "data-cache-inspector": {
        "display_name": "Data Cache Inspector",
        "description": "Inspect cached market data in data_cache (stocks, indices, sectors, metadata) and summarize what is available.",
    },
    "visualization": {
        "display_name": "Visualization",
        "description": "A-share visualization routing and data contract checks for stock/index/sector/market charts.",
    },
    "multiformat-to-md": {
        "display_name": "Multiformat To Markdown",
        "description": "Convert pdf/docx/images/audio/video and web pages into markdown for downstream analysis.",
    },
}

def _assert_agent_graph_valid() -> None:
    known_agents = set(AGENT_PROFILES.keys())
    for agent_name, profile in AGENT_PROFILES.items():
        upstream = profile.get("upstream_agents", [])
        downstream = profile.get("downstream_agents", [])
        skill_ids = profile.get("skill_ids", [])

        if not isinstance(upstream, list) or not all(isinstance(item, str) for item in upstream):
            raise RuntimeError(f"Invalid upstream_agents in {agent_name}.")
        if not isinstance(downstream, list) or not all(isinstance(item, str) for item in downstream):
            raise RuntimeError(f"Invalid downstream_agents in {agent_name}.")
        if not isinstance(skill_ids, list) or not all(isinstance(item, str) for item in skill_ids):
            raise RuntimeError(f"Invalid skill_ids in {agent_name}.")

        unknown_upstream = [item for item in upstream if item not in known_agents]
        unknown_downstream = [item for item in downstream if item not in known_agents]
        if unknown_upstream:
            raise RuntimeError(f"Unknown upstream agent(s) for {agent_name}: {unknown_upstream}")
        if unknown_downstream:
            raise RuntimeError(f"Unknown downstream agent(s) for {agent_name}: {unknown_downstream}")

    visiting = set()
    visited = set()

    def walk(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            raise RuntimeError(f"Cycle detected in AGENT_PROFILES graph around: {node}")
        visiting.add(node)
        for child in AGENT_PROFILES[node].get("downstream_agents", []):
            walk(child)
        visiting.remove(node)
        visited.add(node)

    for agent_name in AGENT_PROFILES.keys():
        walk(agent_name)


def register_skill(skill_id: str, display_name: str = "", description: str = "") -> None:
    clean_id = (skill_id or "").strip()
    if not clean_id:
        raise ValueError("skill_id can not be empty.")
    SKILL_REGISTRY[clean_id] = {
        "display_name": (display_name or clean_id).strip(),
        "description": (description or "").strip(),
    }


def bind_skill_to_agent(agent_name: str, skill_id: str, auto_register: bool = True) -> None:
    profile = AGENT_PROFILES.get(agent_name)
    if not profile:
        raise ValueError(f"Unknown agent: {agent_name}")

    clean_id = (skill_id or "").strip()
    if not clean_id:
        raise ValueError("skill_id can not be empty.")

    if clean_id not in SKILL_REGISTRY and auto_register:
        register_skill(clean_id)

    skill_ids = profile.setdefault("skill_ids", [])
    if clean_id not in skill_ids:
        skill_ids.append(clean_id)


def get_agent_profile(agent_name: str) -> Dict[str, Any]:
    return AGENT_PROFILES.get(agent_name) or AGENT_PROFILES[AGENT_PROFILE_FALLBACK]


_assert_agent_graph_valid()


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


def _build_history_block(history: List[Dict]) -> str:
    lines = []
    for item in history[-24:]:
        role = item.get("role", "")
        content = (item.get("content") or "").strip()
        if role in {"user", "assistant"} and content:
            lines.append(f"[{role}] {content}")
    return "\n".join(lines)


def _get_agent_prompt(agent_name: str) -> str:
    profile = get_agent_profile(agent_name)
    return profile["sys_prompt"]


def _compose_context_blocks(resource_context: str, extra_context: str) -> List[str]:
    blocks: List[str] = []
    if resource_context:
        blocks.append("Resource context (Markdown):\n\n" + resource_context)
    if extra_context:
        blocks.append("Extra context:\n\n" + extra_context)
    return blocks


def set_runtime_event_hook(hook: Optional[Callable[[Dict[str, Any]], None]]) -> None:
    global _RUNTIME_EVENT_HOOK
    with _RUNTIME_EVENT_HOOK_LOCK:
        _RUNTIME_EVENT_HOOK = hook


def _emit_runtime_event(event_type: str, **fields) -> None:
    with _RUNTIME_EVENT_HOOK_LOCK:
        hook = _RUNTIME_EVENT_HOOK
    if not callable(hook):
        return

    event: Dict[str, Any] = {
        "event_type": str(event_type or "").strip() or "runtime_event",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    context = _TRACE_CONTEXT.get() or {}
    if isinstance(context, dict) and context:
        event.update(context)
    if fields:
        event.update(fields)

    try:
        hook(event)
    except Exception:
        pass


def _scan_data_cache_snapshot(max_files: int = 4000) -> str:
    data_cache_root = BASE_DIR / "data_cache"
    targets: List[Tuple[str, Path, bool]] = [
        ("data_cache_root", data_cache_root, False),
        ("indices", data_cache_root / "indices", False),
        ("stock_daily", data_cache_root / "stock_daily", False),
        ("stock_minute", data_cache_root / "stock_minute", False),
        ("other", data_cache_root / "other", False),
        ("strategy_watch", data_cache_root / "strategy_watch", True),
    ]

    def _scan_one(path: Path, recursive: bool) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "exists": path.exists() and path.is_dir(),
            "file_count": 0,
            "size_bytes": 0,
            "latest_mtime_iso": "",
            "latest_inferred_date": "",
            "truncated": False,
        }
        if not out["exists"]:
            return out

        latest_mtime = 0.0
        latest_date = ""
        inspected = 0
        iterator = path.rglob("*") if recursive else path.glob("*")
        for item in iterator:
            if not item.is_file():
                continue
            inspected += 1
            if inspected > max_files:
                out["truncated"] = True
                break
            out["file_count"] += 1
            try:
                st = item.stat()
                out["size_bytes"] += int(st.st_size)
                if st.st_mtime > latest_mtime:
                    latest_mtime = st.st_mtime
            except Exception:
                pass

            name = item.name
            for pattern in (
                r"(?<!\d)(20\d{2})[-./年](\d{1,2})[-./月](\d{1,2})(?:日)?(?!\d)",
                r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)",
            ):
                hit = re.search(pattern, name)
                if not hit:
                    continue
                try:
                    y, m, d = int(hit.group(1)), int(hit.group(2)), int(hit.group(3))
                    date_str = f"{y:04d}-{m:02d}-{d:02d}"
                    if date_str > latest_date:
                        latest_date = date_str
                except Exception:
                    pass
                break

        if latest_mtime > 0:
            out["latest_mtime_iso"] = datetime.fromtimestamp(latest_mtime, tz=timezone.utc).isoformat()
        out["latest_inferred_date"] = latest_date
        return out

    snapshots = [(name, _scan_one(path, recursive)) for name, path, recursive in targets]
    latest_dates = []
    for _, snap in snapshots:
        inferred = str(snap.get("latest_inferred_date") or "").strip()
        if inferred:
            latest_dates.append(inferred)
    latest_date = max(latest_dates) if latest_dates else ""

    lines = [
        "Data cache inspector snapshot:",
        f"- cache_root: {data_cache_root.as_posix()}",
        f"- latest_inferred_data_date: {latest_date or 'unknown'}",
    ]
    for name, snap in snapshots:
        if not snap.get("exists"):
            lines.append(f"- {name}: missing")
            continue
        lines.append(
            "- {name}: files={files}, size_bytes={size}, latest_mtime={mtime}, inferred_date={idata}{tail}".format(
                name=name,
                files=snap.get("file_count", 0),
                size=snap.get("size_bytes", 0),
                mtime=snap.get("latest_mtime_iso") or "unknown",
                idata=snap.get("latest_inferred_date") or "unknown",
                tail=", truncated=true" if snap.get("truncated") else "",
            )
        )
    lines.extend(["", *_build_data_cache_schema_lines()])
    return "\n".join(lines)


def _detect_date_column(schema_items: List[Tuple[str, Any]]) -> str:
    if not schema_items:
        return ""

    preferred_names = (
        "日期",
        "date",
        "trade_date",
        "时间",
        "datetime",
        "timestamp",
    )
    lowered_map = {str(name).strip().lower(): str(name) for name, _ in schema_items}
    for key in preferred_names:
        hit = lowered_map.get(key.lower())
        if hit:
            return hit

    for name, dtype in schema_items:
        dt = str(dtype or "")
        if "Date" in dt or "Datetime" in dt:
            return str(name)
    return ""


def _schema_preview_for_parquet(path: Path, max_columns: int = 18) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "exists": path.exists() and path.is_file(),
        "col_count": 0,
        "schema_preview": [],
        "date_col": "",
        "date_min": "",
        "date_max": "",
        "error": "",
    }
    if not out["exists"]:
        return out

    try:
        import polars as pl  # type: ignore

        lf = pl.scan_parquet(str(path))
        schema = dict(getattr(lf, "schema", {}) or {})
        schema_items = list(schema.items())
        out["col_count"] = len(schema_items)
        preview_items = schema_items[:max_columns]
        out["schema_preview"] = [f"{name}:{dtype}" for name, dtype in preview_items]
        if len(schema_items) > max_columns:
            out["schema_preview"].append(f"...(+{len(schema_items) - max_columns} cols)")

        date_col = _detect_date_column(schema_items)
        out["date_col"] = date_col
        if date_col:
            try:
                date_range = (
                    lf.select(
                        pl.col(date_col).min().alias("__min_date__"),
                        pl.col(date_col).max().alias("__max_date__"),
                    )
                    .collect()
                    .to_dicts()
                )
                if date_range:
                    row = date_range[0] or {}
                    if row.get("__min_date__") is not None:
                        out["date_min"] = str(row.get("__min_date__"))
                    if row.get("__max_date__") is not None:
                        out["date_max"] = str(row.get("__max_date__"))
            except Exception:
                pass
    except Exception as exc:
        out["error"] = str(exc)

    return out


def _build_data_cache_schema_lines() -> List[str]:
    root = BASE_DIR / "data_cache"
    targets: List[str] = [
        "indices/index_daily_metadata.parquet",
        "indices/index_minute_metadata.parquet",
        "stock_daily/stock_daily_metadata.parquet",
        "other/market_metadata.parquet",
        "other/market_states.parquet",
        "sectors/sectors_ths.parquet",
        "sectors/sectors_dc.parquet",
    ]
    lines = ["Core metadata schemas (column:dtype preview):"]
    for rel in targets:
        path = root / rel
        summary = _schema_preview_for_parquet(path)
        if not summary.get("exists"):
            lines.append(f"- {rel}: missing")
            continue
        if summary.get("error"):
            lines.append(f"- {rel}: read_error={summary.get('error')}")
            continue

        details: List[str] = [f"cols={summary.get('col_count', 0)}"]
        date_col = str(summary.get("date_col") or "").strip()
        date_min = str(summary.get("date_min") or "").strip()
        date_max = str(summary.get("date_max") or "").strip()
        if date_col:
            details.append(f"date_col={date_col}")
        if date_min or date_max:
            details.append(f"date_range={date_min or 'unknown'} -> {date_max or 'unknown'}")
        schema_preview = ", ".join(summary.get("schema_preview") or [])
        lines.append(f"- {rel}: {'; '.join(details)}; schema={schema_preview or 'unknown'}")
    return lines


_DATA_CACHE_TABLE_PATHS: Dict[str, str] = {
    "index_daily_metadata": "indices/index_daily_metadata.parquet",
    "index_minute_metadata": "indices/index_minute_metadata.parquet",
    "stock_daily_metadata": "stock_daily/stock_daily_metadata.parquet",
    "market_metadata": "other/market_metadata.parquet",
    "market_states": "other/market_states.parquet",
    "sectors_ths": "sectors/sectors_ths.parquet",
    "sectors_dc": "sectors/sectors_dc.parquet",
}


_DATA_CACHE_TABLE_ALIASES: Dict[str, str] = {
    "index_daily": "index_daily_metadata",
    "index_minute": "index_minute_metadata",
    "stock_daily": "stock_daily_metadata",
    "market": "market_metadata",
    "market_meta": "market_metadata",
    "market_state": "market_states",
    "sector_ths": "sectors_ths",
    "sector_dc": "sectors_dc",
}


def _normalize_date_ymd(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    hit = re.search(r"(20\d{2})[-./年](\d{1,2})[-./月](\d{1,2})", text)
    if hit:
        y, m, d = int(hit.group(1)), int(hit.group(2)), int(hit.group(3))
        return f"{y:04d}-{m:02d}-{d:02d}"
    hit = re.search(r"(20\d{2})(\d{2})(\d{2})", text)
    if hit:
        y, m, d = int(hit.group(1)), int(hit.group(2)), int(hit.group(3))
        return f"{y:04d}-{m:02d}-{d:02d}"
    return ""


def _resolve_data_cache_table(table: str) -> Tuple[str, Path]:
    raw = str(table or "").strip().lower().replace("\\", "/")
    table_key = _DATA_CACHE_TABLE_ALIASES.get(raw, raw)
    if table_key not in _DATA_CACHE_TABLE_PATHS:
        for key, rel in _DATA_CACHE_TABLE_PATHS.items():
            rel_norm = rel.lower().replace("\\", "/")
            rel_no_ext = rel_norm[:-8] if rel_norm.endswith(".parquet") else rel_norm
            if raw in {rel_norm, rel_no_ext, key.lower()}:
                table_key = key
                break
    if table_key not in _DATA_CACHE_TABLE_PATHS:
        raise ValueError(
            "Unknown table. Available: "
            + ", ".join(sorted(_DATA_CACHE_TABLE_PATHS.keys()))
        )
    rel = _DATA_CACHE_TABLE_PATHS[table_key]
    return table_key, (BASE_DIR / "data_cache" / rel)


def _parse_columns_arg(columns: str, available_columns: List[str]) -> Tuple[List[str], List[str]]:
    if not columns:
        return [], []
    requested = [item.strip() for item in str(columns).split(",") if item.strip()]
    if not requested:
        return [], []
    available_set = set(available_columns)
    valid = [item for item in requested if item in available_set]
    invalid = [item for item in requested if item not in available_set]
    return valid, invalid


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _query_data_cache_table(
    table: str,
    date: str = "",
    date_from: str = "",
    date_to: str = "",
    columns: str = "",
    filter_column: str = "",
    filter_value: str = "",
    sort_desc: bool = True,
    limit: int = 20,
) -> Dict[str, Any]:
    try:
        import polars as pl  # type: ignore
    except Exception as exc:
        return {"ok": False, "error": f"polars unavailable: {exc}"}

    try:
        table_key, table_path = _resolve_data_cache_table(table)
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    if not table_path.exists() or not table_path.is_file():
        return {"ok": False, "error": f"table file not found: {table_path.as_posix()}"}

    safe_limit = max(1, min(int(limit or 20), 200))
    try:
        lf = pl.scan_parquet(str(table_path))
        schema = dict(getattr(lf, "schema", {}) or {})
    except Exception as exc:
        return {"ok": False, "error": f"failed to scan parquet: {exc}"}

    available_columns = list(schema.keys())
    schema_items = list(schema.items())
    date_col = _detect_date_column(schema_items)

    valid_columns, invalid_columns = _parse_columns_arg(columns, available_columns)
    selected_columns = valid_columns if valid_columns else available_columns[: min(18, len(available_columns))]

    working = lf
    date_norm = _normalize_date_ymd(date)
    date_from_norm = _normalize_date_ymd(date_from)
    date_to_norm = _normalize_date_ymd(date_to)
    if date_col:
        working = working.with_columns(pl.col(date_col).cast(pl.Utf8).str.slice(0, 10).alias("__date_key__"))
        if date_norm:
            working = working.filter(pl.col("__date_key__") == date_norm)
        if date_from_norm:
            working = working.filter(pl.col("__date_key__") >= date_from_norm)
        if date_to_norm:
            working = working.filter(pl.col("__date_key__") <= date_to_norm)

    fcol = str(filter_column or "").strip()
    if fcol:
        if fcol not in schema:
            return {
                "ok": False,
                "error": f"filter_column not found: {fcol}",
                "available_columns": available_columns,
            }
        filter_values = [x.strip() for x in re.split(r"[,，]", str(filter_value or "")) if x.strip()]
        if len(filter_values) > 1:
            working = working.filter(pl.col(fcol).cast(pl.Utf8).is_in(filter_values))
        else:
            working = working.filter(pl.col(fcol).cast(pl.Utf8) == str(filter_value or ""))

    if date_col:
        working = working.sort("__date_key__", descending=bool(sort_desc))

    try:
        rows_df = working.select([pl.col(col) for col in selected_columns]).head(safe_limit).collect()
        rows = rows_df.to_dicts()
    except Exception as exc:
        return {"ok": False, "error": f"failed to collect rows: {exc}"}

    return {
        "ok": True,
        "table": table_key,
        "path": table_path.relative_to(BASE_DIR).as_posix(),
        "date_col": date_col,
        "query": {
            "date": date_norm,
            "date_from": date_from_norm,
            "date_to": date_to_norm,
            "filter_column": fcol,
            "filter_value": str(filter_value or ""),
            "sort_desc": bool(sort_desc),
            "limit": safe_limit,
        },
        "available_columns": available_columns,
        "selected_columns": selected_columns,
        "invalid_columns": invalid_columns,
        "returned_rows": len(rows),
        "rows": rows,
    }


def _tool_data_cache_query_table(
    table: str,
    date: str = "",
    date_from: str = "",
    date_to: str = "",
    columns: str = "",
    filter_column: str = "",
    filter_value: str = "",
    sort_desc: bool = True,
    limit: int = 20,
) -> Any:
    start_perf = time.perf_counter()
    _emit_runtime_event(
        "agent_tool_start",
        tool_name="data_cache_query_table",
        table=str(table or ""),
        date=str(date or ""),
        date_from=str(date_from or ""),
        date_to=str(date_to or ""),
        filter_column=str(filter_column or ""),
        limit=int(limit or 20),
    )
    result = query_data_cache_table_via_skill(
        table=table,
        date=date,
        date_from=date_from,
        date_to=date_to,
        columns=columns,
        filter_column=filter_column,
        filter_value=filter_value,
        sort_desc=sort_desc,
        limit=limit,
    )
    text = json.dumps(result, ensure_ascii=False, default=str)
    _emit_runtime_event(
        "agent_tool_done",
        tool_name="data_cache_query_table",
        duration_ms=int((time.perf_counter() - start_perf) * 1000),
        output_chars=len(text or ""),
        ok=bool(result.get("ok")),
    )
    try:
        from agentscope.tool import ToolResponse  # type: ignore

        return ToolResponse(content=text)
    except Exception:
        return text


def _tool_compare_market_turnover_dates(date_a: str, date_b: str) -> Any:
    start_perf = time.perf_counter()
    _emit_runtime_event(
        "agent_tool_start",
        tool_name="compare_market_turnover_dates",
        date_a=str(date_a or ""),
        date_b=str(date_b or ""),
    )
    output = compare_market_turnover_dates_via_skill(
        date_a=str(date_a or ""),
        date_b=str(date_b or ""),
    )

    text = json.dumps(output, ensure_ascii=False, default=str)
    _emit_runtime_event(
        "agent_tool_done",
        tool_name="compare_market_turnover_dates",
        duration_ms=int((time.perf_counter() - start_perf) * 1000),
        output_chars=len(text or ""),
        ok=bool(output.get("ok")),
    )
    try:
        from agentscope.tool import ToolResponse  # type: ignore

        return ToolResponse(content=text)
    except Exception:
        return text


def _tool_data_cache_inspector() -> Any:
    start_ts = time.time()
    start_perf = time.perf_counter()
    _emit_runtime_event(
        "agent_tool_start",
        tool_name="data_cache_inspector",
        started_at=datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat(),
    )
    text = _scan_data_cache_snapshot()
    duration_ms = int((time.perf_counter() - start_perf) * 1000)
    _emit_runtime_event(
        "agent_tool_done",
        tool_name="data_cache_inspector",
        duration_ms=duration_ms,
        output_chars=len(text or ""),
    )
    try:
        from agentscope.tool import ToolResponse  # type: ignore

        return ToolResponse(content=text)
    except Exception:
        return text


def _tool_specs_for_agent(agent_name: str) -> List[Dict[str, Any]]:
    profile = get_agent_profile(agent_name)
    skill_ids = set(profile.get("skill_ids", []) or [])
    tools: List[Dict[str, Any]] = []
    if "data-cache-inspector" in skill_ids:
        tools.append(
            {
                "name": "data_cache_inspector",
                "func": _tool_data_cache_inspector,
                "description": (
                    "Inspect local data_cache status (stocks/indices/market metadata), "
                    "return file count/size/latest date, plus schema previews with column names and dtypes."
                ),
            }
        )
        tools.append(
            {
                "name": "data_cache_query_table",
                "func": _tool_data_cache_query_table,
                "description": (
                    "Read rows from a known data_cache parquet table with filters. "
                    "Arguments: table, date/date_from/date_to, columns(comma-separated), "
                    "filter_column/filter_value, sort_desc, limit."
                ),
            }
        )
        tools.append(
            {
                "name": "compare_market_turnover_dates",
                "func": _tool_compare_market_turnover_dates,
                "description": (
                    "Compare market total turnover between two dates using "
                    "data_cache/other/market_metadata.parquet field 成交总额."
                ),
            }
        )
    return tools


def _build_agentscope_toolkit(agent_name: str):
    tool_specs = _tool_specs_for_agent(agent_name)
    if not tool_specs:
        _emit_runtime_event("agent_tools_bound", agent_name=agent_name, tool_names=[])
        return None

    try:
        from agentscope.tool import Toolkit  # type: ignore

        toolkit = Toolkit()
        for spec in tool_specs:
            register = getattr(toolkit, "register_tool_function", None)
            if callable(register):
                try:
                    register(
                        spec["func"],
                        func_name=spec["name"],
                        func_description=spec["description"],
                    )
                except TypeError:
                    try:
                        register(spec["func"], spec["name"], spec["description"])
                    except Exception:
                        register(spec["func"])
                continue
            register = getattr(toolkit, "add_tool", None)
            if callable(register):
                register(spec["func"], name=spec["name"], description=spec["description"])
                continue
        _emit_runtime_event(
            "agent_tools_bound",
            agent_name=agent_name,
            tool_names=[str(item.get("name") or "") for item in tool_specs],
        )
        return toolkit
    except Exception as primary_error:
        last_error = primary_error

        try:
            from agentscope.service import ServiceToolkit  # type: ignore

            toolkit = ServiceToolkit()
            for spec in tool_specs:
                if hasattr(toolkit, "add"):
                    toolkit.add(spec["func"])
            return toolkit
        except Exception:
            raise RuntimeError(f"Failed to initialize tool toolkit: {last_error}")

    return None


def _build_agentscope_model(
    model_name: str,
    base_url: str,
    api_key: str,
    temperature: float,
    stream: bool = False,
):
    try:
        from agentscope.model import OpenAIChatModel  # type: ignore

        return OpenAIChatModel(
            model_name=model_name,
            api_key=api_key,
            stream=bool(stream),
            client_kwargs={"base_url": base_url},
            generate_kwargs={"temperature": max(0.0, min(float(temperature), 2.0))},
        )
    except Exception:
        _ensure_agentscope_init(
            model_name=model_name,
            base_url=base_url,
            api_key=api_key,
            temperature=temperature,
        )
        return "stock_llm"


def _build_agentscope_formatter():
    try:
        from agentscope.formatter import OpenAIChatFormatter  # type: ignore

        return OpenAIChatFormatter()
    except Exception:
        return None


def _build_agentscope_memory():
    try:
        from agentscope.memory import InMemoryMemory  # type: ignore

        return InMemoryMemory()
    except Exception:
        return None


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _normalize_model_name(model_name: str) -> str:
    raw = (model_name or "").strip()
    if not raw:
        return raw
    lowered = raw.lower()
    alias_map = {
        "ds v3.2": "deepseek-v3.2",
        "ds-v3.2": "deepseek-v3.2",
        "ds v3.2 thinking": "deepseek-v3.2-thinking",
        "ds-v3.2-thinking": "deepseek-v3.2-thinking",
        "ds v3.1": "deepseek-v3.1",
        "ds-v3.1": "deepseek-v3.1",
        "deepseek/deepseek-v3.2": "deepseek-v3.2",
        "deepseek/deepseek-v3.2-thinking": "deepseek-v3.2-thinking",
        "deepseek/deepseek-v3.1": "deepseek-v3.1",
        "deepseek/deepseek-v3.1-terminus": "deepseek-v3.1",
    }
    if lowered in alias_map:
        return alias_map[lowered]
    if lowered.startswith("deepseek/"):
        tail = raw.split("/", 1)[1].strip()
        if tail.lower().startswith("deepseek-"):
            return tail
    return raw


def _truncate_text(text: str, max_chars: int) -> str:
    value = (text or "").strip()
    if max_chars <= 0 or len(value) <= max_chars:
        return value
    keep = max(64, max_chars - 48)
    return value[:keep] + "\n...[truncated]"


def _estimate_tokens_from_text(text: str) -> int:
    if not text:
        return 0
    # Conservative approximation for mixed Chinese/English prompts.
    byte_len = len(text.encode("utf-8", errors="ignore"))
    return max(1, int(byte_len / 2.4))


def _estimate_messages_tokens(messages: List[Dict[str, Any]]) -> int:
    total = 3
    for item in messages:
        total += 4
        total += _estimate_tokens_from_text(str(item.get("content") or ""))
    return total


def _fit_messages_to_budget(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not messages:
        return []

    max_input_tokens = _env_int("OPENAI_MAX_INPUT_TOKENS", 20_000)
    max_output_tokens = _env_int("OPENAI_MAX_OUTPUT_TOKENS", 1_500)
    soft_budget = max(2_000, max_input_tokens - max_output_tokens)

    trimmed: List[Dict[str, Any]] = [
        {"role": str(item.get("role") or ""), "content": str(item.get("content") or "")}
        for item in messages
    ]

    system_block_max_chars = _env_int("OPENAI_SYSTEM_BLOCK_MAX_CHARS", 12_000)
    for idx, item in enumerate(trimmed):
        if item.get("role") == "system" and idx > 0:
            item["content"] = _truncate_text(item.get("content") or "", system_block_max_chars)

    while _estimate_messages_tokens(trimmed) > soft_budget and len(trimmed) > 2:
        drop_idx = -1
        for idx in range(1, len(trimmed) - 1):
            if trimmed[idx].get("role") in {"assistant", "user", "system"}:
                drop_idx = idx
                break
        if drop_idx == -1:
            break
        trimmed.pop(drop_idx)

    if _estimate_messages_tokens(trimmed) > soft_budget:
        last_idx = len(trimmed) - 1
        last_msg_cap = _env_int("OPENAI_LAST_USER_MAX_CHARS", 6_000)
        trimmed[last_idx]["content"] = _truncate_text(trimmed[last_idx].get("content") or "", last_msg_cap)

    if _estimate_messages_tokens(trimmed) > soft_budget and trimmed:
        first_cap = _env_int("OPENAI_SYSTEM_PROMPT_MAX_CHARS", 3_000)
        trimmed[0]["content"] = _truncate_text(trimmed[0].get("content") or "", first_cap)

    return trimmed


def _normalize_history_messages(
    history_messages: List[Dict[str, Any]],
    user_content: str,
) -> List[Dict[str, str]]:
    per_message_max_chars = _env_int("OPENAI_HISTORY_ITEM_MAX_CHARS", 2_400)
    total_history_max_chars = _env_int("OPENAI_HISTORY_TOTAL_MAX_CHARS", 14_000)
    normalized_reversed: List[Dict[str, str]] = []
    total_chars = 0

    for item in reversed(history_messages[-24:]):
        role = (item.get("role") or "").strip()
        content = (item.get("content") or "").strip()
        if role in {"user", "assistant"} and content:
            clipped = _truncate_text(content, per_message_max_chars)
            projected = total_chars + len(clipped)
            if projected > total_history_max_chars:
                remain = total_history_max_chars - total_chars
                if remain <= 100:
                    continue
                clipped = _truncate_text(clipped, remain)
                projected = total_chars + len(clipped)
            normalized_reversed.append({"role": role, "content": clipped})
            total_chars = projected

    user_content = (user_content or "").strip()
    normalized = list(reversed(normalized_reversed))
    if user_content:
        if not normalized or normalized[-1].get("role") != "user" or normalized[-1].get("content") != user_content:
            normalized.append({"role": "user", "content": user_content})
    return normalized


def _build_openai_messages(
    sys_prompt: str,
    resource_context: str,
    extra_context: str,
    history_messages: List[Dict[str, Any]],
    user_content: str,
) -> List[Dict[str, Any]]:
    messages: List[Dict[str, Any]] = [{"role": "system", "content": sys_prompt}]
    for block in _compose_context_blocks(resource_context, extra_context):
        messages.append({"role": "system", "content": block})
    messages.extend(_normalize_history_messages(history_messages, user_content))
    return _fit_messages_to_budget(messages)


def _ensure_agentscope_init(model_name: str, base_url: str, api_key: str, temperature: float) -> None:
    global _AGENTSCOPE_READY
    if _AGENTSCOPE_READY:
        return

    agentscope.init(
        model_configs=[
            {
                "config_name": "stock_llm",
                "model_type": "openai_chat",
                "model_name": model_name,
                "api_key": api_key,
                "client_args": {"base_url": base_url},
                "generate_args": {"temperature": temperature},
            }
        ],
        logger_level="WARNING",
    )
    _AGENTSCOPE_READY = True



def _build_agentscope_agent(
    agent_name: str,
    sys_prompt: str,
    model: Any,
    formatter: Any,
    toolkit: Any,
    memory: Any,
):
    cls = ReActAgent
    signatures = inspect.signature(cls.__init__).parameters

    def _pick_kwargs(raw: Dict[str, Any]) -> Dict[str, Any]:
        if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in signatures.values()):
            return raw
        return {k: v for k, v in raw.items() if k in signatures}

    base = {"name": agent_name, "sys_prompt": sys_prompt}
    if model is not None:
        base["model"] = model
    if formatter is not None:
        base["formatter"] = formatter
    if memory is not None:
        base["memory"] = memory

    if toolkit is not None:
        for key in ("toolkit", "service_toolkit", "tools", "tool_manager"):
            base[key] = toolkit

    candidates = [
        dict(base),
        dict(base, system_prompt=sys_prompt),
        dict(base, model_config_name=model if isinstance(model, str) else "stock_llm"),
        {"name": agent_name, "sys_prompt": sys_prompt, "model_config_name": "stock_llm"},
        {"name": agent_name, "system_prompt": sys_prompt, "model_config_name": "stock_llm"},
        {"name": agent_name, "model_config_name": "stock_llm"},
    ]
    last_error = None
    for item in candidates:
        kwargs = _pick_kwargs(item)
        try:
            return cls(**kwargs)
        except Exception as exc:  # pragma: no cover - runtime compatibility branch
            last_error = exc
            continue

    if last_error:
        raise last_error
    raise RuntimeError(f"Failed to build agentscope agent for {agent_name}.")


def _call_agentscope_agent(agent: Any, user_prompt: str) -> Any:
    _emit_runtime_event("agent_invoke_start", prompt_chars=len(user_prompt or ""))
    msg = Msg(
        name="user",
        role="user",
        content=user_prompt,
    )
    try:
        reply = agent(msg)
    except TypeError:
        reply = agent(user_prompt)

    if inspect.isawaitable(reply):
        try:
            asyncio.get_running_loop()
            raise RuntimeError("Async AgentScope call is not supported inside running event loop.")
        except RuntimeError as exc:
            if "no running event loop" in str(exc).lower():
                reply = asyncio.run(reply)
            else:
                _emit_runtime_event("agent_invoke_error", error=str(exc))
                raise
    _emit_runtime_event("agent_invoke_done")
    return reply


def _normalize_reply_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("text", "content", "message", "output_text"):
            if key in value:
                text = _normalize_reply_text(value.get(key))
                if text:
                    return text
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value).strip()
    if isinstance(value, (list, tuple)):
        parts: List[str] = []
        for item in value:
            text = _normalize_reply_text(item)
            if text:
                parts.append(text)
        return "\n".join(parts).strip()
    nested_content = getattr(value, "content", None)
    if nested_content is not None and nested_content is not value:
        text = _normalize_reply_text(nested_content)
        if text:
            return text
    return str(value).strip()


def _extract_reply_content(reply: Any) -> str:
    content = getattr(reply, "content", None)
    if content is not None:
        text = _normalize_reply_text(content)
        if text:
            return text
    text = _normalize_reply_text(reply)
    return text


def _call_openai_compatible(
    messages: List[Dict],
    model_name: str,
    base_url: str,
    api_key: str,
    temperature: float,
) -> Tuple[str, Dict[str, Any]]:
    response = requests.post(
        f"{base_url.rstrip('/')}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model_name,
            "messages": messages,
            "temperature": max(0.0, min(float(temperature), 2.0)),
        },
        timeout=int(os.getenv("OPENAI_TIMEOUT_SECONDS", "180")),
    )
    if response.status_code >= 400:
        raise RuntimeError(f"LLM interface error {response.status_code}: {response.text[:400]}")
    data = response.json()
    choices = data.get("choices") or []
    if not choices:
        raise RuntimeError("LLM returned empty choices.")
    message = (choices[0] or {}).get("message") or {}
    content = message.get("content", "")
    usage = data.get("usage") or {}
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                parts.append(item.get("text") or "")
            else:
                parts.append(str(item))
        content = "".join(parts)
    return (str(content) or "").strip() or "Model returned empty content.", usage





def _stream_openai_compatible(
    messages: List[Dict],
    model_name: str,
    base_url: str,
    api_key: str,
    temperature: float,
    usage_collector: Dict[str, Any] = None,
):
    def _is_parameter_error(text: str) -> bool:
        lowered = (text or "").lower()
        needles = (
            "parameter error",
            "-10003",
            "invalid parameter",
            "unknown parameter",
            "unsupported parameter",
            "does not support",
            "not support",
            "参数错误",
            "不支持",
        )
        return any(item in lowered for item in needles)

    include_usage = os.getenv("OPENAI_STREAM_INCLUDE_USAGE", "1").strip() != "0"
    req_payload = {
        "model": model_name,
        "messages": messages,
        "temperature": max(0.0, min(float(temperature), 2.0)),
        "stream": True,
    }
    if include_usage:
        req_payload["stream_options"] = {"include_usage": True}

    def _post_stream(payload: Dict[str, Any]):
        return requests.post(
            f"{base_url.rstrip('/')}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            stream=True,
            timeout=int(os.getenv("OPENAI_TIMEOUT_SECONDS", "180")),
        )

    response = _post_stream(req_payload)
    body_hint = (response.text or "")[:800] if response.status_code >= 400 else ""
    if response.status_code >= 400 and include_usage and "stream_options" in req_payload:
        if _is_parameter_error(body_hint):
            retry_payload = dict(req_payload)
            retry_payload.pop("stream_options", None)
            response = _post_stream(retry_payload)
            body_hint = (response.text or "")[:800] if response.status_code >= 400 else ""

    if response.status_code >= 400 and _is_parameter_error(body_hint):
        # Some gateways reject streaming or stream_options for specific models.
        # Fall back to one-shot completion so the request can still succeed.
        content, usage = _call_openai_compatible(
            messages=messages,
            model_name=model_name,
            base_url=base_url,
            api_key=api_key,
            temperature=temperature,
        )
        if usage_collector is not None and isinstance(usage, dict):
            usage_collector["usage"] = usage
        if content:
            yield content
        return

    if response.status_code >= 400:
        raise RuntimeError(f"LLM interface error {response.status_code}: {response.text[:400]}")

    for raw_line in response.iter_lines(decode_unicode=True):
        if not raw_line:
            continue
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            break
        try:
            payload = json.loads(data)
        except Exception:
            continue
        usage = payload.get("usage")
        if usage_collector is not None and isinstance(usage, dict):
            usage_collector["usage"] = usage
        choices = payload.get("choices") or []
        if not choices:
            continue
        delta = (choices[0] or {}).get("delta") or {}
        chunk = delta.get("content") or ""
        if chunk:
            yield str(chunk)





def list_agent_profiles() -> List[Dict]:
    sorted_profiles = sorted(
        AGENT_PROFILES.items(),
        key=lambda item: (int(item[1].get("stage", 10_000)), item[0]),
    )
    result = []
    for name, profile in sorted_profiles:
        skill_ids = list(profile.get("skill_ids", []))
        skills = []
        for skill_id in skill_ids:
            skill_info = SKILL_REGISTRY.get(skill_id) or {}
            skills.append(
                {
                    "skill_id": skill_id,
                    "display_name": skill_info.get("display_name", skill_id),
                    "description": skill_info.get("description", ""),
                }
            )
        result.append(
            {
                "name": name,
                "display_name": profile["display_name"],
                "description": profile["description"],
                "stage": int(profile.get("stage", 10_000)),
                "upstream_agents": list(profile.get("upstream_agents", [])),
                "downstream_agents": list(profile.get("downstream_agents", [])),
                "skill_ids": skill_ids,
                "skills": skills,
            }
        )
    return result


def chat_with_agent(
    agent_name: str,
    user_content: str,
    history_messages: List[Dict],
    resource_context: str = "",
    extra_context: str = "",
    model_name: str = "",
    temperature: float = 0.2,
    trace_context: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str, str, Dict[str, Any]]:
    _load_env_files()
    strict_agentscope = (os.getenv("AGENTSCOPE_STRICT") or "1").strip() != "0"
    trace_token = _TRACE_CONTEXT.set(dict(trace_context or {}))

    try:
        api_key = (os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY / LLM_API_KEY is not configured.")

        resolved_model = _normalize_model_name(model_name or os.getenv("OPENAI_MODEL") or "gpt-4o-mini")
        base_url = (os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").strip()
        _emit_runtime_event(
            "agent_request_start",
            agent_name=agent_name,
            model=resolved_model,
            stream=False,
            prompt_chars=len(user_content or ""),
        )

        sys_prompt = _get_agent_prompt(agent_name)
        history_block = _build_history_block(history_messages)
        composed_user = user_content
        if history_block:
            composed_user = (
                "History summary:\n"
                f"{history_block}\n\n"
                "User question:\n"
                f"{user_content}"
            )
        context_blocks = _compose_context_blocks(resource_context, extra_context)

        try:
            model = _build_agentscope_model(
                model_name=resolved_model,
                base_url=base_url,
                api_key=api_key,
                temperature=temperature,
                stream=False,
            )
            formatter = _build_agentscope_formatter()
            memory = _build_agentscope_memory()
            toolkit = _build_agentscope_toolkit(agent_name)
            agent = _build_agentscope_agent(
                agent_name=agent_name,
                sys_prompt=sys_prompt,
                model=model,
                formatter=formatter,
                toolkit=toolkit,
                memory=memory,
            )
            composed_user_for_agent = composed_user
            if context_blocks:
                composed_user_for_agent = "\n\n".join(context_blocks) + "\n\nUser question:\n" + composed_user
            reply = _call_agentscope_agent(agent, composed_user_for_agent)
            content = _extract_reply_content(reply)
            _emit_runtime_event(
                "agent_request_done",
                agent_name=agent_name,
                model=resolved_model,
                provider="agentscope_react",
                response_chars=len(content or ""),
            )
            return content.strip(), resolved_model, "agentscope_react", {}
        except Exception as exc:
            _emit_runtime_event(
                "agent_request_error",
                agent_name=agent_name,
                model=resolved_model,
                provider="agentscope_react",
                error=str(exc),
            )
            if strict_agentscope:
                raise

        messages = _build_openai_messages(
            sys_prompt=sys_prompt,
            resource_context=resource_context,
            extra_context=extra_context,
            history_messages=history_messages,
            user_content=user_content,
        )
        fallback_reply, usage = _call_openai_compatible(
            messages=messages,
            model_name=resolved_model,
            base_url=base_url,
            api_key=api_key,
            temperature=temperature,
        )
        _emit_runtime_event(
            "agent_request_done",
            agent_name=agent_name,
            model=resolved_model,
            provider="openai_compatible_fallback",
            response_chars=len(fallback_reply or ""),
        )
        return fallback_reply, resolved_model, "openai_compatible_fallback", usage
    finally:
        _TRACE_CONTEXT.reset(trace_token)









def chat_with_agent_stream(
    agent_name: str,
    user_content: str,
    history_messages: List[Dict],
    resource_context: str = "",
    extra_context: str = "",
    model_name: str = "",
    temperature: float = 0.2,
    trace_context: Optional[Dict[str, Any]] = None,
):
    _load_env_files()
    strict_agentscope = (os.getenv("AGENTSCOPE_STRICT") or "1").strip() != "0"
    trace_token = _TRACE_CONTEXT.set(dict(trace_context or {}))

    try:
        api_key = (os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY / LLM_API_KEY is not configured.")

        resolved_model = _normalize_model_name(model_name or os.getenv("OPENAI_MODEL") or "gpt-4o-mini")
        base_url = (os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").strip()
        sys_prompt = _get_agent_prompt(agent_name)

        try:
            content, resolved_model, provider, _usage = chat_with_agent(
                agent_name=agent_name,
                user_content=user_content,
                history_messages=history_messages,
                resource_context=resource_context,
                extra_context=extra_context,
                model_name=model_name,
                temperature=temperature,
                trace_context=trace_context,
            )

            def _single():
                if content:
                    yield content

            return _single(), resolved_model, provider, {}
        except Exception as exc:
            _emit_runtime_event(
                "agent_stream_error",
                agent_name=agent_name,
                model=resolved_model,
                provider="agentscope_react",
                error=str(exc),
            )
            if strict_agentscope:
                raise

        messages = _build_openai_messages(
            sys_prompt=sys_prompt,
            resource_context=resource_context,
            extra_context=extra_context,
            history_messages=history_messages,
            user_content=user_content,
        )
        usage_collector: Dict[str, Any] = {}
        _emit_runtime_event(
            "agent_request_start",
            agent_name=agent_name,
            model=resolved_model,
            provider="openai_compatible_stream",
            stream=True,
            prompt_chars=len(user_content or ""),
        )
        stream = _stream_openai_compatible(
            messages=messages,
            model_name=resolved_model,
            base_url=base_url,
            api_key=api_key,
            temperature=temperature,
            usage_collector=usage_collector,
        )
        return stream, resolved_model, "openai_compatible_stream", usage_collector
    finally:
        _TRACE_CONTEXT.reset(trace_token)
