#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import re
import shutil
import threading
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, jsonify, request, send_file, Response, stream_with_context
from werkzeug.utils import secure_filename

from utils.agents import (
    SUPPORTED_EXTENSIONS,
    chat_with_agent,
    chat_with_agent_stream,
    convert_file_to_markdown_via_skill,
    crawl_wechat_articles_from_text,
    list_agent_profiles,
)


strategy_watch_bp = Blueprint("strategy_watch", __name__)

BASE_DIR = Path(__file__).resolve().parent.parent
STORE_DIR = BASE_DIR / "data_cache" / "strategy_watch"
UPLOAD_DIR = STORE_DIR / "uploads"
MARKDOWN_DIR = STORE_DIR / "markdown"
CRAWLED_DIR = STORE_DIR / "crawled"
RESOURCES_FILE = STORE_DIR / "resources.json"
CONVERSATIONS_FILE = STORE_DIR / "conversations.json"
STRATEGIES_FILE = STORE_DIR / "strategies.json"
STRATEGY_INDEX_FILE = STORE_DIR / "strategies_index.json"
STRATEGY_DIR = STORE_DIR / "strategies"
MEMORY_PROFILES_FILE = STORE_DIR / "memory_profiles.json"
MEMORY_LINKS_FILE = STORE_DIR / "memory_links.json"
MEMORY_PORTRAITS_FILE = STORE_DIR / "memory_portraits.json"

MAX_CONTEXT_CHARS = 120_000
MAX_RESOURCE_CHARS = 30_000
MAX_HISTORY_MESSAGES = 24
MAX_MEMORY_CONTEXT_CHARS = 40_000
DEFAULT_GROUP_NAME = "未分组"
DEFAULT_STRATEGY_VIEW = "basic"

_DATE_YMD_SEP_PATTERN = re.compile(r"(?<!\d)(20\d{2})[-./年](\d{1,2})[-./月](\d{1,2})(?:日)?(?!\d)")
_DATE_YMD_COMPACT_PATTERN = re.compile(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)")
_DATE_YYMD_COMPACT_PATTERN = re.compile(r"(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)")

_STORE_LOCK = threading.Lock()
_JOB_LOCK = threading.Lock()
_JOBS: Dict[str, Dict] = {}


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


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_markdown_stem(filename: str) -> str:
    stem = Path(filename or "").stem.strip()
    if not stem:
        return "file"
    # Windows-invalid chars and control chars
    stem = re.sub('[<>:"/\\\\|?*\\x00-\\x1F]', "_", stem)
    stem = stem.strip(" .")
    if not stem:
        return "file"
    return stem[:80]


def _markdown_filename(resource_id: str, original_name: str) -> str:
    safe_stem = _safe_markdown_stem(original_name)
    return f"{safe_stem}__{resource_id}.md"


def _compose_user_content_with_prompt_template(user_content: str, prompt_template: str) -> str:
    content = (user_content or "").strip()
    template = (prompt_template or "").strip()
    if not template:
        return content
    return (
        "请严格参考以下提示词模板组织回答：\n"
        f"{template}\n\n"
        "用户本次问题：\n"
        f"{content}"
    )


def _ensure_store() -> None:
    STORE_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    MARKDOWN_DIR.mkdir(parents=True, exist_ok=True)
    CRAWLED_DIR.mkdir(parents=True, exist_ok=True)
    STRATEGY_DIR.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_date_str(year: int, month: int, day: int) -> str:
    try:
        return datetime(year, month, day, tzinfo=timezone.utc).date().isoformat()
    except Exception:
        return ""


def _extract_first_date(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""
    match = _DATE_YMD_SEP_PATTERN.search(text)
    if match:
        date_str = _safe_date_str(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        if date_str:
            return date_str, match.group(0)
    match = _DATE_YMD_COMPACT_PATTERN.search(text)
    if match:
        date_str = _safe_date_str(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        if date_str:
            return date_str, match.group(0)
    match = _DATE_YYMD_COMPACT_PATTERN.search(text)
    if match:
        yy = int(match.group(1))
        year = (2000 + yy) if yy <= 69 else (1900 + yy)
        date_str = _safe_date_str(year, int(match.group(2)), int(match.group(3)))
        if date_str:
            return date_str, match.group(0)
    return "", ""


def _normalize_date_only(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    lowered = raw.replace("Z", "+00:00")
    try:
        dt_obj = datetime.fromisoformat(lowered)
        return dt_obj.date().isoformat()
    except Exception:
        pass
    date_str, _ = _extract_first_date(raw)
    return date_str


def _normalize_datetime_to_iso(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    lowered = raw.replace("Z", "+00:00").replace("/", "-")
    try:
        return datetime.fromisoformat(lowered).astimezone(timezone.utc).isoformat()
    except Exception:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(lowered, fmt)
            if fmt == "%Y-%m-%d":
                parsed = parsed.replace(hour=0, minute=0, second=0)
            return parsed.replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            continue
    return ""


def _parse_frontmatter_map(text: str) -> Dict[str, str]:
    if not text.startswith("---"):
        return {}
    parts = text.split("---", 2)
    if len(parts) < 3:
        return {}
    front = parts[1]
    result: Dict[str, str] = {}
    for raw in front.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, val = line.split(":", 1)
        key = key.strip().lower()
        val = val.strip().strip('"').strip("'")
        if key:
            result[key] = val
    return result


def _read_markdown_text_by_relpath(rel_path: str) -> str:
    target = (rel_path or "").strip()
    if not target or "://" in target:
        return ""
    md_path = BASE_DIR / target
    if not md_path.exists() or not md_path.is_file():
        return ""
    return md_path.read_text(encoding="utf-8", errors="ignore")


def _enrich_resource_temporal_fields(record: Dict, markdown_text: str = "") -> bool:
    changed = False
    record.setdefault("content_time", "")
    record.setdefault("content_time_type", "unknown")
    record.setdefault("content_time_confidence", 0.0)
    record.setdefault("content_time_evidence", "")
    record.setdefault("published_at", "")

    if not isinstance(record.get("content_time_confidence"), (int, float)):
        record["content_time_confidence"] = 0.0
        changed = True

    normalized_published = _normalize_datetime_to_iso(record.get("published_at", ""))
    if normalized_published != (record.get("published_at") or ""):
        record["published_at"] = normalized_published
        changed = True

    normalized_content_time = _normalize_date_only(record.get("content_time", ""))
    if normalized_content_time != (record.get("content_time") or ""):
        record["content_time"] = normalized_content_time
        changed = True

    if not record.get("content_time"):
        inferred_date, hit = _extract_first_date(record.get("original_name") or "")
        if inferred_date:
            record["content_time"] = inferred_date
            record["content_time_type"] = "inferred_filename"
            record["content_time_confidence"] = 0.70
            record["content_time_evidence"] = f"filename:{hit}"
            changed = True

    text = markdown_text or ""
    if text:
        front = _parse_frontmatter_map(text)

        if not record.get("published_at"):
            for key in ("published_at", "publish_time", "published_time", "publishdate", "date"):
                candidate = _normalize_datetime_to_iso(front.get(key, ""))
                if candidate:
                    record["published_at"] = candidate
                    changed = True
                    break

        better_content_time = ""
        better_type = ""
        better_evidence = ""
        better_conf = 0.0

        for key in ("content_time", "recorded_at", "publish_date", "published_at", "date"):
            raw = front.get(key, "")
            normalized = _normalize_date_only(raw)
            if not normalized:
                continue
            if key in {"published_at", "publish_date"}:
                better_type = "published"
                better_conf = 0.95
            elif key in {"recorded_at"}:
                better_type = "recorded"
                better_conf = 0.90
            else:
                better_type = "inferred_text"
                better_conf = 0.82
            better_content_time = normalized
            better_evidence = f"frontmatter:{key}"
            break

        if not better_content_time:
            sample = "\n".join(
                [
                    front.get("title", ""),
                    text[:5000],
                ]
            )
            inferred_date, hit = _extract_first_date(sample)
            if inferred_date:
                better_content_time = inferred_date
                better_type = "inferred_text"
                better_conf = 0.55
                better_evidence = f"content:{hit}"

        current_type = (record.get("content_time_type") or "unknown").strip() or "unknown"
        priority = {
            "unknown": 0,
            "inferred_text": 1,
            "inferred_filename": 2,
            "recorded": 3,
            "published": 4,
        }
        if better_content_time:
            should_replace = not record.get("content_time")
            if not should_replace:
                should_replace = priority.get(better_type, 0) >= priority.get(current_type, 0)
            if should_replace:
                record["content_time"] = better_content_time
                record["content_time_type"] = better_type
                record["content_time_confidence"] = better_conf
                record["content_time_evidence"] = better_evidence
                changed = True

    if not record.get("content_time_type"):
        record["content_time_type"] = "unknown"
        changed = True

    if not record.get("content_time"):
        if record.get("content_time_confidence") != 0.0:
            record["content_time_confidence"] = 0.0
            changed = True
        if record.get("content_time_type") != "unknown":
            record["content_time_type"] = "unknown"
            changed = True

    return changed


def _normalize_resource_record(record: Dict) -> Dict:
    group_id = (record.get("group_id") or "").strip()
    group_name = (record.get("group_name") or "").strip()
    if group_name and not group_id:
        group_id = f"legacy_{group_name}"
    if not group_name:
        group_name = DEFAULT_GROUP_NAME
    if not group_id:
        group_id = f"legacy_{DEFAULT_GROUP_NAME}"
    record["group_id"] = group_id
    record["group_name"] = group_name
    record["strategy_id"] = (record.get("strategy_id") or "").strip()
    record["strategy_name"] = (record.get("strategy_name") or "").strip()
    record.setdefault("progress", 0)
    record.setdefault("progress_message", "")
    _enrich_resource_temporal_fields(record)
    return record


def _set_job(job_id: str, **fields) -> Dict:
    with _JOB_LOCK:
        job = _JOBS.get(job_id, {})
        job.update(fields)
        _JOBS[job_id] = job
        return dict(job)


def _get_job(job_id: str) -> Dict:
    with _JOB_LOCK:
        return dict(_JOBS.get(job_id) or {})


def _list_jobs() -> List[Dict]:
    with _JOB_LOCK:
        jobs = list(_JOBS.values())
    return sorted(jobs, key=lambda x: x.get("created_at", ""), reverse=True)


def _update_resource_fields(resource_id: str, **fields) -> None:
    with _STORE_LOCK:
        payload = _load_resources_payload()
        resources = payload.get("resources", [])
        updated = False
        for item in resources:
            if item.get("id") == resource_id:
                item.update(fields)
                updated = True
                break
        if updated:
            payload["resources"] = resources
            _save_resources_payload(payload)


def _process_resource_job(
    job_id: str,
    resource_id: str,
    upload_path: Path,
    markdown_path: Path,
    whisper_model: str,
) -> None:
    _set_job(
        job_id,
        status="running",
        progress=0,
        message="开始处理",
        started_at=_now_iso(),
    )
    _update_resource_fields(
        resource_id,
        status="processing",
        progress=0,
        progress_message="开始处理",
        error="",
    )

    last_percent = -1
    last_ts = 0.0

    def _progress_cb(current: int, total: int, message: str) -> None:
        nonlocal last_percent, last_ts
        if total <= 0:
            return
        percent = int(max(0, min(100, (current / total) * 100)))
        now_ts = time.time()
        if percent == last_percent and (now_ts - last_ts) < 0.6:
            return
        last_percent = percent
        last_ts = now_ts
        msg = (message or "").strip()
        _set_job(job_id, progress=percent, message=msg)
        _update_resource_fields(resource_id, progress=percent, progress_message=msg)

    status = "failed"
    error = ""
    try:
        status, error = convert_file_to_markdown_via_skill(
            input_file=upload_path,
            output_markdown=markdown_path,
            whisper_model=whisper_model,
            progress_callback=_progress_cb,
        )
    except Exception as exc:
        status = "failed"
        error = str(exc)

    final_status = "ok" if status == "ok" else "failed"
    final_message = "完成" if final_status == "ok" else (error or "转换失败")
    _set_job(
        job_id,
        status=final_status,
        progress=100,
        message=final_message,
        error=error,
        finished_at=_now_iso(),
    )
    _update_resource_fields(
        resource_id,
        status=final_status,
        progress=100,
        progress_message=final_message,
        error=error,
    )
    try:
        if upload_path.exists() and upload_path.is_file():
            upload_path.unlink()
    except Exception:
        pass


def _load_resources_payload() -> Dict:
    _ensure_store()
    raw = _read_json(RESOURCES_FILE, {"resources": [], "active_resource_id": ""})
    if isinstance(raw, list):
        raw = {"resources": raw, "active_resource_id": ""}
    resources = raw.get("resources", [])
    normalized: List[Dict] = []
    touched = False
    for item in resources:
        if isinstance(item, dict):
            normalized_item = _normalize_resource_record(item)
            md_rel = (normalized_item.get("markdown_relpath") or "").strip()
            should_probe_markdown = (
                (not normalized_item.get("content_time"))
                or (normalized_item.get("content_time_type") in {"unknown", "inferred_filename"})
                or (not normalized_item.get("published_at"))
            )
            if should_probe_markdown and md_rel:
                md_text = _read_markdown_text_by_relpath(md_rel)
                if md_text and _enrich_resource_temporal_fields(normalized_item, markdown_text=md_text):
                    touched = True
            normalized.append(normalized_item)
    active_resource_id = (raw.get("active_resource_id") or "").strip()
    if touched:
        _save_resources_payload({"resources": normalized, "active_resource_id": active_resource_id})
    return {"resources": normalized, "active_resource_id": active_resource_id}


def _save_resources_payload(payload: Dict) -> None:
    _ensure_store()
    _write_json(
        RESOURCES_FILE,
        {
            "resources": payload.get("resources", []),
            "active_resource_id": (payload.get("active_resource_id") or "").strip(),
        },
    )


def _load_resources() -> List[Dict]:
    return _load_resources_payload().get("resources", [])


def _save_resources(resources: List[Dict]) -> None:
    payload = _load_resources_payload()
    payload["resources"] = resources
    valid_ids = {item.get("id") for item in resources}
    if payload.get("active_resource_id") not in valid_ids:
        payload["active_resource_id"] = ""
    _save_resources_payload(payload)


def _load_conversations() -> List[Dict]:
    _ensure_store()
    payload = _read_json(CONVERSATIONS_FILE, {"conversations": []})
    return payload.get("conversations", [])


def _save_conversations(conversations: List[Dict]) -> None:
    _ensure_store()
    _write_json(CONVERSATIONS_FILE, {"conversations": conversations})


def _normalize_strategy_id(strategy_id: str, allow_empty: bool = False) -> str:
    raw = (strategy_id or "").strip()
    if not raw:
        if allow_empty:
            return ""
        raw = f"strategy_{uuid.uuid4().hex[:12]}"
    return "".join(ch if (ch.isalnum() or ch in {"-", "_"}) else "_" for ch in raw)


def _normalize_strategy_record(strategy: Dict) -> Dict:
    item = dict(strategy or {})
    item["id"] = _normalize_strategy_id(item.get("id", ""))
    item["name"] = (item.get("name") or "").strip() or item["id"]
    item["description"] = (item.get("description") or "").strip()
    item["view_type"] = (item.get("view_type") or DEFAULT_STRATEGY_VIEW).strip() or DEFAULT_STRATEGY_VIEW
    item["config"] = item.get("config") if isinstance(item.get("config"), dict) else {}
    item["created_at"] = (item.get("created_at") or _now_iso()).strip()
    item["updated_at"] = (item.get("updated_at") or item["created_at"]).strip()
    return item


def _strategy_file_path(strategy_id: str) -> Path:
    return STRATEGY_DIR / f"{_normalize_strategy_id(strategy_id)}.json"


def _load_strategies_payload() -> Dict:
    _ensure_store()
    strategies: List[Dict] = []
    active_strategy_id = ""

    # New layout: one JSON file per strategy + index file
    if STRATEGY_INDEX_FILE.exists():
        raw_index = _read_json(STRATEGY_INDEX_FILE, {"strategy_ids": [], "active_strategy_id": ""})
        strategy_ids = raw_index.get("strategy_ids", [])
        seen = set()

        if isinstance(strategy_ids, list):
            for sid in strategy_ids:
                sid = _normalize_strategy_id(str(sid))
                if sid in seen:
                    continue
                path = _strategy_file_path(sid)
                rec = _read_json(path, {})
                if isinstance(rec, dict):
                    normalized = _normalize_strategy_record(rec)
                    strategies.append(normalized)
                    seen.add(normalized["id"])

        # Backfill: include strategy files that are not present in index
        for path in STRATEGY_DIR.glob("*.json"):
            rec = _read_json(path, {})
            if not isinstance(rec, dict):
                continue
            normalized = _normalize_strategy_record(rec)
            if normalized["id"] in seen:
                continue
            strategies.append(normalized)
            seen.add(normalized["id"])

        active_strategy_id = _normalize_strategy_id(raw_index.get("active_strategy_id", ""), allow_empty=True)
        valid_ids = {item["id"] for item in strategies}
        if active_strategy_id not in valid_ids:
            active_strategy_id = strategies[0]["id"] if strategies else ""

        # Heal index/layout if needed
        _save_strategies_payload({"strategies": strategies, "active_strategy_id": active_strategy_id})
        return {"strategies": strategies, "active_strategy_id": active_strategy_id}

    # Legacy layout fallback: migrate from aggregated strategies.json
    raw = _read_json(STRATEGIES_FILE, {"strategies": [], "active_strategy_id": ""})
    if isinstance(raw, list):
        raw = {"strategies": raw, "active_strategy_id": ""}

    raw_strategies = raw.get("strategies", [])
    if isinstance(raw_strategies, list):
        for item in raw_strategies:
            if isinstance(item, dict):
                strategies.append(_normalize_strategy_record(item))

    active_strategy_id = _normalize_strategy_id(raw.get("active_strategy_id", ""), allow_empty=True)
    valid_ids = {item["id"] for item in strategies}
    if active_strategy_id not in valid_ids:
        active_strategy_id = strategies[0]["id"] if strategies else ""

    if strategies or active_strategy_id:
        _save_strategies_payload({"strategies": strategies, "active_strategy_id": active_strategy_id})

    return {"strategies": strategies, "active_strategy_id": active_strategy_id}


def _save_strategies_payload(payload: Dict) -> None:
    _ensure_store()
    raw_list = payload.get("strategies", [])
    if not isinstance(raw_list, list):
        raw_list = []

    # Keep input order while removing duplicated ids
    seen = set()
    strategies: List[Dict] = []
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        normalized = _normalize_strategy_record(item)
        sid = normalized["id"]
        if sid in seen:
            continue
        strategies.append(normalized)
        seen.add(sid)

    active_strategy_id = _normalize_strategy_id(payload.get("active_strategy_id", ""), allow_empty=True)
    valid_ids = {item["id"] for item in strategies}
    if active_strategy_id not in valid_ids:
        active_strategy_id = strategies[0]["id"] if strategies else ""

    current_files = {path.name for path in STRATEGY_DIR.glob("*.json")}
    target_files = set()
    ordered_ids = []

    for item in strategies:
        sid = item["id"]
        ordered_ids.append(sid)
        path = _strategy_file_path(sid)
        target_files.add(path.name)
        _write_json(path, item)

    # Remove stale strategy files
    for filename in current_files - target_files:
        stale_path = STRATEGY_DIR / filename
        try:
            stale_path.unlink()
        except Exception:
            pass

    # New index file
    _write_json(
        STRATEGY_INDEX_FILE,
        {
            "strategy_ids": ordered_ids,
            "active_strategy_id": active_strategy_id,
        },
    )

    # Backward-compatible mirror for existing external readers
    _write_json(
        STRATEGIES_FILE,
        {
            "strategies": strategies,
            "active_strategy_id": active_strategy_id,
        },
    )


def _normalize_memory_subscription(item: Dict) -> Optional[Dict]:
    if not isinstance(item, dict):
        return None
    group_id = (item.get("group_id") or "").strip()
    if not group_id:
        return None
    first_bound_at = (item.get("first_bound_at") or _now_iso()).strip()
    last_sync_at = (item.get("last_sync_at") or "").strip()
    return {
        "group_id": group_id,
        "first_bound_at": first_bound_at,
        "last_sync_at": last_sync_at,
    }


def _normalize_memory_profile_record(profile: Dict) -> Dict:
    raw_id = (profile.get("id") or "").strip()
    normalized_id = _normalize_strategy_id(raw_id, allow_empty=True) or _gen_id("mp")
    subs_raw = profile.get("group_subscriptions") if isinstance(profile.get("group_subscriptions"), list) else []
    normalized_subs: List[Dict] = []
    seen_subs = set()
    for item in subs_raw:
        sub = _normalize_memory_subscription(item)
        if not sub:
            continue
        gid = sub["group_id"]
        if gid in seen_subs:
            continue
        seen_subs.add(gid)
        normalized_subs.append(sub)
    created_at = (profile.get("created_at") or _now_iso()).strip()
    updated_at = (profile.get("updated_at") or created_at).strip()
    return {
        "id": normalized_id,
        "name": (profile.get("name") or "").strip() or normalized_id,
        "description": (profile.get("description") or "").strip(),
        "source_blogger": (profile.get("source_blogger") or "").strip(),
        "created_at": created_at,
        "updated_at": updated_at,
        "group_subscriptions": normalized_subs,
    }


def _load_memory_profiles_payload() -> Dict:
    _ensure_store()
    raw = _read_json(MEMORY_PROFILES_FILE, {"profiles": [], "active_profile_id": ""})
    if isinstance(raw, list):
        raw = {"profiles": raw, "active_profile_id": ""}

    profiles: List[Dict] = []
    seen = set()
    for item in (raw.get("profiles") or []):
        if not isinstance(item, dict):
            continue
        normalized = _normalize_memory_profile_record(item)
        pid = normalized["id"]
        if pid in seen:
            continue
        seen.add(pid)
        profiles.append(normalized)

    active_profile_id = (raw.get("active_profile_id") or "").strip()
    valid_ids = {item["id"] for item in profiles}
    if active_profile_id not in valid_ids:
        active_profile_id = ""
    return {"profiles": profiles, "active_profile_id": active_profile_id}


def _save_memory_profiles_payload(payload: Dict) -> None:
    _ensure_store()
    raw_list = payload.get("profiles")
    if not isinstance(raw_list, list):
        raw_list = []
    profiles: List[Dict] = []
    seen = set()
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        normalized = _normalize_memory_profile_record(item)
        pid = normalized["id"]
        if pid in seen:
            continue
        seen.add(pid)
        profiles.append(normalized)
    active_profile_id = (payload.get("active_profile_id") or "").strip()
    valid_ids = {item["id"] for item in profiles}
    if active_profile_id not in valid_ids:
        active_profile_id = ""
    _write_json(
        MEMORY_PROFILES_FILE,
        {
            "profiles": profiles,
            "active_profile_id": active_profile_id,
        },
    )


def _normalize_memory_link(item: Dict) -> Optional[Dict]:
    if not isinstance(item, dict):
        return None
    profile_id = (item.get("profile_id") or "").strip()
    resource_id = (item.get("resource_id") or "").strip()
    if not profile_id or not resource_id:
        return None
    bind_source = (item.get("bind_source") or "manual").strip().lower()
    if bind_source not in {"manual", "group"}:
        bind_source = "manual"
    group_id = (item.get("group_id") or "").strip()
    added_at = (item.get("added_at") or _now_iso()).strip()
    return {
        "profile_id": profile_id,
        "resource_id": resource_id,
        "bind_source": bind_source,
        "group_id": group_id,
        "added_at": added_at,
    }


def _load_memory_links() -> List[Dict]:
    _ensure_store()
    raw = _read_json(MEMORY_LINKS_FILE, {"links": []})
    if isinstance(raw, list):
        raw = {"links": raw}
    links: List[Dict] = []
    seen = set()
    for item in (raw.get("links") or []):
        normalized = _normalize_memory_link(item)
        if not normalized:
            continue
        key = (normalized["profile_id"], normalized["resource_id"])
        if key in seen:
            continue
        seen.add(key)
        links.append(normalized)
    return links


def _save_memory_links(links: List[Dict]) -> None:
    _ensure_store()
    normalized_links: List[Dict] = []
    seen = set()
    for item in (links or []):
        normalized = _normalize_memory_link(item)
        if not normalized:
            continue
        key = (normalized["profile_id"], normalized["resource_id"])
        if key in seen:
            continue
        seen.add(key)
        normalized_links.append(normalized)
    _write_json(MEMORY_LINKS_FILE, {"links": normalized_links})


def _default_memory_portrait(profile_id: str) -> Dict:
    return {
        "profile_id": profile_id,
        "methodology": "",
        "tactics": "",
        "views": "",
        "operations": "",
        "risk_rules": "",
        "style_constraints": "",
        "evidence_refs": [],
        "updated_at": _now_iso(),
        "updated_by": "",
    }


def _normalize_evidence_ref(item: Dict) -> Optional[Dict]:
    if not isinstance(item, dict):
        return None
    resource_id = (item.get("resource_id") or "").strip()
    quote = (item.get("quote") or "").strip()
    if not resource_id and not quote:
        return None
    return {
        "id": (item.get("id") or _gen_id("ev")).strip(),
        "resource_id": resource_id,
        "topic": (item.get("topic") or "").strip(),
        "quote": quote,
        "source_time": _normalize_date_only(item.get("source_time") or ""),
        "source_time_type": (item.get("source_time_type") or "").strip(),
        "timecode": (item.get("timecode") or "").strip(),
    }


def _normalize_memory_portrait(item: Dict) -> Optional[Dict]:
    if not isinstance(item, dict):
        return None
    profile_id = (item.get("profile_id") or "").strip()
    if not profile_id:
        return None
    normalized = _default_memory_portrait(profile_id)
    for key in ("methodology", "tactics", "views", "operations", "risk_rules", "style_constraints"):
        normalized[key] = (item.get(key) or "").strip()
    refs_raw = item.get("evidence_refs") if isinstance(item.get("evidence_refs"), list) else []
    refs: List[Dict] = []
    seen = set()
    for ref in refs_raw:
        normalized_ref = _normalize_evidence_ref(ref)
        if not normalized_ref:
            continue
        rid = normalized_ref["id"]
        if rid in seen:
            continue
        seen.add(rid)
        refs.append(normalized_ref)
    normalized["evidence_refs"] = refs
    normalized["updated_at"] = (item.get("updated_at") or _now_iso()).strip()
    normalized["updated_by"] = (item.get("updated_by") or "").strip()
    return normalized


def _load_memory_portraits() -> Dict[str, Dict]:
    _ensure_store()
    raw = _read_json(MEMORY_PORTRAITS_FILE, {"portraits": []})
    if isinstance(raw, list):
        raw = {"portraits": raw}
    portraits: Dict[str, Dict] = {}
    for item in (raw.get("portraits") or []):
        normalized = _normalize_memory_portrait(item)
        if not normalized:
            continue
        portraits[normalized["profile_id"]] = normalized
    return portraits


def _save_memory_portraits(portraits: Dict[str, Dict]) -> None:
    _ensure_store()
    if not isinstance(portraits, dict):
        portraits = {}
    records: List[Dict] = []
    for profile_id, value in portraits.items():
        rec = dict(value or {})
        rec["profile_id"] = profile_id
        normalized = _normalize_memory_portrait(rec)
        if normalized:
            records.append(normalized)
    _write_json(MEMORY_PORTRAITS_FILE, {"portraits": records})


def _find_memory_profile(profile_id: str) -> Optional[Dict]:
    target_id = (profile_id or "").strip()
    if not target_id:
        return None
    payload = _load_memory_profiles_payload()
    return _find_by_id(payload.get("profiles", []), target_id)


def _collect_profile_resource_snippets(
    profile_id: str,
    max_chars: int = 24000,
    max_items: int = 10,
    per_item_chars: int = 2200,
) -> Tuple[str, List[str], List[Dict]]:
    links = [x for x in _load_memory_links() if x.get("profile_id") == profile_id]
    links.sort(key=lambda x: x.get("added_at", ""), reverse=True)
    resources_payload = _load_resources_payload()
    resources = resources_payload.get("resources", [])
    resource_map = {item.get("id"): item for item in resources}

    used_ids: List[str] = []
    skipped: List[Dict] = []
    blocks: List[str] = []
    total = 0
    for link in links:
        if len(used_ids) >= max_items:
            break
        rid = (link.get("resource_id") or "").strip()
        item = resource_map.get(rid)
        if not item:
            skipped.append({"resource_id": rid, "reason": "资源不存在"})
            continue
        if (item.get("status") or "") != "ok":
            skipped.append({"resource_id": rid, "reason": f"资源状态异常: {item.get('status') or 'unknown'}"})
            continue
        md_text = _read_markdown_content(item).strip()
        if not md_text:
            skipped.append({"resource_id": rid, "reason": "markdown 文件不存在或为空"})
            continue
        if not _markdown_has_meaningful_content(md_text):
            skipped.append({"resource_id": rid, "reason": "markdown 信息量不足"})
            continue

        snippet = md_text[:per_item_chars]
        name = item.get("original_name") or rid
        content_time = item.get("content_time") or ""
        time_label = f"（内容时间: {content_time}）" if content_time else ""
        block = f"### 资料: {name}{time_label}\n\n{snippet}"
        projected = total + len(block)
        if projected > max_chars:
            remain = max_chars - total
            if remain <= 0:
                break
            block = block[:remain]
            projected = total + len(block)
        blocks.append(block)
        used_ids.append(rid)
        total = projected
        if total >= max_chars:
            break
    return "\n\n".join(blocks).strip(), used_ids, skipped


def _build_memory_profile_context(memory_profile_id: str) -> Tuple[str, Dict[str, Any]]:
    profile_id = (memory_profile_id or "").strip()
    if not profile_id:
        return "", {"memory_profile_id": "", "used_memory_resource_ids": [], "skipped_memory_refs": []}

    profiles_payload = _load_memory_profiles_payload()
    profiles = profiles_payload.get("profiles", [])
    profile = _find_by_id(profiles, profile_id)
    if not profile:
        return "", {
            "memory_profile_id": "",
            "used_memory_resource_ids": [],
            "skipped_memory_refs": [{"profile_id": profile_id, "reason": "人格不存在"}],
        }

    portraits = _load_memory_portraits()
    portrait = portraits.get(profile_id) or _default_memory_portrait(profile_id)

    lines: List[str] = [
        f"长期记忆人格：{profile.get('name') or profile_id}",
        f"- 人格ID: {profile_id}",
    ]
    description = (profile.get("description") or "").strip()
    if description:
        lines.append(f"- 人格说明: {description}")
    source_blogger = (profile.get("source_blogger") or "").strip()
    if source_blogger:
        lines.append(f"- 来源博主: {source_blogger}")

    sections = [
        ("交易方法论", "methodology"),
        ("交易手法", "tactics"),
        ("观点", "views"),
        ("交易操作", "operations"),
        ("风控规则", "risk_rules"),
        ("风格约束", "style_constraints"),
    ]
    for title, key in sections:
        text = (portrait.get(key) or "").strip()
        if not text:
            continue
        lines.extend(["", f"## {title}", text])

    evidence_refs = portrait.get("evidence_refs") if isinstance(portrait.get("evidence_refs"), list) else []
    if evidence_refs:
        lines.extend(["", "## 证据索引"])
        for idx, ref in enumerate(evidence_refs, start=1):
            resource_id = (ref.get("resource_id") or "").strip() if isinstance(ref, dict) else ""
            quote = (ref.get("quote") or "").strip() if isinstance(ref, dict) else ""
            topic = (ref.get("topic") or "").strip() if isinstance(ref, dict) else ""
            source_time = (ref.get("source_time") or "").strip() if isinstance(ref, dict) else ""
            timecode = (ref.get("timecode") or "").strip() if isinstance(ref, dict) else ""
            meta = " | ".join(
                [x for x in [topic, source_time, timecode, resource_id] if x]
            )
            if quote:
                lines.append(f"{idx}. {quote} {'（' + meta + '）' if meta else ''}")
            elif meta:
                lines.append(f"{idx}. {meta}")

    base_context = "\n".join(lines).strip()
    remain_chars = max(0, MAX_MEMORY_CONTEXT_CHARS - len(base_context) - 2_000)
    snippets, used_ids, skipped = _collect_profile_resource_snippets(
        profile_id,
        max_chars=max(8_000, remain_chars),
        max_items=8,
        per_item_chars=1_800,
    )
    if snippets:
        base_context += "\n\n## 关联资料摘录\n\n" + snippets

    if len(base_context) > MAX_MEMORY_CONTEXT_CHARS:
        base_context = base_context[:MAX_MEMORY_CONTEXT_CHARS]

    return base_context.strip(), {
        "memory_profile_id": profile_id,
        "memory_profile_name": profile.get("name") or profile_id,
        "used_memory_resource_ids": used_ids,
        "skipped_memory_refs": skipped,
    }


def _memory_portrait_sections(portrait: Dict) -> List[Tuple[str, str]]:
    source = portrait if isinstance(portrait, dict) else {}
    return [
        ("交易方法论", str(source.get("methodology") or "").strip()),
        ("交易手法", str(source.get("tactics") or "").strip()),
        ("观点", str(source.get("views") or "").strip()),
        ("交易操作", str(source.get("operations") or "").strip()),
        ("风控规则", str(source.get("risk_rules") or "").strip()),
        ("风格约束", str(source.get("style_constraints") or "").strip()),
    ]


def _merge_portrait_override(base_portrait: Dict, override: Any) -> Dict:
    merged = dict(base_portrait or {})
    if not isinstance(override, dict):
        return merged
    key_map = {
        "methodology": "methodology",
        "tactics": "tactics",
        "views": "views",
        "operations": "operations",
        "risk_rules": "risk_rules",
        "style_constraints": "style_constraints",
        "交易方法论": "methodology",
        "交易手法": "tactics",
        "观点": "views",
        "交易操作": "operations",
        "风控规则": "risk_rules",
        "风格约束": "style_constraints",
    }
    for raw_key, value in override.items():
        key = key_map.get(str(raw_key).strip())
        if key:
            merged[key] = str(value or "").strip()
    return merged


def _build_memory_portrait_markdown(profile: Dict, portrait: Dict) -> str:
    name = (profile.get("name") or "").strip() or "未命名人格"
    profile_id = (profile.get("id") or "").strip()
    description = (profile.get("description") or "").strip()
    source_blogger = (profile.get("source_blogger") or "").strip()
    updated_at = (portrait.get("updated_at") or "").strip()
    updated_by = (portrait.get("updated_by") or "").strip()

    lines: List[str] = [
        f"# 人物侧写：{name}",
        "",
        f"- 人格ID: {profile_id or '--'}",
        f"- 导出时间(UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 侧写更新时间: {updated_at or '--'}",
        f"- 侧写来源: {updated_by or '--'}",
    ]
    if description:
        lines.append(f"- 人格说明: {description}")
    if source_blogger:
        lines.append(f"- 来源博主: {source_blogger}")
    lines.append("")

    for title, body in _memory_portrait_sections(portrait):
        lines.append(f"## {title}")
        lines.append(body or "（空）")
        lines.append("")

    evidence_refs = portrait.get("evidence_refs") if isinstance(portrait.get("evidence_refs"), list) else []
    if evidence_refs:
        lines.append("## 证据引用")
        for idx, item in enumerate(evidence_refs, start=1):
            if not isinstance(item, dict):
                continue
            rid = str(item.get("resource_id") or "--").strip() or "--"
            topic = str(item.get("topic") or "--").strip() or "--"
            quote = str(item.get("quote") or "").strip()
            source_time = str(item.get("source_time") or "").strip()
            source_type = str(item.get("source_time_type") or "").strip()
            timecode = str(item.get("timecode") or "").strip()
            lines.append(f"{idx}. [{topic}] 资源: {rid}")
            if quote:
                lines.append(f"   - 引文: {quote}")
            if source_time:
                lines.append(f"   - 时间: {source_time} ({source_type or 'unknown'})")
            if timecode:
                lines.append(f"   - 片段: {timecode}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def _resolve_portrait_pdf_font() -> Tuple[str, Optional[Path]]:
    candidates = [
        Path("C:/Windows/Fonts/msyh.ttc"),
        Path("C:/Windows/Fonts/msyh.ttf"),
        Path("C:/Windows/Fonts/msyhbd.ttc"),
        Path("C:/Windows/Fonts/msyhbd.ttf"),
        Path("C:/Windows/Fonts/simhei.ttf"),
        Path("C:/Windows/Fonts/simsun.ttc"),
    ]
    for path in candidates:
        if path.exists():
            return "PortraitFont", path
    return "Helvetica", None


def _render_memory_portrait_docx(profile: Dict, portrait: Dict) -> bytes:
    try:
        from docx import Document
    except Exception as exc:
        raise RuntimeError("缺少 Word 导出依赖 python-docx") from exc

    doc = Document()
    name = (profile.get("name") or "").strip() or "未命名人格"
    doc.add_heading(f"人物侧写：{name}", level=1)
    doc.add_paragraph(f"人格ID：{(profile.get('id') or '').strip() or '--'}")
    doc.add_paragraph(f"导出时间(UTC)：{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    doc.add_paragraph(f"侧写更新时间：{(portrait.get('updated_at') or '').strip() or '--'}")
    doc.add_paragraph(f"侧写来源：{(portrait.get('updated_by') or '').strip() or '--'}")
    if (profile.get("description") or "").strip():
        doc.add_paragraph(f"人格说明：{(profile.get('description') or '').strip()}")
    if (profile.get("source_blogger") or "").strip():
        doc.add_paragraph(f"来源博主：{(profile.get('source_blogger') or '').strip()}")
    doc.add_paragraph("")

    for title, body in _memory_portrait_sections(portrait):
        doc.add_heading(title, level=2)
        for line in (body or "（空）").splitlines():
            doc.add_paragraph(line)

    evidence_refs = portrait.get("evidence_refs") if isinstance(portrait.get("evidence_refs"), list) else []
    if evidence_refs:
        doc.add_heading("证据引用", level=2)
        for idx, item in enumerate(evidence_refs, start=1):
            if not isinstance(item, dict):
                continue
            rid = str(item.get("resource_id") or "--").strip() or "--"
            topic = str(item.get("topic") or "--").strip() or "--"
            quote = str(item.get("quote") or "").strip()
            source_time = str(item.get("source_time") or "").strip()
            source_type = str(item.get("source_time_type") or "").strip()
            timecode = str(item.get("timecode") or "").strip()
            doc.add_paragraph(f"{idx}. [{topic}] 资源: {rid}")
            if quote:
                doc.add_paragraph(f"引文: {quote}")
            if source_time:
                doc.add_paragraph(f"时间: {source_time} ({source_type or 'unknown'})")
            if timecode:
                doc.add_paragraph(f"片段: {timecode}")

    out = BytesIO()
    doc.save(out)
    return out.getvalue()


def _render_memory_portrait_pdf(profile: Dict, portrait: Dict) -> bytes:
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer
    except Exception as exc:
        raise RuntimeError("缺少 PDF 导出依赖 reportlab") from exc

    font_name, font_path = _resolve_portrait_pdf_font()
    if font_path:
        try:
            pdfmetrics.registerFont(TTFont(font_name, str(font_path)))
        except Exception:
            font_name = "Helvetica"

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("portrait_title", parent=styles["Heading1"], fontName=font_name, fontSize=16, leading=20, spaceAfter=10)
    h2_style = ParagraphStyle("portrait_h2", parent=styles["Heading2"], fontName=font_name, fontSize=13, leading=18, spaceAfter=6)
    body_style = ParagraphStyle("portrait_body", parent=styles["Normal"], fontName=font_name, fontSize=10.5, leading=16, spaceAfter=3)
    meta_style = ParagraphStyle("portrait_meta", parent=styles["Normal"], fontName=font_name, fontSize=9, leading=13, textColor="#5a6570", spaceAfter=2)

    def _safe(text: str) -> str:
        return str(text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    story: List[Any] = []
    name = (profile.get("name") or "").strip() or "未命名人格"
    story.append(Paragraph(_safe(f"人物侧写：{name}"), title_style))
    story.append(Paragraph(_safe(f"人格ID：{(profile.get('id') or '').strip() or '--'}"), meta_style))
    story.append(Paragraph(_safe(f"导出时间(UTC)：{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}"), meta_style))
    story.append(Paragraph(_safe(f"侧写更新时间：{(portrait.get('updated_at') or '').strip() or '--'}"), meta_style))
    story.append(Paragraph(_safe(f"侧写来源：{(portrait.get('updated_by') or '').strip() or '--'}"), meta_style))
    if (profile.get("description") or "").strip():
        story.append(Paragraph(_safe(f"人格说明：{(profile.get('description') or '').strip()}"), meta_style))
    if (profile.get("source_blogger") or "").strip():
        story.append(Paragraph(_safe(f"来源博主：{(profile.get('source_blogger') or '').strip()}"), meta_style))
    story.append(Spacer(1, 8))

    for title, body in _memory_portrait_sections(portrait):
        story.append(Paragraph(_safe(title), h2_style))
        text = body or "（空）"
        for line in text.splitlines():
            story.append(Paragraph(_safe(line), body_style))
        story.append(Spacer(1, 4))

    evidence_refs = portrait.get("evidence_refs") if isinstance(portrait.get("evidence_refs"), list) else []
    if evidence_refs:
        story.append(Paragraph(_safe("证据引用"), h2_style))
        for idx, item in enumerate(evidence_refs, start=1):
            if not isinstance(item, dict):
                continue
            rid = str(item.get("resource_id") or "--").strip() or "--"
            topic = str(item.get("topic") or "--").strip() or "--"
            quote = str(item.get("quote") or "").strip()
            source_time = str(item.get("source_time") or "").strip()
            source_type = str(item.get("source_time_type") or "").strip()
            timecode = str(item.get("timecode") or "").strip()
            story.append(Paragraph(_safe(f"{idx}. [{topic}] 资源: {rid}"), body_style))
            if quote:
                story.append(Paragraph(_safe(f"引文: {quote}"), body_style))
            if source_time:
                story.append(Paragraph(_safe(f"时间: {source_time} ({source_type or 'unknown'})"), body_style))
            if timecode:
                story.append(Paragraph(_safe(f"片段: {timecode}"), body_style))
        story.append(Spacer(1, 4))

    out = BytesIO()
    doc = SimpleDocTemplate(out, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=36, bottomMargin=36, title=name, author="strategy_watch")
    doc.build(story)
    return out.getvalue()


def _bind_resources_to_profile(
    profile_id: str,
    resource_ids: List[str],
    bind_source: str = "manual",
    group_id: str = "",
) -> Dict[str, Any]:
    target_ids = [str(x).strip() for x in (resource_ids or []) if str(x).strip()]
    target_ids = list(dict.fromkeys(target_ids))

    resources_payload = _load_resources_payload()
    resource_map = {item.get("id"): item for item in resources_payload.get("resources", [])}
    links = _load_memory_links()
    existing = {(x.get("profile_id"), x.get("resource_id")) for x in links}

    added = 0
    skipped: List[Dict] = []
    for rid in target_ids:
        item = resource_map.get(rid)
        if not item:
            skipped.append({"resource_id": rid, "reason": "资源不存在"})
            continue
        key = (profile_id, rid)
        if key in existing:
            skipped.append({"resource_id": rid, "reason": "已在人格长期记忆中"})
            continue
        links.append(
            {
                "profile_id": profile_id,
                "resource_id": rid,
                "bind_source": bind_source,
                "group_id": group_id,
                "added_at": _now_iso(),
            }
        )
        existing.add(key)
        added += 1

    _save_memory_links(links)
    return {"added": added, "skipped": skipped}


def _upsert_profile_subscription(profile: Dict, group_id: str, sync_now: bool = False) -> bool:
    group_id = (group_id or "").strip()
    if not group_id:
        return False
    subs = profile.get("group_subscriptions") if isinstance(profile.get("group_subscriptions"), list) else []
    now = _now_iso()
    for sub in subs:
        if not isinstance(sub, dict):
            continue
        if (sub.get("group_id") or "").strip() == group_id:
            if sync_now:
                sub["last_sync_at"] = now
            profile["group_subscriptions"] = subs
            return False
    subs.append(
        {
            "group_id": group_id,
            "first_bound_at": now,
            "last_sync_at": now if sync_now else "",
        }
    )
    profile["group_subscriptions"] = subs
    return True


def _sync_profile_group_resources(profile: Dict, group_id: str) -> Dict[str, Any]:
    gid = (group_id or "").strip()
    if not gid:
        return {"added": 0, "skipped": [{"group_id": gid, "reason": "缺少 group_id"}]}
    resources_payload = _load_resources_payload()
    group_resource_ids = [
        (item.get("id") or "").strip()
        for item in resources_payload.get("resources", [])
        if (item.get("group_id") or "").strip() == gid and (item.get("id") or "").strip()
    ]
    result = _bind_resources_to_profile(
        profile_id=profile.get("id") or "",
        resource_ids=group_resource_ids,
        bind_source="group",
        group_id=gid,
    )
    _upsert_profile_subscription(profile, gid, sync_now=True)
    return result


def _memory_profile_view(profile: Dict, links: List[Dict], portraits: Dict[str, Dict]) -> Dict:
    pid = (profile.get("id") or "").strip()
    linked_ids = [item.get("resource_id") for item in links if item.get("profile_id") == pid]
    portrait = portraits.get(pid) or _default_memory_portrait(pid)
    subscription_ids = []
    for sub in (profile.get("group_subscriptions") or []):
        if isinstance(sub, dict) and (sub.get("group_id") or "").strip():
            subscription_ids.append((sub.get("group_id") or "").strip())
    return {
        **profile,
        "linked_resource_count": len(set(linked_ids)),
        "group_subscription_count": len(set(subscription_ids)),
        "portrait_updated_at": portrait.get("updated_at") or "",
    }


def _extract_json_block(text: str) -> Dict:
    if not text:
        return {}
    cleaned = text.strip()
    import re

    # Try plain JSON first.
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    # Handle fenced code block content like ```json ... ```.
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if len(lines) >= 3 and lines[0].startswith("```") and lines[-1].strip().startswith("```"):
            body = "\n".join(lines[1:-1]).strip()
            try:
                parsed = json.loads(body)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass

    fenced = re.search(r"```json\\s*({[\\s\\S]*?})\\s*```", text, re.IGNORECASE)
    if fenced:
        try:
            return json.loads(fenced.group(1))
        except Exception:
            return {}

    generic = re.search(r"({[\\s\\S]*})", text)
    if generic:
        try:
            return json.loads(generic.group(1))
        except Exception:
            return {}

    return {}


def _normalize_portrait_draft_payload(parsed: Dict, reply: str = "") -> Dict:
    if not isinstance(parsed, dict):
        parsed = {}

    key_alias = {
        "交易方法论": "methodology",
        "交易手法": "tactics",
        "观点": "views",
        "交易操作": "operations",
        "风控规则": "risk_rules",
        "风格约束": "style_constraints",
        "evidence_refs": "evidence_refs",
    }
    normalized: Dict[str, Any] = {}
    for raw_key, value in parsed.items():
        key = key_alias.get(str(raw_key).strip(), str(raw_key).strip())
        normalized[key] = value

    # Some models put the full JSON object string inside "methodology".
    nested_text = normalized.get("methodology")
    has_other_sections = any(
        str(normalized.get(k) or "").strip()
        for k in ("tactics", "views", "operations", "risk_rules", "style_constraints")
    )
    if isinstance(nested_text, str) and nested_text.strip() and not has_other_sections:
        nested = _extract_json_block(nested_text)
        if isinstance(nested, dict) and nested:
            nested_norm = _normalize_portrait_draft_payload(nested, "")
            if any(
                str(nested_norm.get(k) or "").strip()
                for k in ("tactics", "views", "operations", "risk_rules", "style_constraints")
            ):
                normalized = nested_norm

    if not normalized and reply:
        fallback = _extract_json_block(reply)
        if isinstance(fallback, dict) and fallback:
            normalized = _normalize_portrait_draft_payload(fallback, "")

    return normalized


def _build_visualization_prompt(goal: str, view_type: str = "") -> str:
    view_hint = view_type or DEFAULT_STRATEGY_VIEW
    return f"""
你是A股可视化规划助理。请根据目标生成“看盘视图配置”的JSON，只返回JSON，不要附加解释。

目标：{goal}
偏好视图类型：{view_hint}

允许的widget.type：
- market_sentiment_bundle（市场情绪组合图：红盘率/涨跌停/连板/涨跌幅分布）
- index_kline（指数K线）
- market_volume（市场量能对比）

JSON结构示例：
{{
  "name": "策略名称",
  "description": "策略说明",
  "view_type": "basic",
  "widgets": [
    {{
      "id": "market-sentiment",
      "type": "market_sentiment_bundle",
      "title": "市场情绪组合",
      "params": {{ "days_back": 30 }}
    }}
  ]
}}
""".strip()


def _gen_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _find_by_id(items: List[Dict], item_id: str) -> Dict:
    return next((item for item in items if item.get("id") == item_id), None)


def _read_markdown_content(resource: Dict) -> str:
    md_rel = resource.get("markdown_relpath") or ""
    if not md_rel:
        return ""
    md_path = BASE_DIR / md_rel
    if not md_path.exists():
        return ""
    return md_path.read_text(encoding="utf-8", errors="ignore")


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


def _markdown_has_meaningful_content(md_content: str, min_chars: int = 20) -> bool:
    if not md_content:
        return False
    marker = "\n## Content\n"
    content = md_content.split(marker, 1)[1] if marker in md_content else md_content
    if not content.strip():
        return False
    content = _strip_likely_page_headings(content)
    compact = re.sub(r"\s+", "", content)
    meaningful = re.sub(r"[^\u4e00-\u9fffA-Za-z0-9]", "", compact)
    return len(meaningful) >= min_chars


def _build_strategy_reference_markdown(strategy: Dict) -> str:
    sid = (strategy.get("id") or "").strip()
    name = (strategy.get("name") or "").strip() or sid
    description = (strategy.get("description") or "").strip()
    view_type = (strategy.get("view_type") or DEFAULT_STRATEGY_VIEW).strip() or DEFAULT_STRATEGY_VIEW
    updated_at = (strategy.get("updated_at") or "").strip()
    config = strategy.get("config") if isinstance(strategy.get("config"), dict) else {}
    widgets = config.get("widgets") if isinstance(config, dict) else None

    lines: List[str] = [
        f"# 策略：{name}",
        "",
        "## 展示内容",
        f"- 策略ID：{sid}",
        f"- 视图类型：{view_type}",
        f"- 更新时间：{updated_at or '(unknown)'}",
    ]
    if description:
        lines.append(f"- 说明：{description}")
    else:
        lines.append("- 说明：暂无")

    lines.extend(["", "## 策略逻辑"])
    if isinstance(widgets, list) and widgets:
        for idx, widget in enumerate(widgets, start=1):
            w_type = (widget.get("type") or "").strip() if isinstance(widget, dict) else ""
            w_title = (widget.get("title") or "").strip() if isinstance(widget, dict) else ""
            w_id = (widget.get("id") or "").strip() if isinstance(widget, dict) else ""
            params = widget.get("params") if isinstance(widget, dict) else {}
            params_json = json.dumps(params if isinstance(params, dict) else {}, ensure_ascii=False, separators=(",", ":"))
            lines.append(
                f"{idx}. {w_title or w_type or w_id or 'widget'}"
                f"（type={w_type or 'unknown'}, id={w_id or 'unknown'}, params={params_json}）"
            )
    else:
        lines.append("1. 当前策略未配置 widgets，默认由 view_type 决定展示内容。")

    lines.extend(["", "## 原始配置", "```json", json.dumps(config, ensure_ascii=False, indent=2), "```"])
    return "\n".join(lines).strip()


def _build_resource_context(resource_ids: List[str]) -> Tuple[str, List[str], List[Dict]]:
    payload = _load_resources_payload()
    resources = payload.get("resources", [])
    active_resource_id = payload.get("active_resource_id") or ""
    resource_map = {r.get("id"): r for r in resources}
    strategies_payload = _load_strategies_payload()
    strategies = strategies_payload.get("strategies", [])
    strategy_map = {s.get("id"): s for s in strategies if s.get("id")}

    selected_ids = resource_ids or ([active_resource_id] if active_resource_id else [])
    if not selected_ids:
        selected_ids = [r.get("id") for r in resources if r.get("status") == "ok"]
    total_len = 0
    used_ids: List[str] = []
    skipped: List[Dict] = []
    blocks: List[str] = []

    for rid in selected_ids:
        if isinstance(rid, str) and rid.startswith("strategy_ctx:"):
            strategy_id = rid.split(":", 1)[1].strip()
            strategy = strategy_map.get(strategy_id)
            if not strategy:
                skipped.append({"resource_id": rid, "reason": "策略不存在"})
                continue

            strategy_md = _build_strategy_reference_markdown(strategy)
            snippet = strategy_md[:MAX_RESOURCE_CHARS]
            projected = total_len + len(snippet)
            if projected > MAX_CONTEXT_CHARS:
                remain = MAX_CONTEXT_CHARS - total_len
                if remain <= 0:
                    break
                snippet = snippet[:remain]
                projected = total_len + len(snippet)

            strategy_name = (strategy.get("name") or strategy_id).strip()
            blocks.append(f"## Strategy Reference: {strategy_name}\n\n{snippet}")
            used_ids.append(rid)
            total_len = projected
            if total_len >= MAX_CONTEXT_CHARS:
                break
            continue

        item = resource_map.get(rid)
        if not item:
            skipped.append({"resource_id": rid, "reason": "资源不存在"})
            continue
        if (item.get("status") or "") != "ok":
            skipped.append({"resource_id": rid, "reason": f"资源状态异常: {item.get('status') or 'unknown'}"})
            continue

        md_content = _read_markdown_content(item).strip()
        if not md_content:
            skipped.append({"resource_id": rid, "reason": "markdown 文件不存在或为空"})
            continue
        if not _markdown_has_meaningful_content(md_content):
            skipped.append({"resource_id": rid, "reason": "markdown 信息量不足（疑似空壳转换结果）"})
            continue

        snippet = md_content[:MAX_RESOURCE_CHARS]
        projected = total_len + len(snippet)
        if projected > MAX_CONTEXT_CHARS:
            remain = MAX_CONTEXT_CHARS - total_len
            if remain <= 0:
                break
            snippet = snippet[:remain]
            projected = total_len + len(snippet)

        blocks.append(f"## Resource: {item.get('original_name', rid)}\n\n{snippet}")
        used_ids.append(rid)
        total_len = projected
        if total_len >= MAX_CONTEXT_CHARS:
            break

    return ("\n\n---\n\n".join(blocks)).strip(), used_ids, skipped


def _build_strategy_context(strategy_id: str = "") -> Tuple[str, str]:
    payload = _load_strategies_payload()
    strategies = payload.get("strategies", [])
    active_strategy_id = (payload.get("active_strategy_id") or "").strip()
    strategy_map = {item.get("id"): item for item in strategies if item.get("id")}

    requested_id = (strategy_id or "").strip()
    resolved_id = requested_id or active_strategy_id
    target = strategy_map.get(resolved_id) if resolved_id else None

    if not target and active_strategy_id:
        target = strategy_map.get(active_strategy_id)
        resolved_id = active_strategy_id if target else resolved_id

    if not target and strategies:
        target = strategies[0]
        resolved_id = (target.get("id") or "").strip()

    lines: List[str] = [
        "策略与看盘上下文：",
        f"- active_strategy_id: {active_strategy_id or '(none)'}",
        f"- strategy_count: {len(strategies)}",
    ]

    if not target:
        lines.append("- 当前没有可用策略配置。")
        return "\n".join(lines), ""

    config = target.get("config") if isinstance(target.get("config"), dict) else {}
    widgets = config.get("widgets") if isinstance(config, dict) else None
    widget_count = len(widgets) if isinstance(widgets, list) else 0
    config_json = json.dumps(config, ensure_ascii=False, separators=(",", ":"))

    lines.extend(
        [
            "- 当前策略：",
            f"  - id: {target.get('id') or ''}",
            f"  - name: {target.get('name') or ''}",
            f"  - description: {target.get('description') or ''}",
            f"  - view_type: {target.get('view_type') or DEFAULT_STRATEGY_VIEW}",
            f"  - widget_count: {widget_count}",
            f"  - config_json: {config_json}",
        ]
    )

    return "\n".join(lines), resolved_id


def _is_relpath_referenced(resources: List[Dict], rel_path: str, exclude_resource_id: str = "") -> bool:
    target = (rel_path or "").strip()
    if not target:
        return False
    for item in resources:
        if exclude_resource_id and item.get("id") == exclude_resource_id:
            continue
        if (item.get("source_relpath") or "").strip() == target:
            return True
        if (item.get("markdown_relpath") or "").strip() == target:
            return True
    return False


def _register_crawled_resources(crawl_results: List[Dict]) -> List[str]:
    created_ids: List[str] = []
    payload = _load_resources_payload()
    resources = payload.get("resources", [])
    crawl_group_id = "group_crawled"
    crawl_group_name = "爬虫抓取"

    for item in crawl_results:
        if item.get("status") != "ok" or not item.get("markdown_path"):
            continue

        md_path = Path(item["markdown_path"]).resolve()
        if not md_path.exists():
            continue

        rid = _gen_id("res")
        rel_markdown = md_path.relative_to(BASE_DIR).as_posix()
        published_at = _normalize_datetime_to_iso(item.get("published_at") or "")
        content_time = _normalize_date_only(item.get("published_at") or "")
        record = {
            "id": rid,
            "original_name": item.get("title") or item.get("url"),
            "stored_name": md_path.name,
            "extension": ".wx",
            "size_bytes": md_path.stat().st_size,
            "uploaded_at": _now_iso(),
            "status": "ok",
            "error": "",
            "source_type": "url",
            "source_url": item.get("url", ""),
            "source_relpath": "",
            "markdown_relpath": rel_markdown,
            "group_id": crawl_group_id,
            "group_name": crawl_group_name,
            "published_at": published_at,
            "content_time": content_time,
            "content_time_type": "published" if content_time else "unknown",
            "content_time_confidence": 0.95 if content_time else 0.0,
            "content_time_evidence": ("wechat:published_at" if content_time else ""),
        }
        resources.append(record)
        created_ids.append(rid)

    if created_ids:
        payload["resources"] = resources
        _save_resources_payload(payload)
    return created_ids


def _attach_crawl_relpaths(crawl_results: List[Dict]) -> List[Dict]:
    normalized: List[Dict] = []
    key_map = {
        "markdown_path": "markdown_relpath",
        "pdf_path": "pdf_relpath",
        "docx_path": "docx_relpath",
    }
    for item in crawl_results or []:
        if not isinstance(item, dict):
            continue
        cloned = dict(item)
        for key, rel_key in key_map.items():
            raw_path = cloned.get(key)
            if not raw_path:
                continue
            try:
                rel = Path(raw_path).resolve().relative_to(BASE_DIR).as_posix()
            except Exception:
                continue
            cloned[rel_key] = rel
        normalized.append(cloned)
    return normalized


def _build_crawler_agent_summary(crawl_results: List[Dict]) -> str:
    total = len(crawl_results or [])
    ok_count = sum(1 for item in (crawl_results or []) if (item or {}).get("status") == "ok")
    fail_count = max(total - ok_count, 0)
    lines = [f"抓取任务完成：共 {total} 条，成功 {ok_count} 条，失败 {fail_count} 条。"]
    if not total:
        lines.append("未识别到可抓取链接，请检查输入内容是否包含公众号文章 URL。")
        return "\n".join(lines)
    for idx, item in enumerate(crawl_results, start=1):
        title = (item or {}).get("title") or (item or {}).get("url") or f"抓取文章{idx}"
        status = "成功" if (item or {}).get("status") == "ok" else "失败"
        lines.append(f"{idx}. {title}（{status}）")
    return "\n".join(lines)


def _summarize_resource_groups(resources: List[Dict]) -> List[Dict]:
    groups: Dict[str, Dict] = {}
    for item in resources:
        group_id = item.get("group_id") or f"legacy_{DEFAULT_GROUP_NAME}"
        group_name = item.get("group_name") or DEFAULT_GROUP_NAME
        group = groups.get(group_id)
        if not group:
            group = {
                "group_id": group_id,
                "group_name": group_name,
                "count": 0,
                "updated_at": item.get("uploaded_at") or "",
            }
            groups[group_id] = group
        group["count"] += 1
        uploaded_at = item.get("uploaded_at") or ""
        if uploaded_at and uploaded_at > (group.get("updated_at") or ""):
            group["updated_at"] = uploaded_at
    return sorted(groups.values(), key=lambda x: x.get("updated_at", ""), reverse=True)


def _find_group_by_name(resources: List[Dict], group_name: str) -> Tuple[str, str]:
    target = (group_name or "").strip()
    if not target:
        return "", ""
    for item in resources:
        if (item.get("group_name") or "").strip() == target:
            return (item.get("group_id") or "").strip(), target
    return "", ""


def _resolve_group_id_by_name(resources: List[Dict], group_name: str) -> str:
    target = (group_name or "").strip()
    if not target:
        return ""
    existing_id, _ = _find_group_by_name(resources, target)
    if existing_id:
        return existing_id
    return _gen_id("group")


def _copy_resource_record(item: Dict, target_group_id: str, target_name: str) -> Tuple[Dict, str]:
    new_id = _gen_id("res")
    ext = (item.get("extension") or "").strip() or Path(item.get("original_name") or "").suffix
    original_name = item.get("original_name") or new_id
    safe_stem = secure_filename(Path(original_name).stem) or "file"
    stored_name = f"{new_id}_{safe_stem}{ext}"
    upload_rel = item.get("source_relpath") or ""
    markdown_rel = item.get("markdown_relpath") or ""

    new_upload_rel = ""
    new_markdown_rel = ""

    if upload_rel:
        src_upload = (BASE_DIR / upload_rel).resolve()
        if src_upload.exists() and src_upload.is_file():
            dst_upload = (UPLOAD_DIR / stored_name).resolve()
            dst_upload.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src_upload, dst_upload)
            new_upload_rel = dst_upload.relative_to(BASE_DIR).as_posix()

    if markdown_rel:
        src_md = (BASE_DIR / markdown_rel).resolve()
        if src_md.exists() and src_md.is_file():
            dst_md = (MARKDOWN_DIR / _markdown_filename(new_id, original_name)).resolve()
            dst_md.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src_md, dst_md)
            new_markdown_rel = dst_md.relative_to(BASE_DIR).as_posix()

    if not new_markdown_rel and markdown_rel:
        return {}, "markdown 文件不存在"

    record = dict(item)
    record.update(
        {
            "id": new_id,
            "stored_name": stored_name,
            "uploaded_at": _now_iso(),
            "group_id": target_group_id,
            "group_name": target_name,
            "source_relpath": new_upload_rel,
            "markdown_relpath": new_markdown_rel,
            "status": item.get("status") or "ok",
            "error": "",
            "progress": 100,
            "progress_message": "完成",
        }
    )
    if new_upload_rel:
        try:
            record["size_bytes"] = (BASE_DIR / new_upload_rel).stat().st_size
        except Exception:
            record["size_bytes"] = item.get("size_bytes", 0)
    return record, ""


@strategy_watch_bp.route("/api/strategy-watch/runtime", methods=["GET"])
def strategy_watch_runtime():
    _load_env_files()
    return jsonify(
        {
            "success": True,
            "data": {
                "base_url": (os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1"),
                "model": (os.getenv("OPENAI_MODEL") or "gpt-4o-mini"),
                "api_key_configured": bool(
                    (os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY") or "").strip()
                ),
                "agents": list_agent_profiles(),
            },
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/agents", methods=["GET"])
def strategy_watch_agents():
    return jsonify({"success": True, "data": list_agent_profiles(), "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/resources", methods=["GET"])
def list_strategy_resources():
    with _STORE_LOCK:
        payload = _load_resources_payload()
        resources = payload.get("resources", [])
        active_resource_id = payload.get("active_resource_id") or ""
    resources = sorted(resources, key=lambda x: x.get("uploaded_at", ""), reverse=True)
    return jsonify(
        {
            "success": True,
            "data": {
                "resources": resources,
                "groups": _summarize_resource_groups(resources),
                "active_resource_id": active_resource_id,
            },
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/resources", methods=["POST"])
def upload_strategy_resources():
    if "files" not in request.files:
        return jsonify({"success": False, "error": "请通过 files 字段上传文件。", "timestamp": _now_iso()}), 400

    files = request.files.getlist("files")
    whisper_model = (request.form.get("whisper_model") or "tiny").strip()
    group_name = (request.form.get("group_name") or "").strip() or DEFAULT_GROUP_NAME
    strategy_id = (request.form.get("strategy_id") or "").strip()
    strategy_name = ""
    if strategy_id:
        strategies_payload = _load_strategies_payload()
        strategy = _find_by_id(strategies_payload.get("strategies", []), strategy_id)
        if not strategy:
            return jsonify({"success": False, "error": "策略不存在。", "timestamp": _now_iso()}), 404
        strategy_name = (strategy.get("name") or "").strip()

    uploaded = []
    jobs = []
    rejected = []
    to_process = []
    with _STORE_LOCK:
        payload = _load_resources_payload()
        resources = payload.get("resources", [])

        existing_group_id = _resolve_group_id_by_name(resources, group_name)
        group_id = existing_group_id or _gen_id("group")

        for file_storage in files:
            original_name = (file_storage.filename or "").strip()
            if not original_name:
                continue

            ext = Path(original_name).suffix.lower()
            if ext not in SUPPORTED_EXTENSIONS:
                rejected.append({"filename": original_name, "reason": f"不支持的文件类型: {ext}"})
                continue

            rid = _gen_id("res")
            safe_stem = secure_filename(Path(original_name).stem) or "file"
            stored_name = f"{rid}_{safe_stem}{ext}"
            upload_path = UPLOAD_DIR / stored_name
            markdown_path = MARKDOWN_DIR / _markdown_filename(rid, original_name)

            file_storage.save(upload_path)

            rel_upload = upload_path.relative_to(BASE_DIR).as_posix()
            rel_markdown = markdown_path.relative_to(BASE_DIR).as_posix()
            inferred_content_time, inferred_hit = _extract_first_date(original_name)
            record = {
                "id": rid,
                "original_name": original_name,
                "stored_name": stored_name,
                "extension": ext,
                "size_bytes": upload_path.stat().st_size if upload_path.exists() else 0,
                "uploaded_at": _now_iso(),
                "status": "processing",
                "error": "",
                "source_type": "file",
                "source_relpath": rel_upload,
                "markdown_relpath": rel_markdown,
                "group_id": group_id,
                "group_name": group_name,
                "strategy_id": strategy_id,
                "strategy_name": strategy_name,
                "progress": 0,
                "progress_message": "排队中",
                "published_at": "",
                "content_time": inferred_content_time,
                "content_time_type": "inferred_filename" if inferred_content_time else "unknown",
                "content_time_confidence": 0.70 if inferred_content_time else 0.0,
                "content_time_evidence": (f"filename:{inferred_hit}" if inferred_content_time else ""),
            }
            resources.append(record)
            uploaded.append(record)
            to_process.append(
                {
                    "resource_id": rid,
                    "upload_path": upload_path,
                    "markdown_path": markdown_path,
                    "whisper_model": whisper_model,
                    "original_name": original_name,
                }
            )

        payload["resources"] = resources
        _save_resources_payload(payload)

    for item in to_process:
        job_id = _gen_id("job")
        job_payload = {
            "resource_id": item["resource_id"],
            "original_name": item["original_name"],
            "status": "queued",
            "progress": 0,
            "message": "排队中",
            "created_at": _now_iso(),
        }
        _set_job(job_id, **job_payload)
        job_payload["job_id"] = job_id
        jobs.append(job_payload)
        worker = threading.Thread(
            target=_process_resource_job,
            args=(
                job_id,
                item["resource_id"],
                item["upload_path"],
                item["markdown_path"],
                item["whisper_model"],
            ),
            daemon=True,
        )
        worker.start()

    return jsonify(
        {
            "success": True,
            "data": {
                "uploaded": uploaded,
                "rejected": rejected,
                "group_id": group_id,
                "group_name": group_name,
                "jobs": jobs,
                "async": True,
            },
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/resources/jobs/<string:job_id>", methods=["GET"])
def get_resource_job(job_id: str):
    job = _get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "任务不存在。", "timestamp": _now_iso()}), 404
    return jsonify({"success": True, "data": job, "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/resources/jobs", methods=["GET"])
def list_resource_jobs():
    return jsonify({"success": True, "data": _list_jobs(), "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/resources/<string:resource_id>", methods=["DELETE"])
def delete_strategy_resource(resource_id: str):
    remaining_resources: List[Dict] = []
    with _STORE_LOCK:
        payload = _load_resources_payload()
        resources = payload.get("resources", [])
        target = _find_by_id(resources, resource_id)
        if not target:
            return jsonify({"success": False, "error": "资源不存在。", "timestamp": _now_iso()}), 404

        resources = [item for item in resources if item.get("id") != resource_id]
        remaining_resources = resources
        payload["resources"] = resources
        if payload.get("active_resource_id") == resource_id:
            payload["active_resource_id"] = ""
        _save_resources_payload(payload)

    for rel_key in ("source_relpath", "markdown_relpath"):
        rel_path = target.get(rel_key) or ""
        if not rel_path or "://" in rel_path:
            continue
        if _is_relpath_referenced(remaining_resources, rel_path):
            continue
        file_path = BASE_DIR / rel_path
        try:
            if file_path.exists() and file_path.is_file():
                file_path.unlink()
        except Exception:
            pass

    return jsonify({"success": True, "message": "资源已删除。", "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/resources/<string:resource_id>", methods=["PATCH"])
def rename_strategy_resource(resource_id: str):
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or payload.get("original_name") or "").strip()
    if not name:
        return jsonify({"success": False, "error": "资源名称不能为空。", "timestamp": _now_iso()}), 400

    with _STORE_LOCK:
        resources_payload = _load_resources_payload()
        resources = resources_payload.get("resources", [])
        target = _find_by_id(resources, resource_id)
        if not target:
            return jsonify({"success": False, "error": "资源不存在。", "timestamp": _now_iso()}), 404

        current_ext = (target.get("extension") or "").strip()
        incoming_ext = Path(name).suffix
        if incoming_ext and incoming_ext != current_ext:
            name = f"{Path(name).stem}{current_ext}" if current_ext else name

        target["original_name"] = name
        resources_payload["resources"] = resources
        _save_resources_payload(resources_payload)

    return jsonify({"success": True, "data": target, "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/resources/<string:resource_id>/markdown", methods=["GET"])
def get_resource_markdown(resource_id: str):
    with _STORE_LOCK:
        payload = _load_resources_payload()
        resources = payload.get("resources", [])
    target = _find_by_id(resources, resource_id)
    if not target:
        return jsonify({"success": False, "error": "资源不存在。", "timestamp": _now_iso()}), 404

    content = _read_markdown_content(target)
    return jsonify(
        {
            "success": True,
            "data": {
                "resource_id": resource_id,
                "original_name": target.get("original_name"),
                "content": content,
            },
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/crawled/download", methods=["GET"])
def download_crawled_file():
    relpath = (request.args.get("path") or "").strip()
    if not relpath:
        return jsonify({"success": False, "error": "缺少 path 参数。", "timestamp": _now_iso()}), 400
    if "://" in relpath or Path(relpath).is_absolute():
        return jsonify({"success": False, "error": "非法路径。", "timestamp": _now_iso()}), 400

    file_path = (BASE_DIR / relpath).resolve()
    try:
        file_path.relative_to(CRAWLED_DIR)
    except Exception:
        return jsonify({"success": False, "error": "非法路径。", "timestamp": _now_iso()}), 403
    if not file_path.exists() or not file_path.is_file():
        return jsonify({"success": False, "error": "文件不存在。", "timestamp": _now_iso()}), 404

    return send_file(file_path, as_attachment=True, download_name=file_path.name)


@strategy_watch_bp.route("/api/strategy-watch/resource-groups/<string:group_id>", methods=["PATCH"])
def rename_strategy_resource_group(group_id: str):
    payload = request.get_json(silent=True) or {}
    group_name = (payload.get("group_name") or payload.get("name") or "").strip()
    if not group_name:
        return jsonify({"success": False, "error": "组名不能为空", "timestamp": _now_iso()}), 400

    with _STORE_LOCK:
        resources_payload = _load_resources_payload()
        resources = resources_payload.get("resources", [])
        existing_id, _ = _find_group_by_name(resources, group_name)
        if existing_id and existing_id != group_id:
            return jsonify({"success": False, "error": "组名已存在", "timestamp": _now_iso()}), 400
        updated = 0
        for item in resources:
            if item.get("group_id") == group_id:
                item["group_name"] = group_name
                updated += 1
        if updated == 0:
            return jsonify({"success": False, "error": "资源组不存在", "timestamp": _now_iso()}), 404
        resources_payload["resources"] = resources
        _save_resources_payload(resources_payload)

    return jsonify(
        {
            "success": True,
            "data": {"group_id": group_id, "group_name": group_name, "updated_count": updated},
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/resource-groups/transfer", methods=["POST"])
def transfer_strategy_resource_group():
    payload = request.get_json(silent=True) or {}
    source_group_id = (payload.get("source_group_id") or "").strip()
    target_group_id = (payload.get("target_group_id") or "").strip()
    target_group_name = (payload.get("target_group_name") or "").strip()
    mode = (payload.get("mode") or "move").strip().lower()
    if mode not in {"move", "copy"}:
        return jsonify({"success": False, "error": "mode 必须为 move 或 copy", "timestamp": _now_iso()}), 400
    if not source_group_id:
        return jsonify({"success": False, "error": "缺少 source_group_id", "timestamp": _now_iso()}), 400
    if not target_group_id and not target_group_name:
        return jsonify({"success": False, "error": "缺少目标分组", "timestamp": _now_iso()}), 400

    with _STORE_LOCK:
        resources_payload = _load_resources_payload()
        resources = resources_payload.get("resources", [])
        source_items = [item for item in resources if item.get("group_id") == source_group_id]
        if not source_items:
            return jsonify({"success": False, "error": "源分组不存在", "timestamp": _now_iso()}), 404

        if target_group_id:
            target_name = next(
                (item.get("group_name") for item in resources if item.get("group_id") == target_group_id), ""
            ).strip()
            if not target_name:
                return jsonify({"success": False, "error": "目标分组不存在", "timestamp": _now_iso()}), 404
        else:
            existing_id, _ = _find_group_by_name(resources, target_group_name)
            if existing_id:
                target_group_id = existing_id
                target_name = target_group_name
            else:
                target_group_id = _gen_id("group")
                target_name = target_group_name

        if target_group_id == source_group_id:
            return jsonify({"success": False, "error": "源分组与目标分组相同", "timestamp": _now_iso()}), 400

        moved = 0
        copied = 0
        skipped = []

        if mode == "move":
            for item in resources:
                if item.get("group_id") == source_group_id:
                    item["group_id"] = target_group_id
                    item["group_name"] = target_name
                    moved += 1
        else:
            new_records = []
            for item in source_items:
                new_id = _gen_id("res")
                ext = (item.get("extension") or "").strip() or Path(item.get("original_name") or "").suffix
                original_name = item.get("original_name") or new_id
                safe_stem = secure_filename(Path(original_name).stem) or "file"
                stored_name = f"{new_id}_{safe_stem}{ext}"
                upload_rel = item.get("source_relpath") or ""
                markdown_rel = item.get("markdown_relpath") or ""

                new_upload_rel = ""
                new_markdown_rel = ""

                # Copy upload file if exists
                if upload_rel:
                    src_upload = (BASE_DIR / upload_rel).resolve()
                    if src_upload.exists() and src_upload.is_file():
                        dst_upload = (UPLOAD_DIR / stored_name).resolve()
                        dst_upload.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(src_upload, dst_upload)
                        new_upload_rel = dst_upload.relative_to(BASE_DIR).as_posix()

                # Copy markdown file if exists
                if markdown_rel:
                    src_md = (BASE_DIR / markdown_rel).resolve()
                    if src_md.exists() and src_md.is_file():
                        dst_md = (MARKDOWN_DIR / _markdown_filename(new_id, original_name)).resolve()
                        dst_md.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(src_md, dst_md)
                        new_markdown_rel = dst_md.relative_to(BASE_DIR).as_posix()

                if not new_markdown_rel and markdown_rel:
                    skipped.append({"resource_id": item.get("id"), "reason": "markdown 文件不存在"})
                    continue

                record = dict(item)
                record.update(
                    {
                        "id": new_id,
                        "stored_name": stored_name,
                        "uploaded_at": _now_iso(),
                        "group_id": target_group_id,
                        "group_name": target_name,
                        "source_relpath": new_upload_rel,
                        "markdown_relpath": new_markdown_rel,
                        "status": item.get("status") or "ok",
                        "error": "",
                        "progress": 100,
                        "progress_message": "完成",
                    }
                )
                if new_upload_rel:
                    try:
                        record["size_bytes"] = (BASE_DIR / new_upload_rel).stat().st_size
                    except Exception:
                        record["size_bytes"] = item.get("size_bytes", 0)
                new_records.append(record)
                copied += 1

            resources.extend(new_records)

        resources_payload["resources"] = resources
        _save_resources_payload(resources_payload)

    return jsonify(
        {
            "success": True,
            "data": {
                "mode": mode,
                "source_group_id": source_group_id,
                "target_group_id": target_group_id,
                "target_group_name": target_name,
                "moved": moved,
                "copied": copied,
                "skipped": skipped,
            },
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/resources/transfer", methods=["POST"])
def transfer_strategy_resources():
    payload = request.get_json(silent=True) or {}
    resource_ids = payload.get("resource_ids") or []
    if not isinstance(resource_ids, list):
        resource_ids = []
    mode = (payload.get("mode") or "move").strip().lower()
    if mode not in {"move", "copy"}:
        return jsonify({"success": False, "error": "mode 必须为 move 或 copy", "timestamp": _now_iso()}), 400
    if not resource_ids:
        return jsonify({"success": False, "error": "resource_ids 不能为空", "timestamp": _now_iso()}), 400

    target_group_id = (payload.get("target_group_id") or "").strip()
    target_group_name = (payload.get("target_group_name") or "").strip()
    if not target_group_id and not target_group_name:
        return jsonify({"success": False, "error": "缺少目标分组", "timestamp": _now_iso()}), 400

    with _STORE_LOCK:
        resources_payload = _load_resources_payload()
        resources = resources_payload.get("resources", [])
        resource_map = {item.get("id"): item for item in resources}
        targets = [resource_map.get(rid) for rid in resource_ids if resource_map.get(rid)]
        if not targets:
            return jsonify({"success": False, "error": "未找到资源", "timestamp": _now_iso()}), 404

        if target_group_id:
            target_name = next(
                (item.get("group_name") for item in resources if item.get("group_id") == target_group_id),
                "",
            )
            if not target_name:
                target_name = target_group_name or DEFAULT_GROUP_NAME
        else:
            existing_id, _ = _find_group_by_name(resources, target_group_name)
            if existing_id:
                target_group_id = existing_id
                target_name = target_group_name
            else:
                target_group_id = _gen_id("group")
                target_name = target_group_name

        moved = 0
        copied = 0
        skipped: List[Dict] = []

        if mode == "move":
            for item in targets:
                item["group_id"] = target_group_id
                item["group_name"] = target_name
                moved += 1
        else:
            new_records: List[Dict] = []
            for item in targets:
                record, reason = _copy_resource_record(item, target_group_id, target_name)
                if not record:
                    skipped.append({"resource_id": item.get("id"), "reason": reason or "复制失败"})
                    continue
                new_records.append(record)
                copied += 1
            resources.extend(new_records)

        resources_payload["resources"] = resources
        _save_resources_payload(resources_payload)

    return jsonify(
        {
            "success": True,
            "data": {
                "mode": mode,
                "target_group_id": target_group_id,
                "target_group_name": target_name,
                "moved": moved,
                "copied": copied,
                "skipped": skipped,
            },
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/resources/active", methods=["PATCH"])
def set_active_strategy_resource():
    payload = request.get_json(silent=True) or {}
    resource_id = (payload.get("resource_id") or "").strip()

    with _STORE_LOCK:
        resources_payload = _load_resources_payload()
        resources = resources_payload.get("resources", [])
        if resource_id and not _find_by_id(resources, resource_id):
            return jsonify({"success": False, "error": "资源不存在。", "timestamp": _now_iso()}), 404
        resources_payload["active_resource_id"] = resource_id
        _save_resources_payload(resources_payload)

    return jsonify(
        {"success": True, "data": {"active_resource_id": resource_id}, "timestamp": _now_iso()}
    )


@strategy_watch_bp.route("/api/strategy-watch/memory-profiles", methods=["GET"])
def list_memory_profiles():
    with _STORE_LOCK:
        payload = _load_memory_profiles_payload()
        profiles = payload.get("profiles", [])
        active_profile_id = payload.get("active_profile_id") or ""
        links = _load_memory_links()
        portraits = _load_memory_portraits()
    data = [_memory_profile_view(item, links, portraits) for item in profiles]
    data.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    return jsonify(
        {
            "success": True,
            "data": {
                "profiles": data,
                "active_profile_id": active_profile_id,
            },
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/memory-profiles", methods=["POST"])
def create_memory_profile():
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip() or f"人格 {datetime.now().strftime('%m-%d %H:%M')}"
    description = (payload.get("description") or "").strip()
    source_blogger = (payload.get("source_blogger") or "").strip()
    profile = {
        "id": _gen_id("mp"),
        "name": name,
        "description": description,
        "source_blogger": source_blogger,
        "group_subscriptions": [],
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
    }
    with _STORE_LOCK:
        profiles_payload = _load_memory_profiles_payload()
        profiles = profiles_payload.get("profiles", [])
        profiles.append(profile)
        profiles_payload["profiles"] = profiles
        if not profiles_payload.get("active_profile_id"):
            profiles_payload["active_profile_id"] = profile["id"]
        _save_memory_profiles_payload(profiles_payload)
        portraits = _load_memory_portraits()
        portraits[profile["id"]] = _default_memory_portrait(profile["id"])
        _save_memory_portraits(portraits)
    return jsonify({"success": True, "data": profile, "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/memory-profiles/active", methods=["PATCH"])
def set_active_memory_profile():
    payload = request.get_json(silent=True) or {}
    profile_id = (payload.get("profile_id") or "").strip()
    with _STORE_LOCK:
        profiles_payload = _load_memory_profiles_payload()
        profiles = profiles_payload.get("profiles", [])
        if profile_id and not _find_by_id(profiles, profile_id):
            return jsonify({"success": False, "error": "人格不存在。", "timestamp": _now_iso()}), 404
        profiles_payload["active_profile_id"] = profile_id
        _save_memory_profiles_payload(profiles_payload)
    return jsonify(
        {"success": True, "data": {"active_profile_id": profile_id}, "timestamp": _now_iso()}
    )


@strategy_watch_bp.route("/api/strategy-watch/memory-profiles/<string:profile_id>", methods=["PATCH"])
def update_memory_profile(profile_id: str):
    payload = request.get_json(silent=True) or {}
    with _STORE_LOCK:
        profiles_payload = _load_memory_profiles_payload()
        profiles = profiles_payload.get("profiles", [])
        target = _find_by_id(profiles, profile_id)
        if not target:
            return jsonify({"success": False, "error": "人格不存在。", "timestamp": _now_iso()}), 404
        if "name" in payload:
            target["name"] = (payload.get("name") or "").strip() or target.get("name")
        if "description" in payload:
            target["description"] = (payload.get("description") or "").strip()
        if "source_blogger" in payload:
            target["source_blogger"] = (payload.get("source_blogger") or "").strip()
        target["updated_at"] = _now_iso()
        profiles_payload["profiles"] = profiles
        _save_memory_profiles_payload(profiles_payload)
    return jsonify({"success": True, "data": target, "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/memory-profiles/<string:profile_id>", methods=["DELETE"])
def delete_memory_profile(profile_id: str):
    with _STORE_LOCK:
        profiles_payload = _load_memory_profiles_payload()
        profiles = profiles_payload.get("profiles", [])
        target = _find_by_id(profiles, profile_id)
        if not target:
            return jsonify({"success": False, "error": "人格不存在。", "timestamp": _now_iso()}), 404
        profiles = [item for item in profiles if item.get("id") != profile_id]
        profiles_payload["profiles"] = profiles
        if profiles_payload.get("active_profile_id") == profile_id:
            profiles_payload["active_profile_id"] = ""
        _save_memory_profiles_payload(profiles_payload)

        links = [item for item in _load_memory_links() if item.get("profile_id") != profile_id]
        _save_memory_links(links)
        portraits = _load_memory_portraits()
        if profile_id in portraits:
            portraits.pop(profile_id, None)
            _save_memory_portraits(portraits)

    return jsonify({"success": True, "message": "人格已删除。", "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/memory-profiles/<string:profile_id>/bind-resources", methods=["POST"])
def bind_memory_profile_resources(profile_id: str):
    payload = request.get_json(silent=True) or {}
    resource_ids = payload.get("resource_ids") or []
    if not isinstance(resource_ids, list):
        resource_ids = []
    if not resource_ids:
        return jsonify({"success": False, "error": "resource_ids 不能为空。", "timestamp": _now_iso()}), 400

    with _STORE_LOCK:
        profiles_payload = _load_memory_profiles_payload()
        profile = _find_by_id(profiles_payload.get("profiles", []), profile_id)
        if not profile:
            return jsonify({"success": False, "error": "人格不存在。", "timestamp": _now_iso()}), 404
        result = _bind_resources_to_profile(profile_id, resource_ids, bind_source="manual")
        profile["updated_at"] = _now_iso()
        _save_memory_profiles_payload(profiles_payload)

    return jsonify({"success": True, "data": result, "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/memory-profiles/<string:profile_id>/bind-group", methods=["POST"])
def bind_memory_profile_group(profile_id: str):
    payload = request.get_json(silent=True) or {}
    group_id = (payload.get("group_id") or "").strip()
    if not group_id:
        return jsonify({"success": False, "error": "缺少 group_id。", "timestamp": _now_iso()}), 400

    with _STORE_LOCK:
        profiles_payload = _load_memory_profiles_payload()
        profile = _find_by_id(profiles_payload.get("profiles", []), profile_id)
        if not profile:
            return jsonify({"success": False, "error": "人格不存在。", "timestamp": _now_iso()}), 404

        result = _sync_profile_group_resources(profile, group_id)
        profile["updated_at"] = _now_iso()
        _save_memory_profiles_payload(profiles_payload)

    return jsonify(
        {
            "success": True,
            "data": {
                "group_id": group_id,
                "added": result.get("added", 0),
                "skipped": result.get("skipped", []),
            },
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/memory-profiles/<string:profile_id>/sync-group", methods=["POST"])
def sync_memory_profile_group(profile_id: str):
    payload = request.get_json(silent=True) or {}
    group_id = (payload.get("group_id") or "").strip()
    with _STORE_LOCK:
        profiles_payload = _load_memory_profiles_payload()
        profile = _find_by_id(profiles_payload.get("profiles", []), profile_id)
        if not profile:
            return jsonify({"success": False, "error": "人格不存在。", "timestamp": _now_iso()}), 404

        subs = profile.get("group_subscriptions") if isinstance(profile.get("group_subscriptions"), list) else []
        target_group_ids = [group_id] if group_id else [
            (item.get("group_id") or "").strip()
            for item in subs
            if isinstance(item, dict) and (item.get("group_id") or "").strip()
        ]
        if not target_group_ids:
            return jsonify({"success": False, "error": "没有可同步的分组。", "timestamp": _now_iso()}), 400

        total_added = 0
        all_skipped: List[Dict] = []
        sync_details: List[Dict] = []
        for gid in target_group_ids:
            result = _sync_profile_group_resources(profile, gid)
            added = int(result.get("added") or 0)
            skipped = result.get("skipped") or []
            total_added += added
            all_skipped.extend(skipped)
            sync_details.append({"group_id": gid, "added": added, "skipped_count": len(skipped)})

        profile["updated_at"] = _now_iso()
        _save_memory_profiles_payload(profiles_payload)

    return jsonify(
        {
            "success": True,
            "data": {
                "synced_groups": sync_details,
                "added": total_added,
                "skipped": all_skipped,
            },
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/memory-profiles/<string:profile_id>/extract-portrait-draft", methods=["POST"])
def extract_memory_portrait_draft(profile_id: str):
    with _STORE_LOCK:
        profiles_payload = _load_memory_profiles_payload()
        profile = _find_by_id(profiles_payload.get("profiles", []), profile_id)
        if not profile:
            return jsonify({"success": False, "error": "人格不存在。", "timestamp": _now_iso()}), 404
        portrait_map = _load_memory_portraits()
        portrait = portrait_map.get(profile_id) or _default_memory_portrait(profile_id)

    snippets, used_ids, skipped = _collect_profile_resource_snippets(
        profile_id,
        max_chars=32_000,
        max_items=12,
        per_item_chars=2_400,
    )
    if not snippets:
        return jsonify(
            {
                "success": False,
                "error": "当前人格还没有可用于侧写的资料，请先绑定资料。",
                "timestamp": _now_iso(),
            }
        ), 400

    prompt = (
        "请基于资料生成人物侧写初稿，并返回 JSON（不要输出其他文字）。\n"
        "JSON 字段:\n"
        "{\n"
        '  "methodology": "交易方法论",\n'
        '  "tactics": "交易手法",\n'
        '  "views": "观点",\n'
        '  "operations": "交易操作",\n'
        '  "risk_rules": "风控规则",\n'
        '  "style_constraints": "表达与行为约束",\n'
        '  "evidence_refs": [\n'
        "    {\n"
        '      "resource_id": "res_xxx",\n'
        '      "topic": "观点|操作|方法论|手法",\n'
        '      "quote": "关键证据原文",\n'
        '      "source_time": "YYYY-MM-DD",\n'
        '      "source_time_type": "published|recorded|inferred_filename|inferred_text",\n'
        '      "timecode": "00:00:00-00:00:10"\n'
        "    }\n"
        "  ]\n"
        "}\n"
        "要求：证据尽量可追溯，source_time 优先使用资料内容时间。"
    )

    extra_context = (
        f"人格名称: {(profile.get('name') or '').strip()}\n"
        f"人格说明: {(profile.get('description') or '').strip()}\n"
        f"来源博主: {(profile.get('source_blogger') or '').strip()}"
    )
    portrait_model = (
        (os.getenv("OPENAI_PORTRAIT_MODEL") or "").strip()
        or "deepseek-v3.2-thinking"
    )
    try:
        reply, _model, _provider, _usage = chat_with_agent(
            agent_name="data_processing_agent",
            user_content=prompt,
            history_messages=[],
            resource_context=snippets,
            extra_context=extra_context,
            model_name=portrait_model,
            temperature=0.2,
        )
    except Exception as exc:
        return jsonify({"success": False, "error": f"侧写初稿生成失败: {exc}", "timestamp": _now_iso()}), 500

    parsed = _normalize_portrait_draft_payload(_extract_json_block(reply), reply)
    if not parsed:
        parsed = {"methodology": reply}

    def _section_to_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, list):
            lines = []
            for item in value:
                txt = str(item or "").strip()
                if txt:
                    lines.append(f"- {txt}")
            return "\n".join(lines).strip()
        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False, indent=2)
        return str(value).strip()

    evidence_refs_raw = parsed.get("evidence_refs") if isinstance(parsed.get("evidence_refs"), list) else []
    evidence_refs: List[Dict] = []
    for item in evidence_refs_raw:
        normalized = _normalize_evidence_ref(item)
        if normalized:
            evidence_refs.append(normalized)

    portrait.update(
        {
            "methodology": _section_to_text(parsed.get("methodology")),
            "tactics": _section_to_text(parsed.get("tactics")),
            "views": _section_to_text(parsed.get("views")),
            "operations": _section_to_text(parsed.get("operations")),
            "risk_rules": _section_to_text(parsed.get("risk_rules")),
            "style_constraints": _section_to_text(parsed.get("style_constraints")),
            "evidence_refs": evidence_refs,
            "updated_at": _now_iso(),
            "updated_by": "ai_draft",
        }
    )

    with _STORE_LOCK:
        portrait_map = _load_memory_portraits()
        portrait_map[profile_id] = portrait
        _save_memory_portraits(portrait_map)
        profiles_payload = _load_memory_profiles_payload()
        target = _find_by_id(profiles_payload.get("profiles", []), profile_id)
        if target:
            target["updated_at"] = _now_iso()
            _save_memory_profiles_payload(profiles_payload)

    return jsonify(
        {
            "success": True,
            "data": {
                "portrait": portrait,
                "used_resource_ids": used_ids,
                "skipped_refs": skipped,
            },
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/memory-profiles/<string:profile_id>/portrait", methods=["GET"])
def get_memory_portrait(profile_id: str):
    with _STORE_LOCK:
        profiles_payload = _load_memory_profiles_payload()
        profile = _find_by_id(profiles_payload.get("profiles", []), profile_id)
        if not profile:
            return jsonify({"success": False, "error": "人格不存在。", "timestamp": _now_iso()}), 404
        portrait_map = _load_memory_portraits()
        portrait = portrait_map.get(profile_id) or _default_memory_portrait(profile_id)
    return jsonify({"success": True, "data": portrait, "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/memory-profiles/<string:profile_id>/portrait", methods=["PATCH"])
def update_memory_portrait(profile_id: str):
    payload = request.get_json(silent=True) or {}
    with _STORE_LOCK:
        profiles_payload = _load_memory_profiles_payload()
        profile = _find_by_id(profiles_payload.get("profiles", []), profile_id)
        if not profile:
            return jsonify({"success": False, "error": "人格不存在。", "timestamp": _now_iso()}), 404
        portrait_map = _load_memory_portraits()
        portrait = portrait_map.get(profile_id) or _default_memory_portrait(profile_id)

        for key in ("methodology", "tactics", "views", "operations", "risk_rules", "style_constraints"):
            if key in payload:
                portrait[key] = (payload.get(key) or "").strip()

        if "evidence_refs" in payload:
            refs_raw = payload.get("evidence_refs")
            refs = []
            if isinstance(refs_raw, list):
                for item in refs_raw:
                    normalized = _normalize_evidence_ref(item)
                    if normalized:
                        refs.append(normalized)
            portrait["evidence_refs"] = refs

        portrait["updated_at"] = _now_iso()
        portrait["updated_by"] = "manual"
        portrait_map[profile_id] = portrait
        _save_memory_portraits(portrait_map)

        profile["updated_at"] = _now_iso()
        _save_memory_profiles_payload(profiles_payload)

    return jsonify({"success": True, "data": portrait, "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/memory-profiles/<string:profile_id>/portrait/export", methods=["POST"])
def export_memory_portrait(profile_id: str):
    payload = request.get_json(silent=True) or {}
    export_format = (payload.get("format") or request.args.get("format") or "md").strip().lower()
    if export_format not in {"md", "docx", "pdf"}:
        return jsonify({"success": False, "error": "仅支持导出 md/docx/pdf。", "timestamp": _now_iso()}), 400

    with _STORE_LOCK:
        profiles_payload = _load_memory_profiles_payload()
        profile = _find_by_id(profiles_payload.get("profiles", []), profile_id)
        if not profile:
            return jsonify({"success": False, "error": "人格不存在。", "timestamp": _now_iso()}), 404
        portrait_map = _load_memory_portraits()
        stored_portrait = portrait_map.get(profile_id) or _default_memory_portrait(profile_id)

    portrait = _merge_portrait_override(stored_portrait, payload.get("portrait"))
    markdown_text = _build_memory_portrait_markdown(profile, portrait)

    profile_name = (profile.get("name") or profile_id).strip() or profile_id
    base_name = secure_filename(profile_name) or profile_id
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")

    try:
        if export_format == "md":
            content = markdown_text.encode("utf-8")
            mimetype = "text/markdown; charset=utf-8"
            ext = "md"
        elif export_format == "docx":
            content = _render_memory_portrait_docx(profile, portrait)
            mimetype = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            ext = "docx"
        else:
            content = _render_memory_portrait_pdf(profile, portrait)
            mimetype = "application/pdf"
            ext = "pdf"
    except Exception as exc:
        return jsonify({"success": False, "error": f"导出失败: {exc}", "timestamp": _now_iso()}), 500

    return send_file(
        BytesIO(content),
        as_attachment=True,
        download_name=f"{base_name}_portrait_{stamp}.{ext}",
        mimetype=mimetype,
    )


@strategy_watch_bp.route("/api/strategy-watch/memory-profiles/<string:profile_id>/preview-context", methods=["GET"])
def preview_memory_profile_context(profile_id: str):
    context_text, meta = _build_memory_profile_context(profile_id)
    if not context_text and not meta.get("memory_profile_id"):
        return jsonify({"success": False, "error": "人格不存在。", "timestamp": _now_iso()}), 404
    return jsonify(
        {
            "success": True,
            "data": {
                "memory_profile_id": meta.get("memory_profile_id") or profile_id,
                "memory_profile_name": meta.get("memory_profile_name") or "",
                "context": context_text,
                "used_memory_resource_ids": meta.get("used_memory_resource_ids") or [],
                "skipped_memory_refs": meta.get("skipped_memory_refs") or [],
            },
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/conversations", methods=["GET"])
def list_strategy_conversations():
    with _STORE_LOCK:
        conversations = _load_conversations()

    summaries = []
    for conv in conversations:
        messages = conv.get("messages", [])
        last_message = messages[-1] if messages else None
        summaries.append(
            {
                "id": conv.get("id"),
                "title": conv.get("title"),
                "created_at": conv.get("created_at"),
                "updated_at": conv.get("updated_at"),
                "message_count": len(messages),
                "last_message_preview": (last_message or {}).get("content", "")[:120] if last_message else "",
            }
        )

    summaries.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    return jsonify({"success": True, "data": summaries, "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/conversations", methods=["POST"])
def create_strategy_conversation():
    payload = request.get_json(silent=True) or {}
    title = (payload.get("title") or "").strip() or f"策略会话 {datetime.now().strftime('%m-%d %H:%M')}"

    conversation = {
        "id": _gen_id("conv"),
        "title": title,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "messages": [],
    }

    with _STORE_LOCK:
        conversations = _load_conversations()
        conversations.append(conversation)
        _save_conversations(conversations)

    return jsonify({"success": True, "data": conversation, "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/conversations/<string:conversation_id>", methods=["PATCH"])
def rename_strategy_conversation(conversation_id: str):
    payload = request.get_json(silent=True) or {}
    title = (payload.get("title") or "").strip()
    if not title:
        return jsonify({"success": False, "error": "会话标题不能为空。", "timestamp": _now_iso()}), 400

    with _STORE_LOCK:
        conversations = _load_conversations()
        conv = _find_by_id(conversations, conversation_id)
        if not conv:
            return jsonify({"success": False, "error": "会话不存在。", "timestamp": _now_iso()}), 404
        conv["title"] = title
        conv["updated_at"] = _now_iso()
        _save_conversations(conversations)

    return jsonify({"success": True, "data": conv, "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/conversations/<string:conversation_id>", methods=["DELETE"])
def delete_strategy_conversation(conversation_id: str):
    with _STORE_LOCK:
        conversations = _load_conversations()
        target = _find_by_id(conversations, conversation_id)
        if not target:
            return jsonify({"success": False, "error": "会话不存在。", "timestamp": _now_iso()}), 404

        conversations = [item for item in conversations if item.get("id") != conversation_id]
        _save_conversations(conversations)

    return jsonify({"success": True, "message": "会话已删除。", "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/conversations/<string:conversation_id>/messages", methods=["GET"])
def list_strategy_messages(conversation_id: str):
    with _STORE_LOCK:
        conversations = _load_conversations()
    conv = _find_by_id(conversations, conversation_id)
    if not conv:
        return jsonify({"success": False, "error": "会话不存在。", "timestamp": _now_iso()}), 404
    return jsonify(
        {
            "success": True,
            "data": {
                "id": conv.get("id"),
                "title": conv.get("title"),
                "messages": conv.get("messages", []),
            },
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/conversations/<string:conversation_id>/messages", methods=["POST"])
def send_strategy_message(conversation_id: str):
    payload = request.get_json(silent=True) or {}
    content = (payload.get("content") or "").strip()
    if not content:
        return jsonify({"success": False, "error": "消息内容不能为空。", "timestamp": _now_iso()}), 400

    resource_ids = payload.get("resource_ids") or []
    if not isinstance(resource_ids, list):
        resource_ids = []
    strategy_id = (payload.get("strategy_id") or "").strip()
    memory_profile_id = (payload.get("memory_profile_id") or "").strip()
    prompt_template_id = (payload.get("prompt_template_id") or "").strip()
    prompt_template = (payload.get("prompt_template") or "").strip()
    llm_user_content = _compose_user_content_with_prompt_template(content, prompt_template)

    agent_name = (payload.get("agent_name") or "planner_agent").strip()
    known_agents = {item["name"] for item in list_agent_profiles()}
    if agent_name not in known_agents:
        agent_name = "planner_agent"

    temperature = payload.get("temperature", 0.2)
    model_name = (payload.get("model") or "").strip()
    crawler_cookie = (
        (payload.get("crawler_cookie") or "")
        or (request.headers.get("X-Wechat-Cookie") or "")
    ).strip()

    with _STORE_LOCK:
        conversations = _load_conversations()
        conv = _find_by_id(conversations, conversation_id)
        if not conv:
            return jsonify({"success": False, "error": "会话不存在。", "timestamp": _now_iso()}), 404

        user_message = {
            "id": _gen_id("msg"),
            "role": "user",
            "content": content,
            "created_at": _now_iso(),
            "resource_ids": resource_ids,
            "agent_name": agent_name,
            "strategy_id": strategy_id,
            "memory_profile_id": memory_profile_id,
            "prompt_template_id": prompt_template_id,
            "prompt_template": prompt_template,
        }
        conv.setdefault("messages", []).append(user_message)
        conv["updated_at"] = _now_iso()
        _save_conversations(conversations)

    crawled_resource_ids: List[str] = []
    crawl_results: List[Dict] = []
    if agent_name == "crawler_agent":
        crawl_results = crawl_wechat_articles_from_text(
            content,
            output_dir=CRAWLED_DIR,
            cookie_override=crawler_cookie,
        )
        crawl_results = _attach_crawl_relpaths(crawl_results)
        ok_items = [x for x in crawl_results if x.get("status") == "ok"]
        if ok_items:
            with _STORE_LOCK:
                crawled_resource_ids = _register_crawled_resources(ok_items)
        assistant_message = {
            "id": _gen_id("msg"),
            "role": "assistant",
            "content": _build_crawler_agent_summary(crawl_results),
            "created_at": _now_iso(),
            "model": "crawler_direct",
            "provider": "crawler",
            "usage": {},
            "agent_name": agent_name,
            "strategy_id": strategy_id,
            "memory_profile_id": memory_profile_id,
            "used_resource_ids": [],
            "skipped_resource_refs": [],
            "used_memory_resource_ids": [],
            "skipped_memory_refs": [],
            "crawled_resource_ids": crawled_resource_ids,
            "crawl_results": crawl_results,
            "error": False,
            "error_message": "",
        }
        with _STORE_LOCK:
            conversations = _load_conversations()
            conv = _find_by_id(conversations, conversation_id)
            if conv:
                conv.setdefault("messages", []).append(assistant_message)
                conv["updated_at"] = _now_iso()
                _save_conversations(conversations)
        return jsonify(
            {
                "success": True,
                "data": {
                    "conversation_id": conversation_id,
                    "user_message": user_message,
                    "assistant_message": assistant_message,
                    "crawl_results": crawl_results,
                },
                "timestamp": _now_iso(),
            }
        )

    resource_context, used_resource_ids, skipped_resources = _build_resource_context(resource_ids)
    history_for_conversation = conv.get("messages", [])[-MAX_HISTORY_MESSAGES:]

    strategy_context, resolved_strategy_id = _build_strategy_context(strategy_id)
    extra_context = strategy_context
    memory_context, memory_meta = _build_memory_profile_context(memory_profile_id)
    resolved_memory_profile_id = (memory_meta.get("memory_profile_id") or "").strip()
    used_memory_resource_ids = memory_meta.get("used_memory_resource_ids") or []
    skipped_memory_refs = memory_meta.get("skipped_memory_refs") or []
    if memory_context:
        if extra_context:
            extra_context += "\n\n"
        extra_context += "长期记忆上下文：\n" + memory_context

    assistant_text = ""
    error_text = ""
    resolved_model = model_name or (os.getenv("OPENAI_MODEL") or "gpt-4o-mini")
    provider = ""
    usage = {}
    try:
        assistant_text, resolved_model, provider, usage = chat_with_agent(
            agent_name=agent_name,
            user_content=llm_user_content,
            history_messages=history_for_conversation,
            resource_context=resource_context,
            extra_context=extra_context,
            model_name=model_name,
            temperature=temperature,
        )
    except Exception as exc:
        error_text = str(exc)
        assistant_text = (
            f"LLM 调用失败：{error_text}\n"
            "请检查 OPENAI_API_KEY / OPENAI_BASE_URL / OPENAI_MODEL 配置。"
        )

    assistant_message = {
        "id": _gen_id("msg"),
        "role": "assistant",
        "content": assistant_text,
        "created_at": _now_iso(),
        "model": resolved_model,
        "provider": provider,
        "usage": usage or {},
        "agent_name": agent_name,
        "strategy_id": resolved_strategy_id,
        "memory_profile_id": resolved_memory_profile_id,
        "used_resource_ids": used_resource_ids,
        "skipped_resource_refs": skipped_resources,
        "used_memory_resource_ids": used_memory_resource_ids,
        "skipped_memory_refs": skipped_memory_refs,
        "crawled_resource_ids": crawled_resource_ids,
        "crawl_results": crawl_results,
        "error": bool(error_text),
        "error_message": error_text,
    }

    with _STORE_LOCK:
        conversations = _load_conversations()
        conv = _find_by_id(conversations, conversation_id)
        if conv:
            conv.setdefault("messages", []).append(assistant_message)
            conv["updated_at"] = _now_iso()
            _save_conversations(conversations)

    return jsonify(
        {
            "success": True,
            "data": {
                "conversation_id": conversation_id,
                "user_message": user_message,
                "assistant_message": assistant_message,
                "crawl_results": crawl_results,
            },
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/conversations/<string:conversation_id>/messages/stream", methods=["POST"])
def send_strategy_message_stream(conversation_id: str):
    payload = request.get_json(silent=True) or {}
    content = (payload.get("content") or "").strip()
    if not content:
        return jsonify({"success": False, "error": "消息内容不能为空。", "timestamp": _now_iso()}), 400

    resource_ids = payload.get("resource_ids") or []
    if not isinstance(resource_ids, list):
        resource_ids = []
    strategy_id = (payload.get("strategy_id") or "").strip()
    memory_profile_id = (payload.get("memory_profile_id") or "").strip()
    prompt_template_id = (payload.get("prompt_template_id") or "").strip()
    prompt_template = (payload.get("prompt_template") or "").strip()
    llm_user_content = _compose_user_content_with_prompt_template(content, prompt_template)

    agent_name = (payload.get("agent_name") or "planner_agent").strip()
    known_agents = {item["name"] for item in list_agent_profiles()}
    if agent_name not in known_agents:
        agent_name = "planner_agent"

    temperature = payload.get("temperature", 0.2)
    model_name = (payload.get("model") or "").strip()
    crawler_cookie = (
        (payload.get("crawler_cookie") or "")
        or (request.headers.get("X-Wechat-Cookie") or "")
    ).strip()

    with _STORE_LOCK:
        conversations = _load_conversations()
        conv = _find_by_id(conversations, conversation_id)
        if not conv:
            return jsonify({"success": False, "error": "会话不存在。", "timestamp": _now_iso()}), 404

        user_message = {
            "id": _gen_id("msg"),
            "role": "user",
            "content": content,
            "created_at": _now_iso(),
            "resource_ids": resource_ids,
            "agent_name": agent_name,
            "strategy_id": strategy_id,
            "memory_profile_id": memory_profile_id,
            "prompt_template_id": prompt_template_id,
            "prompt_template": prompt_template,
        }
        conv.setdefault("messages", []).append(user_message)
        conv["updated_at"] = _now_iso()
        _save_conversations(conversations)

    crawled_resource_ids: List[str] = []
    crawl_results: List[Dict] = []
    if agent_name == "crawler_agent":
        crawl_results = crawl_wechat_articles_from_text(
            content,
            output_dir=CRAWLED_DIR,
            cookie_override=crawler_cookie,
        )
        crawl_results = _attach_crawl_relpaths(crawl_results)
        ok_items = [x for x in crawl_results if x.get("status") == "ok"]
        if ok_items:
            with _STORE_LOCK:
                crawled_resource_ids = _register_crawled_resources(ok_items)

        assistant_message_id = _gen_id("msg")
        assistant_created_at = _now_iso()
        assistant_text = _build_crawler_agent_summary(crawl_results)
        assistant_message = {
            "id": assistant_message_id,
            "role": "assistant",
            "content": assistant_text,
            "created_at": assistant_created_at,
            "model": "crawler_direct",
            "provider": "crawler",
            "agent_name": agent_name,
            "strategy_id": strategy_id,
            "memory_profile_id": memory_profile_id,
            "usage": {},
            "used_resource_ids": [],
            "skipped_resource_refs": [],
            "used_memory_resource_ids": [],
            "skipped_memory_refs": [],
            "crawled_resource_ids": crawled_resource_ids,
            "crawl_results": crawl_results,
            "error": False,
            "error_message": "",
        }

        def _crawler_sse(data: Dict) -> str:
            return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

        def generate_crawler():
            yield _crawler_sse(
                {
                    "type": "meta",
                    "conversation_id": conversation_id,
                    "user_message": user_message,
                    "assistant_message": {
                        "id": assistant_message_id,
                        "role": "assistant",
                        "content": "",
                        "created_at": assistant_created_at,
                        "model": "crawler_direct",
                        "provider": "crawler",
                        "agent_name": agent_name,
                        "strategy_id": strategy_id,
                        "memory_profile_id": memory_profile_id,
                        "used_resource_ids": [],
                        "skipped_resource_refs": [],
                        "used_memory_resource_ids": [],
                        "skipped_memory_refs": [],
                        "crawled_resource_ids": crawled_resource_ids,
                        "crawl_results": crawl_results,
                    },
                }
            )
            if assistant_text:
                yield _crawler_sse({"type": "delta", "text": assistant_text})
            with _STORE_LOCK:
                conversations = _load_conversations()
                conv = _find_by_id(conversations, conversation_id)
                if conv:
                    conv.setdefault("messages", []).append(assistant_message)
                    conv["updated_at"] = _now_iso()
                    _save_conversations(conversations)
            yield _crawler_sse({"type": "done"})

        return Response(
            stream_with_context(generate_crawler()),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    resource_context, used_resource_ids, skipped_resources = _build_resource_context(resource_ids)
    history_for_conversation = conv.get("messages", [])[-MAX_HISTORY_MESSAGES:]

    strategy_context, resolved_strategy_id = _build_strategy_context(strategy_id)
    extra_context = strategy_context
    memory_context, memory_meta = _build_memory_profile_context(memory_profile_id)
    resolved_memory_profile_id = (memory_meta.get("memory_profile_id") or "").strip()
    used_memory_resource_ids = memory_meta.get("used_memory_resource_ids") or []
    skipped_memory_refs = memory_meta.get("skipped_memory_refs") or []
    if memory_context:
        if extra_context:
            extra_context += "\n\n"
        extra_context += "长期记忆上下文：\n" + memory_context

    assistant_message_id = _gen_id("msg")
    assistant_created_at = _now_iso()

    def _sse(data: Dict) -> str:
        return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

    def _save_assistant_message(assistant_message: Dict) -> None:
        with _STORE_LOCK:
            conversations = _load_conversations()
            conv = _find_by_id(conversations, conversation_id)
            if conv:
                conv.setdefault("messages", []).append(assistant_message)
                conv["updated_at"] = _now_iso()
                _save_conversations(conversations)

    def generate():
        error_text = ""
        resolved_model = model_name or (os.getenv("OPENAI_MODEL") or "gpt-4o-mini")
        provider = ""
        full_text = ""
        usage = {}

        usage_collector = {}
        try:
            stream, resolved_model, provider, usage_collector = chat_with_agent_stream(
                agent_name=agent_name,
                user_content=llm_user_content,
                history_messages=history_for_conversation,
                resource_context=resource_context,
                extra_context=extra_context,
                model_name=model_name,
                temperature=temperature,
            )
        except Exception as exc:
            error_text = str(exc)
            assistant_text = (
                f"LLM 调用失败：{error_text}\n"
                "请检查 OPENAI_API_KEY / OPENAI_BASE_URL / OPENAI_MODEL 配置。"
            )
            assistant_message = {
                "id": assistant_message_id,
                "role": "assistant",
                "content": assistant_text,
                "created_at": assistant_created_at,
                "model": resolved_model,
                "provider": provider,
                "agent_name": agent_name,
                "strategy_id": resolved_strategy_id,
                "memory_profile_id": resolved_memory_profile_id,
                "usage": usage or {},
                "used_resource_ids": used_resource_ids,
                "skipped_resource_refs": skipped_resources,
                "used_memory_resource_ids": used_memory_resource_ids,
                "skipped_memory_refs": skipped_memory_refs,
                "crawled_resource_ids": crawled_resource_ids,
                "crawl_results": crawl_results,
                "error": True,
                "error_message": error_text,
            }
            _save_assistant_message(assistant_message)
            yield _sse({"type": "error", "message": assistant_text})
            yield _sse({"type": "done"})
            return

        meta = {
            "type": "meta",
            "conversation_id": conversation_id,
            "user_message": user_message,
            "assistant_message": {
                "id": assistant_message_id,
                "role": "assistant",
                "content": "",
                "created_at": assistant_created_at,
                "model": resolved_model,
                "provider": provider,
                "agent_name": agent_name,
                "strategy_id": resolved_strategy_id,
                "memory_profile_id": resolved_memory_profile_id,
                "used_resource_ids": used_resource_ids,
                "skipped_resource_refs": skipped_resources,
                "used_memory_resource_ids": used_memory_resource_ids,
                "skipped_memory_refs": skipped_memory_refs,
                "crawled_resource_ids": crawled_resource_ids,
                "crawl_results": crawl_results,
            },
        }
        yield _sse(meta)

        for chunk in stream:
            if not chunk:
                continue
            full_text += chunk
            yield _sse({"type": "delta", "text": chunk})

        if isinstance(usage_collector, dict):
            usage = usage_collector.get("usage") or usage

        assistant_message = {
            "id": assistant_message_id,
            "role": "assistant",
            "content": full_text,
            "created_at": assistant_created_at,
            "model": resolved_model,
            "provider": provider,
            "agent_name": agent_name,
            "strategy_id": resolved_strategy_id,
            "memory_profile_id": resolved_memory_profile_id,
            "usage": usage or {},
            "used_resource_ids": used_resource_ids,
            "skipped_resource_refs": skipped_resources,
            "used_memory_resource_ids": used_memory_resource_ids,
            "skipped_memory_refs": skipped_memory_refs,
            "crawled_resource_ids": crawled_resource_ids,
            "crawl_results": crawl_results,
            "error": bool(error_text),
            "error_message": error_text,
        }
        _save_assistant_message(assistant_message)
        yield _sse({"type": "done"})

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@strategy_watch_bp.route("/api/strategy-watch/strategies", methods=["GET"])
def list_strategy_watch_strategies():
    with _STORE_LOCK:
        payload = _load_strategies_payload()
        strategies = payload.get("strategies", [])
        active_strategy_id = payload.get("active_strategy_id") or ""
    strategies = sorted(strategies, key=lambda x: x.get("updated_at", ""), reverse=True)
    return jsonify(
        {
            "success": True,
            "data": {
                "strategies": strategies,
                "active_strategy_id": active_strategy_id,
            },
            "timestamp": _now_iso(),
        }
    )


@strategy_watch_bp.route("/api/strategy-watch/strategies", methods=["POST"])
def create_strategy_watch_strategy():
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip() or f"看盘策略 {datetime.now().strftime('%m-%d %H:%M')}"
    description = (payload.get("description") or "").strip()
    view_type = (payload.get("view_type") or DEFAULT_STRATEGY_VIEW).strip() or DEFAULT_STRATEGY_VIEW
    config = payload.get("config") or {}

    strategy = {
        "id": _gen_id("strategy"),
        "name": name,
        "description": description,
        "view_type": view_type,
        "config": config,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
    }

    with _STORE_LOCK:
        payload = _load_strategies_payload()
        strategies = payload.get("strategies", [])
        strategies.append(strategy)
        payload["strategies"] = strategies
        if not payload.get("active_strategy_id"):
            payload["active_strategy_id"] = strategy["id"]
        _save_strategies_payload(payload)

    return jsonify({"success": True, "data": strategy, "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/strategies/<string:strategy_id>", methods=["PATCH"])
def update_strategy_watch_strategy(strategy_id: str):
    payload = request.get_json(silent=True) or {}
    with _STORE_LOCK:
        data = _load_strategies_payload()
        strategies = data.get("strategies", [])
        target = _find_by_id(strategies, strategy_id)
        if not target:
            return jsonify({"success": False, "error": "策略不存在。", "timestamp": _now_iso()}), 404

        if "name" in payload:
            target["name"] = (payload.get("name") or "").strip() or target.get("name")
        if "description" in payload:
            target["description"] = (payload.get("description") or "").strip()
        if "view_type" in payload:
            target["view_type"] = (payload.get("view_type") or DEFAULT_STRATEGY_VIEW).strip() or DEFAULT_STRATEGY_VIEW
        if "config" in payload and isinstance(payload.get("config"), dict):
            target["config"] = payload.get("config")
        target["updated_at"] = _now_iso()

        data["strategies"] = strategies
        _save_strategies_payload(data)

    return jsonify({"success": True, "data": target, "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/strategies/<string:strategy_id>", methods=["DELETE"])
def delete_strategy_watch_strategy(strategy_id: str):
    with _STORE_LOCK:
        data = _load_strategies_payload()
        strategies = data.get("strategies", [])
        target = _find_by_id(strategies, strategy_id)
        if not target:
            return jsonify({"success": False, "error": "策略不存在。", "timestamp": _now_iso()}), 404
        strategies = [item for item in strategies if item.get("id") != strategy_id]
        data["strategies"] = strategies
        if data.get("active_strategy_id") == strategy_id:
            data["active_strategy_id"] = ""
        _save_strategies_payload(data)

    return jsonify({"success": True, "message": "策略已删除。", "timestamp": _now_iso()})


@strategy_watch_bp.route("/api/strategy-watch/strategies/active", methods=["PATCH"])
def set_active_strategy_watch_strategy():
    payload = request.get_json(silent=True) or {}
    strategy_id = (payload.get("strategy_id") or "").strip()

    with _STORE_LOCK:
        data = _load_strategies_payload()
        strategies = data.get("strategies", [])
        if strategy_id and not _find_by_id(strategies, strategy_id):
            return jsonify({"success": False, "error": "策略不存在。", "timestamp": _now_iso()}), 404
        data["active_strategy_id"] = strategy_id
        _save_strategies_payload(data)

    return jsonify(
        {"success": True, "data": {"active_strategy_id": strategy_id}, "timestamp": _now_iso()}
    )


@strategy_watch_bp.route("/api/strategy-watch/strategies/<string:strategy_id>/generate-view", methods=["POST"])
def generate_strategy_watch_view(strategy_id: str):
    payload = request.get_json(silent=True) or {}
    goal = (payload.get("goal") or "").strip()
    if not goal:
        return jsonify({"success": False, "error": "目标描述不能为空。", "timestamp": _now_iso()}), 400

    with _STORE_LOCK:
        data = _load_strategies_payload()
        strategies = data.get("strategies", [])
        target = _find_by_id(strategies, strategy_id)
        if not target:
            return jsonify({"success": False, "error": "策略不存在。", "timestamp": _now_iso()}), 404

    prompt = _build_visualization_prompt(goal, view_type=target.get("view_type", ""))
    try:
        reply, resolved_model, provider, _usage = chat_with_agent(
            agent_name="visualization_agent",
            user_content=prompt,
            history_messages=[],
            resource_context="",
            extra_context="",
            model_name="",
            temperature=0.2,
        )
    except Exception as exc:
        return jsonify(
            {"success": False, "error": f"生成视图失败: {exc}", "timestamp": _now_iso()}), 500

    view_config = _extract_json_block(reply)
    if not view_config:
        return jsonify(
            {"success": False, "error": "未能解析Agent输出的JSON配置。", "timestamp": _now_iso()}), 500

    with _STORE_LOCK:
        data = _load_strategies_payload()
        strategies = data.get("strategies", [])
        target = _find_by_id(strategies, strategy_id)
        if not target:
            return jsonify({"success": False, "error": "策略不存在。", "timestamp": _now_iso()}), 404

        target["name"] = (view_config.get("name") or target.get("name") or "").strip() or target.get("name")
        target["description"] = (view_config.get("description") or target.get("description") or "").strip()
        target["view_type"] = (
            (view_config.get("view_type") or target.get("view_type") or DEFAULT_STRATEGY_VIEW).strip()
        )
        widgets = view_config.get("widgets")
        if isinstance(widgets, list):
            target["config"] = {"widgets": widgets}
        target["updated_at"] = _now_iso()
        data["strategies"] = strategies
        _save_strategies_payload(data)

    return jsonify(
        {
            "success": True,
            "data": {
                "strategy_id": strategy_id,
                "view_config": view_config,
                "model": resolved_model,
                "provider": provider,
            },
            "timestamp": _now_iso(),
        }
    )

