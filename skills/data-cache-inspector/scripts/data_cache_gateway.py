#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

AVAILABLE_TABLES: Dict[str, str] = {
    "index_daily_metadata": "indices/index_daily_metadata.parquet",
    "index_minute_metadata": "indices/index_minute_metadata.parquet",
    "stock_daily_metadata": "stock_daily/stock_daily_metadata.parquet",
    "market_metadata": "other/market_metadata.parquet",
    "market_states": "other/market_states.parquet",
    "sectors_ths": "sectors/sectors_ths.parquet",
    "sectors_dc": "sectors/sectors_dc.parquet",
}

TABLE_ALIASES: Dict[str, str] = {
    "index_daily": "index_daily_metadata",
    "index_minute": "index_minute_metadata",
    "stock_daily": "stock_daily_metadata",
    "market": "market_metadata",
    "market_meta": "market_metadata",
    "market_state": "market_states",
    "sector_ths": "sectors_ths",
    "sector_dc": "sectors_dc",
}


def _base_dir() -> Path:
    return Path(__file__).resolve().parents[3]


def normalize_date_ymd(value: Any) -> str:
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


def _detect_date_column(schema_items: List[Tuple[str, Any]]) -> str:
    if not schema_items:
        return ""
    preferred = ("日期", "date", "trade_date", "时间", "datetime", "timestamp")
    lowered_map = {str(name).strip().lower(): str(name) for name, _ in schema_items}
    for key in preferred:
        hit = lowered_map.get(key.lower())
        if hit:
            return hit
    for name, dtype in schema_items:
        dtype_text = str(dtype or "")
        if "Date" in dtype_text or "Datetime" in dtype_text:
            return str(name)
    return ""


def _resolve_table(table: str) -> Tuple[str, Path]:
    raw = str(table or "").strip().lower().replace("\\", "/")
    table_key = TABLE_ALIASES.get(raw, raw)
    if table_key not in AVAILABLE_TABLES:
        for key, rel in AVAILABLE_TABLES.items():
            rel_norm = rel.lower().replace("\\", "/")
            rel_no_ext = rel_norm[:-8] if rel_norm.endswith(".parquet") else rel_norm
            if raw in {rel_norm, rel_no_ext, key.lower()}:
                table_key = key
                break
    if table_key not in AVAILABLE_TABLES:
        raise ValueError("Unknown table. Available: " + ", ".join(sorted(AVAILABLE_TABLES.keys())))
    rel = AVAILABLE_TABLES[table_key]
    return table_key, (_base_dir() / "data_cache" / rel)


def _parse_columns(columns: str, available_columns: List[str]) -> Tuple[List[str], List[str]]:
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


def query_data_cache_table(
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
        table_key, table_path = _resolve_table(table)
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
    date_col = _detect_date_column(list(schema.items()))
    valid_columns, invalid_columns = _parse_columns(columns, available_columns)
    selected_columns = valid_columns if valid_columns else available_columns[: min(18, len(available_columns))]

    working = lf
    date_norm = normalize_date_ymd(date)
    date_from_norm = normalize_date_ymd(date_from)
    date_to_norm = normalize_date_ymd(date_to)
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
            return {"ok": False, "error": f"filter_column not found: {fcol}", "available_columns": available_columns}
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
        "path": table_path.relative_to(_base_dir()).as_posix(),
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


def compare_market_turnover_dates(date_a: str, date_b: str) -> Dict[str, Any]:
    date_col_name = "日期"
    turnover_col_name = "成交总额"
    date_a_norm = normalize_date_ymd(date_a)
    date_b_norm = normalize_date_ymd(date_b)
    lo_date, hi_date = date_a_norm, date_b_norm
    if lo_date and hi_date and lo_date > hi_date:
        lo_date, hi_date = hi_date, lo_date

    query_result = query_data_cache_table(
        table="market_metadata",
        columns=f"{date_col_name},{turnover_col_name}",
        date_from=lo_date,
        date_to=hi_date,
        sort_desc=False,
        limit=4000,
    )
    if not query_result.get("ok"):
        return {"ok": False, "error": query_result.get("error", "query failed")}

    rows = query_result.get("rows") or []
    values_by_date: Dict[str, Any] = {}
    for row in rows:
        dval = normalize_date_ymd((row or {}).get(date_col_name))
        if not dval:
            continue
        if dval in {date_a_norm, date_b_norm}:
            values_by_date[dval] = (row or {}).get(turnover_col_name)

    val_a_raw = values_by_date.get(date_a_norm)
    val_b_raw = values_by_date.get(date_b_norm)
    val_a = _as_float(val_a_raw)
    val_b = _as_float(val_b_raw)
    result: Dict[str, Any] = {
        "ok": bool(date_a_norm and date_b_norm and val_a is not None and val_b is not None),
        "date_a": date_a_norm,
        "date_b": date_b_norm,
        "turnover_a": val_a_raw,
        "turnover_b": val_b_raw,
        "change_abs": (val_a - val_b) if (val_a is not None and val_b is not None) else None,
        "change_pct": ((val_a - val_b) / val_b * 100.0) if (val_a is not None and val_b not in (None, 0.0)) else None,
        "available_dates": sorted(values_by_date.keys()),
    }
    if not result["ok"]:
        result["error"] = (
            "failed to find both target dates or parse turnover as number. "
            "Use query_data_cache_table for manual inspection."
        )
    return result
