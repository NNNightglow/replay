#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tushare index_weight manager.

Use cases:
1. Fetch monthly index constituents and weights for long horizons.
2. Persist raw history and a monthly latest snapshot.
"""

from __future__ import annotations

import argparse
import calendar
import json
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
import polars as pl


DEFAULT_INDEX_CODE_CANDIDATES: Dict[str, List[str]] = {
    # Common aliases in Tushare docs / market usage.
    "HS300": ["399300.SZ", "000300.SH"],
    "CSI500": ["399905.SZ", "000905.SH"],
    "CSI1000": ["000852.SH", "399852.SZ"],
}


class IndexWeightManager:
    def __init__(self, cache_dir: str = "data_cache/indices"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.history_file = self.cache_dir / "index_weight_history.parquet"
        self.monthly_snapshot_file = self.cache_dir / "index_weight_monthly_latest.parquet"
        self.state_file = self.cache_dir / "index_weight_sync_state.json"

    @staticmethod
    def _to_compact_date(value: Optional[object]) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime("%Y%m%d")
        if isinstance(value, date):
            return value.strftime("%Y%m%d")
        if isinstance(value, str):
            text = value.strip()
            if len(text) == 8 and text.isdigit():
                return text
            if len(text) == 10 and "-" in text:
                return text.replace("-", "")
        return None

    @staticmethod
    def _read_env_keys_from_dotenv(keys: Sequence[str], dotenv_file: str = ".env") -> Optional[str]:
        dotenv_path = Path(dotenv_file)
        if not dotenv_path.exists():
            return None

        try:
            lines = dotenv_path.read_text(encoding="utf-8").splitlines()
        except Exception:
            return None

        key_set = set(keys)
        for raw_line in lines:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            left, right = line.split("=", 1)
            env_key = left.strip()
            if env_key not in key_set:
                continue

            value = right.strip().strip('"').strip("'")
            if value:
                os.environ.setdefault(env_key, value)
                return value
        return None

    @classmethod
    def _init_tushare_client(cls):
        try:
            import tushare as ts
        except Exception as exc:
            raise RuntimeError("tushare is not installed or import failed") from exc

        token = (
            os.getenv("TUSHARE_API_KEY")
            or os.getenv("TUSHARE_TOKEN")
            or os.getenv("TS_TOKEN")
        )
        if not token:
            token = cls._read_env_keys_from_dotenv(
                keys=("TUSHARE_API_KEY", "TUSHARE_TOKEN", "TS_TOKEN"),
                dotenv_file=".env",
            )
        if not token:
            raise RuntimeError("Tushare token not found. Configure TUSHARE_API_KEY in .env")

        pro = ts.pro_api(token)
        # Keep consistency with existing project behavior.
        pro._DataApi__token = token
        pro._DataApi__http_url = os.getenv(
            "TUSHARE_HTTP_URL",
            "http://lianghua.nanyangqiankun.top",
        )
        return pro

    @staticmethod
    def _month_windows(start_compact: str, end_compact: str) -> List[Tuple[str, str, str]]:
        start_d = datetime.strptime(start_compact, "%Y%m%d").date()
        end_d = datetime.strptime(end_compact, "%Y%m%d").date()
        if start_d > end_d:
            return []

        cursor = date(start_d.year, start_d.month, 1)
        end_month = date(end_d.year, end_d.month, 1)
        windows: List[Tuple[str, str, str]] = []
        while cursor <= end_month:
            last_day = calendar.monthrange(cursor.year, cursor.month)[1]
            first = cursor.strftime("%Y%m01")
            last = cursor.replace(day=last_day).strftime("%Y%m%d")
            windows.append((cursor.strftime("%Y%m"), first, last))
            if cursor.month == 12:
                cursor = date(cursor.year + 1, 1, 1)
            else:
                cursor = date(cursor.year, cursor.month + 1, 1)
        return windows

    @staticmethod
    def _empty_history_df() -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "index_alias": pl.Utf8,
                "index_code": pl.Utf8,
                "query_code": pl.Utf8,
                "con_code": pl.Utf8,
                "trade_date": pl.Utf8,
                "weight": pl.Float64,
                "query_month": pl.Utf8,
                "fetched_at": pl.Utf8,
            }
        )

    def load_history(self) -> pl.DataFrame:
        if not self.history_file.exists():
            return self._empty_history_df()
        try:
            df = pl.read_parquet(self.history_file)
        except Exception:
            return self._empty_history_df()

        expected_cols = self._empty_history_df().columns
        for col in expected_cols:
            if col not in df.columns:
                dtype = self._empty_history_df().schema[col]
                df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))

        return df.select(expected_cols).with_columns(
            [
                pl.col("index_alias").cast(pl.Utf8, strict=False),
                pl.col("index_code").cast(pl.Utf8, strict=False),
                pl.col("query_code").cast(pl.Utf8, strict=False),
                pl.col("con_code").cast(pl.Utf8, strict=False),
                pl.col("trade_date").cast(pl.Utf8, strict=False),
                pl.col("weight").cast(pl.Float64, strict=False),
                pl.col("query_month").cast(pl.Utf8, strict=False),
                pl.col("fetched_at").cast(pl.Utf8, strict=False),
            ]
        )

    def _write_state(self, state: Dict[str, object]) -> None:
        try:
            self.state_file.write_text(
                json.dumps(state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    @staticmethod
    def _normalize_index_weight_df(
        raw_df: pd.DataFrame,
        index_alias: str,
        query_code: str,
        query_month: str,
    ) -> Optional[pl.DataFrame]:
        if raw_df is None or raw_df.empty:
            return None

        for col in ("index_code", "con_code", "trade_date", "weight"):
            if col not in raw_df.columns:
                raw_df[col] = None

        pl_df = pl.from_pandas(raw_df[["index_code", "con_code", "trade_date", "weight"]])
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        pl_df = pl_df.with_columns(
            [
                pl.lit(index_alias).alias("index_alias"),
                pl.col("index_code").cast(pl.Utf8, strict=False),
                pl.lit(query_code).alias("query_code"),
                pl.col("con_code").cast(pl.Utf8, strict=False),
                pl.col("trade_date").cast(pl.Utf8, strict=False),
                pl.col("weight").cast(pl.Float64, strict=False),
                pl.lit(query_month).alias("query_month"),
                pl.lit(fetched_at).alias("fetched_at"),
            ]
        )
        return pl_df.select(
            [
                "index_alias",
                "index_code",
                "query_code",
                "con_code",
                "trade_date",
                "weight",
                "query_month",
                "fetched_at",
            ]
        )

    def fetch_index_weight_history(
        self,
        start_date: str = "20160101",
        end_date: Optional[str] = None,
        index_code_candidates: Optional[Dict[str, List[str]]] = None,
        force_full: bool = False,
        max_retries: int = 3,
        throttle_seconds: float = 0.08,
    ) -> bool:
        pro = self._init_tushare_client()
        start_compact = self._to_compact_date(start_date) or "20160101"
        end_compact = self._to_compact_date(end_date) or datetime.now().strftime("%Y%m%d")
        windows = self._month_windows(start_compact, end_compact)
        if not windows:
            print("No valid month window to fetch.")
            return False

        candidates = index_code_candidates or DEFAULT_INDEX_CODE_CANDIDATES
        existing = self._empty_history_df() if force_full else self.load_history()
        new_chunks: List[pl.DataFrame] = []

        failures: List[Dict[str, str]] = []
        total_tasks = len(windows) * len(candidates)
        done = 0

        for query_month, month_start, month_end in windows:
            for index_alias, code_candidates in candidates.items():
                done += 1
                print(
                    f"[{done}/{total_tasks}] fetch {index_alias} {query_month} "
                    f"({month_start}-{month_end})"
                )

                fetched = False
                for query_code in code_candidates:
                    last_error: Optional[str] = None
                    raw_df: Optional[pd.DataFrame] = None

                    for attempt in range(max_retries):
                        try:
                            raw_df = pro.index_weight(
                                index_code=query_code,
                                start_date=month_start,
                                end_date=month_end,
                            )
                            break
                        except Exception as exc:
                            last_error = str(exc)
                            wait_seconds = (2 ** attempt) + 0.2
                            print(
                                f"  retry {attempt + 1}/{max_retries} "
                                f"{query_code} failed: {last_error}"
                            )
                            time.sleep(wait_seconds)

                    if raw_df is not None and not raw_df.empty:
                        normalized = self._normalize_index_weight_df(
                            raw_df=raw_df,
                            index_alias=index_alias,
                            query_code=query_code,
                            query_month=query_month,
                        )
                        if normalized is not None and not normalized.is_empty():
                            new_chunks.append(normalized)
                            fetched = True
                            print(f"  success via {query_code}: {normalized.height} rows")
                            break
                    elif last_error:
                        print(f"  failed via {query_code}: {last_error}")

                    if throttle_seconds > 0:
                        time.sleep(throttle_seconds)

                if not fetched:
                    failures.append(
                        {
                            "index_alias": index_alias,
                            "query_month": query_month,
                            "reason": "all candidates empty_or_failed",
                        }
                    )

        if new_chunks:
            new_df = pl.concat(new_chunks, how="vertical_relaxed")
            merged = pl.concat([existing, new_df], how="vertical_relaxed")
            merged = merged.unique(
                subset=["index_alias", "index_code", "con_code", "trade_date"],
                keep="last",
            ).sort(["index_alias", "trade_date", "con_code"])
        else:
            merged = existing

        merged.write_parquet(self.history_file)
        snapshot = self.build_monthly_latest_snapshot(history_df=merged)
        snapshot.write_parquet(self.monthly_snapshot_file)

        self._write_state(
            {
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "start_date": start_compact,
                "end_date": end_compact,
                "history_rows": int(merged.height),
                "monthly_rows": int(snapshot.height),
                "failed_tasks": failures,
                "source": "tushare.index_weight",
            }
        )

        print(
            f"Done. history={merged.height}, monthly_snapshot={snapshot.height}, "
            f"failures={len(failures)}"
        )
        return True

    @staticmethod
    def build_monthly_latest_snapshot(history_df: pl.DataFrame) -> pl.DataFrame:
        if history_df is None or history_df.is_empty():
            return pl.DataFrame(
                schema={
                    "index_alias": pl.Utf8,
                    "index_code": pl.Utf8,
                    "query_month": pl.Utf8,
                    "trade_date": pl.Utf8,
                    "con_code": pl.Utf8,
                    "weight": pl.Float64,
                }
            )

        keyed = history_df.with_columns(
            [
                pl.col("query_month").cast(pl.Utf8, strict=False),
                pl.col("trade_date").cast(pl.Utf8, strict=False),
            ]
        )

        latest_trade = keyed.group_by(["index_alias", "query_month"]).agg(
            pl.col("trade_date").max().alias("_latest_trade_date")
        )

        snapshot = keyed.join(
            latest_trade,
            on=["index_alias", "query_month"],
            how="inner",
        ).filter(pl.col("trade_date") == pl.col("_latest_trade_date"))

        return (
            snapshot.select(
                [
                    "index_alias",
                    "index_code",
                    "query_month",
                    "trade_date",
                    "con_code",
                    "weight",
                ]
            )
            .unique(
                subset=["index_alias", "query_month", "con_code"],
                keep="last",
            )
            .sort(["index_alias", "query_month", "con_code"])
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Tushare index_weight data")
    parser.add_argument("--start-date", default="20160101", help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--force-full", action="store_true", help="Ignore old cache")
    parser.add_argument(
        "--indices",
        default="HS300,CSI500,CSI1000",
        help="Comma-separated aliases from HS300,CSI500,CSI1000",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    alias_list = [x.strip().upper() for x in args.indices.split(",") if x.strip()]
    candidates: Dict[str, List[str]] = {}
    for alias in alias_list:
        if alias in DEFAULT_INDEX_CODE_CANDIDATES:
            candidates[alias] = DEFAULT_INDEX_CODE_CANDIDATES[alias]

    if not candidates:
        raise SystemExit("No valid indices selected. Use HS300,CSI500,CSI1000.")

    mgr = IndexWeightManager()
    ok = mgr.fetch_index_weight_history(
        start_date=args.start_date,
        end_date=args.end_date,
        index_code_candidates=candidates,
        force_full=args.force_full,
    )
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

