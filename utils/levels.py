#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
关键位计算与Parquet缓存

提供：
- compute_key_levels_from_market_states: 基于全局 market_states 的个股K线数据，计算当前价到历史高点之间的关键位（轻量版）
- read_levels_cache / write_levels_cache: 读取/写入 Parquet 缓存

缓存文件路径建议： data_cache/other/key_levels.parquet
"""

from __future__ import annotations

import os
import json
from datetime import date, timedelta
from typing import Dict, Any, List, Optional

import polars as pl


DEFAULT_CACHE_PATH = os.path.join('data_cache', 'other', 'key_levels.parquet')


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def read_levels_cache(cache_path: str = DEFAULT_CACHE_PATH) -> pl.DataFrame:
    """读取Parquet缓存文件，不存在则返回空DataFrame。

    当前缓存结构（变化点区间）：
    - code, window_days, method_ver
    - effective_from, effective_to
    - levels(JSON字符串), ath, current, updated_at
    """
    empty_schema = {
        'code': pl.Series([], dtype=pl.Utf8),
        'window_days': pl.Series([], dtype=pl.Int64),
        'method_ver': pl.Series([], dtype=pl.Utf8),
        'effective_from': pl.Series([], dtype=pl.Utf8),
        'effective_to': pl.Series([], dtype=pl.Utf8),
        'levels': pl.Series([], dtype=pl.Utf8),
        'ath': pl.Series([], dtype=pl.Float64),
        'current': pl.Series([], dtype=pl.Float64),
        'updated_at': pl.Series([], dtype=pl.Utf8),
    }
    try:
        if not os.path.exists(cache_path):
            return pl.DataFrame(empty_schema)

        df = pl.read_parquet(cache_path)

        # 兼容旧结构：按日快照 -> 变化点区间
        if 'date' in df.columns and 'effective_from' not in df.columns:
            migrated = df.with_columns([
                pl.col('date').alias('effective_from'),
                pl.col('date').alias('effective_to'),
            ])
            keep_cols = [
                'code', 'window_days', 'method_ver',
                'effective_from', 'effective_to',
                'levels', 'ath', 'current', 'updated_at'
            ]
            migrated = migrated.select([c for c in keep_cols if c in migrated.columns])
            # 统一类型
            migrated = migrated.with_columns([
                pl.col('code').cast(pl.Utf8),
                pl.col('window_days').cast(pl.Int64),
                pl.col('method_ver').cast(pl.Utf8),
                pl.col('effective_from').cast(pl.Utf8),
                pl.col('effective_to').cast(pl.Utf8),
                pl.col('levels').cast(pl.Utf8),
                pl.col('ath').cast(pl.Float64, strict=False),
                pl.col('current').cast(pl.Float64, strict=False),
                pl.col('updated_at').cast(pl.Utf8),
            ])
            migrated.write_parquet(cache_path)
            return migrated

        # 新结构对齐
        for col_name, series in empty_schema.items():
            if col_name not in df.columns:
                df = df.with_columns(pl.lit(None, dtype=series.dtype).alias(col_name))
        return df.select(list(empty_schema.keys())).with_columns([
            pl.col('code').cast(pl.Utf8),
            pl.col('window_days').cast(pl.Int64),
            pl.col('method_ver').cast(pl.Utf8),
            pl.col('effective_from').cast(pl.Utf8),
            pl.col('effective_to').cast(pl.Utf8),
            pl.col('levels').cast(pl.Utf8),
            pl.col('ath').cast(pl.Float64, strict=False),
            pl.col('current').cast(pl.Float64, strict=False),
            pl.col('updated_at').cast(pl.Utf8),
        ])
    except Exception:
        # 若损坏则返回空
        return pl.DataFrame(empty_schema)


def write_levels_cache(record: Dict[str, Any], cache_path: str = DEFAULT_CACHE_PATH) -> None:
    """写入关键位变化点缓存（仅关键位变化时新增区间）。

    record 必须包含:
    - code, effective_from(YYYY-MM-DD), window_days, method_ver
    - levels(JSON字符串或list), ath, current, updated_at
    """
    _ensure_parent_dir(cache_path)
    df = read_levels_cache(cache_path)

    code = str(record['code']).zfill(6)
    effective_from = record.get('effective_from', record.get('date'))
    if effective_from is None:
        raise ValueError('write_levels_cache requires effective_from')
    date_str = str(effective_from)
    window_days = int(record['window_days'])
    method_ver = str(record['method_ver'])
    updated_at = str(record.get('updated_at', ''))
    ath = record.get('ath')
    current = record.get('current')
    levels_value = record.get('levels', [])
    if isinstance(levels_value, str):
        try:
            levels_list = json.loads(levels_value)
        except Exception:
            levels_list = []
    else:
        levels_list = levels_value if isinstance(levels_value, list) else []
    normalized_levels = [float(x) for x in levels_list if x is not None]
    normalized_levels_str = json.dumps(normalized_levels, ensure_ascii=False)

    rec_base = {
        'code': code,
        'window_days': window_days,
        'method_ver': method_ver,
        'levels': normalized_levels_str,
        'ath': ath,
        'current': current,
        'updated_at': updated_at,
    }
    if df.is_empty():
        first_df = pl.DataFrame([{
            **rec_base,
            'effective_from': date_str,
            'effective_to': None,
        }]).select([
            'code', 'window_days', 'method_ver',
            'effective_from', 'effective_to',
            'levels', 'ath', 'current', 'updated_at'
        ])
        first_df.write_parquet(cache_path)
        return

    key_mask = (
        (pl.col('code') == code) &
        (pl.col('window_days') == window_days) &
        (pl.col('method_ver') == method_ver)
    )
    key_df = df.filter(key_mask).sort('effective_from')
    other_df = df.filter(~key_mask)

    # 当前日期可命中的区间（优先）
    active_df = key_df.filter(
        (pl.col('effective_from') <= date_str) &
        (
            pl.col('effective_to').is_null() |
            (pl.col('effective_to') >= date_str)
        )
    )

    if not active_df.is_empty():
        active = active_df.tail(1).to_dicts()[0]
        if str(active.get('levels', '')) == normalized_levels_str:
            # 关键位不变：仅刷新更新时间、ath、current
            active_from = active.get('effective_from')
            key_df = key_df.with_columns([
                pl.when(
                    (pl.col('effective_from') == active_from) &
                    (
                        (pl.col('effective_to') == active.get('effective_to')) |
                        (pl.col('effective_to').is_null() & (active.get('effective_to') is None))
                    )
                ).then(pl.lit(updated_at)).otherwise(pl.col('updated_at')).alias('updated_at'),
                pl.when(
                    (pl.col('effective_from') == active_from) &
                    (
                        (pl.col('effective_to') == active.get('effective_to')) |
                        (pl.col('effective_to').is_null() & (active.get('effective_to') is None))
                    )
                ).then(pl.lit(ath, dtype=pl.Float64)).otherwise(pl.col('ath')).alias('ath'),
                pl.when(
                    (pl.col('effective_from') == active_from) &
                    (
                        (pl.col('effective_to') == active.get('effective_to')) |
                        (pl.col('effective_to').is_null() & (active.get('effective_to') is None))
                    )
                ).then(pl.lit(current, dtype=pl.Float64)).otherwise(pl.col('current')).alias('current'),
            ])
            out_df = pl.concat([other_df, key_df], how='vertical_relaxed')
            out_df.write_parquet(cache_path)
            return

        # 关键位变化：关闭旧区间并新增新区间（effective_from 起生效）
        prev_day = (date.fromisoformat(date_str) - timedelta(days=1)).strftime('%Y-%m-%d')
        active_from = active.get('effective_from')
        active_to = active.get('effective_to')
        key_df = key_df.with_columns([
            pl.when(
                (pl.col('effective_from') == active_from) &
                (
                    (pl.col('effective_to') == active_to) |
                    (pl.col('effective_to').is_null() & (active_to is None))
                )
            ).then(pl.lit(prev_day)).otherwise(pl.col('effective_to')).alias('effective_to')
        ])
        new_row = pl.DataFrame([{
            **rec_base,
            'effective_from': date_str,
            'effective_to': active_to,
        }]).select(key_df.columns)
        key_df = pl.concat([key_df, new_row], how='vertical_relaxed').sort('effective_from')
    else:
        # 无可命中区间：直接新增一个开放区间
        new_row = pl.DataFrame([{
            **rec_base,
            'effective_from': date_str,
            'effective_to': None,
        }]).select(key_df.columns)
        key_df = pl.concat([key_df, new_row], how='vertical_relaxed').sort('effective_from')

    out_df = pl.concat([other_df, key_df], how='vertical_relaxed')
    out_df.write_parquet(cache_path)


def get_levels_cache_for_date(
    cache_df: pl.DataFrame,
    code: str,
    date_str: str,
    window_days: int,
    method_ver: str
) -> Optional[Dict[str, Any]]:
    """按日期命中变化点区间缓存。"""
    if cache_df is None or cache_df.is_empty():
        return None
    matched = cache_df.filter(
        (pl.col('code') == str(code).zfill(6)) &
        (pl.col('window_days') == int(window_days)) &
        (pl.col('method_ver') == str(method_ver)) &
        (pl.col('effective_from') <= date_str) &
        (
            pl.col('effective_to').is_null() |
            (pl.col('effective_to') >= date_str)
        )
    ).sort('effective_from')
    if matched.is_empty():
        return None
    return matched.tail(1).to_dicts()[0]


def compute_key_levels_from_market_states(
    market_states: pl.DataFrame,
    code: str,
    selected_date: date,
    window_days: int = 3650,
    method_ver: str = 'v2'
) -> Dict[str, Any]:
    """按“关键日”规则计算关键位（关键位仅取关键日最高价）。

    关键日筛选（t 日）：
    1) 成交额需 >= t-1 与 t+1 的 140%
    2) 最高价需不低于相邻两日最高价（允许 5% 以内偏差）
    3) 最高价需接近近 10 日最高价（不低于近 10 日最高价的 95%）

    候选关键日统一后按优先级排序：
    1) 成交额大
    2) 价格高
    3) 当日K线穿过其他关键价格的数量多（10% 容差）

    最终关键位约束：
    - 任意两个关键位价格差需超过 10%
    - 若新加入关键位与已选关键位在 10% 内冲突，按“时间近到远、成交额大到小”取舍
    """
    if market_states is None or market_states.is_empty():
        raise ValueError('market_states is empty')

    code = str(code).zfill(6)

    start_date = selected_date - timedelta(days=window_days)
    df = market_states.filter(
        (pl.col('代码') == code) &
        (pl.col('日期') >= start_date) &
        (pl.col('日期') <= selected_date)
    ).sort('日期')

    if df.is_empty():
        return {
            'code': code,
            'date': selected_date.strftime('%Y-%m-%d'),
            'window_days': window_days,
            'method_ver': method_ver,
            'levels': [],
            'ath': None,
            'current': None,
        }

    pd_df = df.to_pandas()
    closes = pd_df.get('收盘').astype(float).tolist()
    highs = pd_df.get('最高').astype(float).tolist()
    lows = pd_df.get('最低').astype(float).tolist()
    amount_series = pd_df.get('成交额')
    if amount_series is None:
        amount_series = pd_df.get('成交量')
    amounts = amount_series.astype(float).tolist() if amount_series is not None else [0.0 for _ in closes]

    if not closes:
        return {
            'code': code,
            'date': selected_date.strftime('%Y-%m-%d'),
            'window_days': window_days,
            'method_ver': method_ver,
            'levels': [],
            'ath': None,
            'current': None,
        }

    current_price = float(closes[-1]) if closes else None
    ath = float(max([h for h in highs if h == h], default=float('nan')))
    n = len(highs)
    if n < 3:
        return {
            'code': code,
            'date': selected_date.strftime('%Y-%m-%d'),
            'window_days': window_days,
            'method_ver': method_ver,
            'levels': [],
            'ath': float(ath) if ath == ath else None,
            'current': float(current_price) if current_price == current_price else None,
        }

    # 先选“关键日”：仅允许由当日最高价构成关键位
    key_days: List[Dict[str, Any]] = []
    near_peak_tolerance = 0.05
    cross_tolerance = 0.10
    for i in range(1, n - 1):
        hi_t = float(highs[i]) if highs[i] == highs[i] else None
        hi_prev = float(highs[i - 1]) if highs[i - 1] == highs[i - 1] else None
        hi_next = float(highs[i + 1]) if highs[i + 1] == highs[i + 1] else None
        amt_t = float(amounts[i]) if i < len(amounts) and amounts[i] == amounts[i] else None
        amt_prev = float(amounts[i - 1]) if i - 1 < len(amounts) and amounts[i - 1] == amounts[i - 1] else None
        amt_next = float(amounts[i + 1]) if i + 1 < len(amounts) and amounts[i + 1] == amounts[i + 1] else None
        lo_t = float(lows[i]) if lows[i] == lows[i] else None
        if None in (hi_t, hi_prev, hi_next, amt_t, amt_prev, amt_next, lo_t):
            continue
        if amt_prev <= 0 or amt_next <= 0:
            continue

        # 条件1：成交额放大到相邻两日的 140% 以上
        if not (amt_t >= 1.4 * amt_prev and amt_t >= 1.4 * amt_next):
            continue

        # 条件2：当日高点不低于相邻高点（允许 5% 偏差）
        neighbor_max = max(hi_prev, hi_next)
        if hi_t < neighbor_max * (1.0 - near_peak_tolerance):
            continue

        # 条件3：接近近10日高点（不低于近10日最高价的95%）
        lookback_start = max(0, i - 9)
        high_10d = max(float(x) for x in highs[lookback_start:i + 1] if x == x)
        if high_10d <= 0:
            continue
        if hi_t < high_10d * (1.0 - near_peak_tolerance):
            continue

        key_days.append({
            'idx': i,
            'price': hi_t,
            'amount': amt_t,
            'low': lo_t,
            'cross_count': 0,
        })

    if not key_days:
        return {
            'code': code,
            'date': selected_date.strftime('%Y-%m-%d'),
            'window_days': window_days,
            'method_ver': method_ver,
            'levels': [],
            'ath': float(ath) if ath == ath else None,
            'current': float(current_price) if current_price == current_price else None,
        }

    # 计算“穿过其他关键价格数量”（10% 容差）
    key_prices = [d['price'] for d in key_days]
    for d in key_days:
        low_band = d['low'] * (1.0 - cross_tolerance)
        high_band = d['price'] * (1.0 + cross_tolerance)
        d['cross_count'] = sum(
            1 for p in key_prices
            if p != d['price'] and low_band <= p <= high_band
        )

    # 按优先级排序：成交额 > 价格 > 穿越数
    key_days.sort(
        key=lambda x: (
            x['amount'],
            x['price'],
            x['cross_count'],
        ),
        reverse=True
    )

    # 合并到最终关键位：关键位间至少 10% 价差，冲突时按“时间近->远、成交额大->小”取舍
    final_key_days: List[Dict[str, Any]] = []
    min_gap_pct = 0.10

    def is_price_conflict(a: float, b: float) -> bool:
        if a <= 0 or b <= 0:
            return False
        base = min(a, b)
        return abs(a - b) / base <= min_gap_pct

    for cand in key_days:
        cand_price = float(cand['price'])
        if cand_price <= 0:
            continue

        conflict_idx = [
            idx for idx, existing in enumerate(final_key_days)
            if is_price_conflict(cand_price, float(existing['price']))
        ]
        if not conflict_idx:
            final_key_days.append(cand)
            continue

        # 有冲突：在“候选 + 冲突已有项”中选择最优（时间近优先，再看成交额）
        pool = [cand] + [final_key_days[i] for i in conflict_idx]
        winner = max(pool, key=lambda x: (x['idx'], x['amount']))

        # 移除所有冲突项，再放入胜出者
        for i in sorted(conflict_idx, reverse=True):
            final_key_days.pop(i)
        final_key_days.append(winner)

    # 最终输出继续按原优先级展示，便于前端直接使用
    final_key_days.sort(
        key=lambda x: (
            x['amount'],
            x['price'],
            x['cross_count'],
        ),
        reverse=True
    )
    levels: List[float] = [float(d['price']) for d in final_key_days if float(d['price']) > 0]

    return {
        'code': code,
        'date': selected_date.strftime('%Y-%m-%d'),
        'window_days': window_days,
        'method_ver': method_ver,
        'levels': levels,
        'ath': float(ath) if ath == ath else None,
        'current': float(current_price) if current_price == current_price else None,
    }
