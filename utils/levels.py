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
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Optional

import polars as pl


DEFAULT_CACHE_PATH = os.path.join('data_cache', 'other', 'key_levels.parquet')


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def read_levels_cache(cache_path: str = DEFAULT_CACHE_PATH) -> pl.DataFrame:
    """读取Parquet缓存文件，不存在则返回空DataFrame"""
    try:
        if not os.path.exists(cache_path):
            return pl.DataFrame({
                'code': pl.Series([], dtype=pl.Utf8),
                'date': pl.Series([], dtype=pl.Utf8),
                'window_days': pl.Series([], dtype=pl.Int32),
                'method_ver': pl.Series([], dtype=pl.Utf8),
                'levels': pl.Series([], dtype=pl.Utf8),
                'ath': pl.Series([], dtype=pl.Float64),
                'current': pl.Series([], dtype=pl.Float64),
                'updated_at': pl.Series([], dtype=pl.Utf8),
            })
        return pl.read_parquet(cache_path)
    except Exception:
        # 若损坏则返回空
        return pl.DataFrame({
            'code': pl.Series([], dtype=pl.Utf8),
            'date': pl.Series([], dtype=pl.Utf8),
            'window_days': pl.Series([], dtype=pl.Int32),
            'method_ver': pl.Series([], dtype=pl.Utf8),
            'levels': pl.Series([], dtype=pl.Utf8),
            'ath': pl.Series([], dtype=pl.Float64),
            'current': pl.Series([], dtype=pl.Float64),
            'updated_at': pl.Series([], dtype=pl.Utf8),
        })


def write_levels_cache(record: Dict[str, Any], cache_path: str = DEFAULT_CACHE_PATH) -> None:
    """将一条记录追加或更新写入Parquet缓存。
    record 必须包含: code, date(YYYY-MM-DD), window_days, method_ver, levels(JSON字符串), ath, current, updated_at
    同一 (code, date, window_days, method_ver) 视为主键，若存在则覆盖。
    """
    _ensure_parent_dir(cache_path)
    df = read_levels_cache(cache_path)

    rec_df = pl.DataFrame([record])

    if df.is_empty():
        rec_df.write_parquet(cache_path)
        return

    key_cols = ['code', 'date', 'window_days', 'method_ver']
    # 去除旧记录
    filtered = df.filter(~(
        (pl.col('code') == record['code']) &
        (pl.col('date') == record['date']) &
        (pl.col('window_days') == record['window_days']) &
        (pl.col('method_ver') == record['method_ver'])
    ))
    # 追加新记录
    out_df = pl.concat([filtered, rec_df], how='vertical_relaxed')
    out_df.write_parquet(cache_path)


def compute_key_levels_from_market_states(
    market_states: pl.DataFrame,
    code: str,
    selected_date: date,
    window_days: int = 3650,
    method_ver: str = 'v1'
) -> Dict[str, Any]:
    """轻量级关键位（全区间）：近十年全价区间内的潜在支撑/压力
    - 使用收盘价与成交额(amount/volume)做价格-成交额直方图，识别高成交额节点(HVN)
    - 叠加简单摆动峰谷（局部高点与局部低点）
    - 合并去重输出若干水平
    返回 dict: { code, date, window_days, method_ver, levels(list[float]), ath, current }
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
    amounts = pd_df.get('成交额', pd_df.get('成交量')).astype(float).tolist()

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
    atl = float(min([l for l in lows if l == l], default=float('nan')))

    price_min = atl
    price_max = ath
    price_range = price_max - price_min
    if price_range <= 0:
        return {
            'code': code,
            'date': selected_date.strftime('%Y-%m-%d'),
            'window_days': window_days,
            'method_ver': method_ver,
            'levels': [],
            'ath': float(ath) if ath == ath else None,
            'current': float(current_price) if current_price == current_price else None,
        }

    # 1) 价格-成交额直方图（收盘价分箱）
    num_bins = min(60, max(20, len(closes) // 15))
    bin_size = price_range / max(num_bins, 1)
    bins = [0.0 for _ in range(max(num_bins, 1))]

    for c, w in zip(closes, amounts):
        if c >= price_min and c <= price_max:
            idx = int((c - price_min) // bin_size) if bin_size > 0 else 0
            if idx >= len(bins):
                idx = len(bins) - 1
            if idx < 0:
                idx = 0
            bins[idx] += float(w) if w == w else 0.0

    mean = sum(bins) / (len(bins) or 1)
    var = sum((v - mean) ** 2 for v in bins) / (len(bins) or 1)
    sd = var ** 0.5
    hvn_candidates = []
    for i in range(1, len(bins) - 1):
        v = bins[i]
        if v > bins[i-1] and v >= bins[i+1]:
            z = (v - mean) / sd if sd > 0 else 0.0
            if z > 1.0:
                price_at_bin = price_min + (i + 0.5) * bin_size
                hvn_candidates.append((price_at_bin, v))
    hvn_candidates.sort(key=lambda x: x[1], reverse=True)

    # 2) 摆动峰谷（5点局部峰/谷）
    swing_high_candidates = []
    for i in range(2, len(highs) - 2):
        h = float(highs[i]) if highs[i] == highs[i] else None
        if h is None:
            continue
        if h >= price_min and h <= price_max and (
            h > highs[i-1] and h >= highs[i+1] and h > highs[i-2] and h >= highs[i+2]
        ):
            swing_high_candidates.append((h, h))

    swing_low_candidates = []
    for i in range(2, len(lows) - 2):
        lo = float(lows[i]) if lows[i] == lows[i] else None
        if lo is None:
            continue
        if lo >= price_min and lo <= price_max and (
            lo < lows[i-1] and lo <= lows[i+1] and lo < lows[i-2] and lo <= lows[i+2]
        ):
            swing_low_candidates.append((lo, lo))

    # 3) 合并去重
    min_gap = max(price_range * 0.02, bin_size * 0.8)
    merged: List[float] = []

    def push_if_far(p: float) -> bool:
        for m in merged:
            if abs(m - p) < min_gap:
                return False
        merged.append(p)
        return True

    for p, s in hvn_candidates:
        if len(merged) >= 8:
            break
        push_if_far(float(p))

    if len(merged) < 8:
        for p, s in swing_high_candidates:
            if len(merged) >= 8:
                break
            push_if_far(float(p))

    if len(merged) < 8:
        for p, s in swing_low_candidates:
            if len(merged) >= 8:
                break
            push_if_far(float(p))

    merged.sort()

    return {
        'code': code,
        'date': selected_date.strftime('%Y-%m-%d'),
        'window_days': window_days,
        'method_ver': method_ver,
        'levels': merged,
        'ath': float(ath),
        'current': float(current_price),
    }


