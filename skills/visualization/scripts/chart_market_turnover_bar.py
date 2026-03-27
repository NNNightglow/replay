#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Dict, List


def plot_market_turnover_bar(
    rows: List[Dict[str, Any]],
    date_col: str = "日期",
    value_col: str = "成交总额",
    title: str = "两日成交总额对比",
    unit: str = "亿元",
) -> str:
    if not rows:
        return "<div style='padding:16px;color:#666;'>无可用数据。</div>"

    x_data: List[str] = []
    y_data: List[float] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        x_val = str(row.get(date_col) or "").strip()
        y_raw = row.get(value_col)
        if not x_val or y_raw is None:
            continue
        try:
            y_val = float(y_raw)
        except Exception:
            continue
        x_data.append(x_val)
        y_data.append(round(y_val, 3))

    if not x_data:
        return "<div style='padding:16px;color:#666;'>输入数据缺少可绘制值。</div>"

    try:
        from pyecharts import options as opts
        from pyecharts.charts import Bar

        bar = Bar()
        bar.add_xaxis(x_data)
        bar.add_yaxis(f"成交总额({unit})", y_data)
        bar.set_global_opts(
            title_opts=opts.TitleOpts(title=title),
            xaxis_opts=opts.AxisOpts(type_="category"),
            yaxis_opts=opts.AxisOpts(type_="value"),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
        )
        return bar.render_embed()
    except Exception as exc:
        return f"<div style='padding:16px;color:#c00;'>绘图失败: {exc}</div>"


CHART_SPECS = [
    {
        "id": "market_turnover_bar",
        "func": "plot_market_turnover_bar",
        "description": "根据查询结果绘制成交总额柱状图（输入 rows）。",
        "output_type": "embedded_html",
    }
]
