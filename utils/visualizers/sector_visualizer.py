#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
板块可视化模块
专门处理行业板块和概念板块相关的图表生成

作者: AI助手
日期: 2025-01-24
"""

import polars as pl
from pyecharts.charts import Kline, Line, Bar, Pie
from pyecharts import options as opts
from pyecharts.globals import ThemeType
from pyecharts.commons.utils import JsCode
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import warnings

from .common import ChartConfig, ChartUtils, ChartFormatters, UniversalKlineChart

# 屏蔽pandas警告
warnings.filterwarnings('ignore')

class SectorVisualizer:
    """板块可视化器，处理所有板块相关的图表"""
    
    @staticmethod
    def plot_sector_kline(data_fetcher, date_str: str, days_range: int = 30, index_name: str = None) -> str:
        """
        绘制行业K线图 - 平均五日成交量前十的行业
        每个行业都绘制成独立的K线图，使用涨跌幅作为坐标轴
        """
        try:
            print(f"🔧 开始生成行业K线图: date={date_str}, days={days_range}")

            # 从板块数据管理器获取数据
            sector_data = data_fetcher.sector_data_manager.load_sector_data(days_back=days_range)
            if sector_data.is_empty():
                return "<div>无法获取行业数据</div>"

            print(f"📊 获取到行业数据: {sector_data.height} 条记录")

            # 检查数据来源并适配列名
            if '板块名称' not in sector_data.columns:
                return "<div>数据格式错误：缺少板块名称列</div>"

            # 确保有成交额列
            if '成交额' not in sector_data.columns:
                if '总市值' in sector_data.columns:
                    sector_data = sector_data.with_columns([
                        pl.col('总市值').alias('成交额')
                    ])
                else:
                    return "<div>数据缺少成交额信息</div>"

            # 过滤有效数据
            filtered_data = sector_data.filter(
                (pl.col('成交额').is_not_null()) & 
                (pl.col('成交额') > 0) &
                (pl.col('板块名称').is_not_null())
            )

            if filtered_data.is_empty():
                return "<div>没有有效的行业数据</div>"

            print(f"📊 过滤后数据: {filtered_data.height} 条记录")

            # 计算每个行业的平均五日成交量，选择前十
            top_sector_names = SectorVisualizer.get_top_volume_sectors(filtered_data, 10, 5)
            if not top_sector_names:
                return "<div>无法计算前十大成交量行业</div>"

            print(f"📊 选择前十大平均五日成交量行业: {top_sector_names}")

            # 准备日期数据
            dates = sorted(filtered_data['日期'].unique().to_list())
            date_strs = [str(date) for date in dates]

            print(f"📊 日期范围: {len(dates)} 天，从 {min(dates)} 到 {max(dates)}")

            # 创建主K线图容器
            kline = Kline(init_opts=ChartConfig.get_common_init_opts(height="700px"))
            kline.add_xaxis(date_strs)

            # 为每个行业生成K线数据
            for i, sector_name in enumerate(top_sector_names):
                sector_data_subset = filtered_data.filter(pl.col('板块名称') == sector_name).sort('日期')
                
                if not sector_data_subset.is_empty():
                    print(f"📊 处理行业 {i+1}/{len(top_sector_names)}: {sector_name}")
                    
                    # 检查必要的列
                    required_cols = ['涨跌幅']
                    missing_cols = [col for col in required_cols if col not in sector_data_subset.columns]
                    
                    if not missing_cols:
                        # 准备K线数据 - 使用累计涨跌幅作为坐标轴，以0%为起点
                        kline_data = []
                        cumulative_change = 0.0  # 以0%为起点

                        for date in dates:
                            day_data = sector_data_subset.filter(pl.col('日期') == date)
                            if not day_data.is_empty():
                                row = day_data.row(0, named=True)
                                # 获取涨跌幅数据
                                change_pct = float(row.get('涨跌幅', 0))

                                # 计算振幅（如果有的话）
                                amplitude = float(row.get('振幅', abs(change_pct)))

                                # 构造K线数据：累计涨跌幅（以0%为起点）
                                open_val = cumulative_change
                                close_val = cumulative_change + change_pct  # 累计涨跌幅

                                # 计算最高最低价（基于振幅）
                                if change_pct >= 0:  # 上涨
                                    high_val = close_val + amplitude / 2
                                    low_val = open_val - amplitude / 2
                                else:  # 下跌
                                    high_val = open_val + amplitude / 2
                                    low_val = close_val - amplitude / 2

                                kline_data.append([open_val, close_val, low_val, high_val])
                                cumulative_change = close_val  # 下一天的基准是今天的收盘价
                            else:
                                # 如果某天没有数据，保持前一天的值
                                kline_data.append([cumulative_change, cumulative_change, cumulative_change, cumulative_change])
                        
                        # 获取该行业的颜色配置
                        color_config = ChartConfig.get_kline_color_config(i)
                        
                        # 添加K线系列
                        kline.add_yaxis(
                            sector_name,
                            kline_data,
                            itemstyle_opts=opts.ItemStyleOpts(
                                color=color_config["up"],        # 阳线颜色（空心）
                                color0=color_config["down"],     # 阴线颜色（实心）
                                border_color=color_config["up_border"],    # 阳线边框
                                border_color0=color_config["down_border"]  # 阴线边框
                            )
                        )
                        print(f"✅ {sector_name} K线数据添加成功，共 {len(kline_data)} 个数据点")
                    else:
                        print(f"❌ {sector_name} 缺少必要列: {missing_cols}")
                else:
                    print(f"⚠️ {sector_name} 无有效数据")

            # 设置全局配置
            kline.set_global_opts(
                title_opts=ChartConfig.get_common_title_opts("板块K线图 - 平均五日成交量前十"),
                xaxis_opts=opts.AxisOpts(type_="category"),
                yaxis_opts=opts.AxisOpts(
                    name="涨跌幅(%)",
                    position="left",
                    axislabel_opts=opts.LabelOpts(formatter="{value}%"),
                    splitline_opts=opts.SplitLineOpts(is_show=True),
                    axisline_opts=opts.AxisLineOpts(
                        linestyle_opts=opts.LineStyleOpts(color="#666")
                    )
                ),
                tooltip_opts=opts.TooltipOpts(
                    trigger="axis",
                    axis_pointer_type="cross",
                    formatter="{b}<br/>{a}: {c}%"
                ),
                legend_opts=ChartConfig.get_common_legend_opts("top"),
                datazoom_opts=ChartConfig.get_common_datazoom_opts(),
                toolbox_opts=ChartConfig.get_common_toolbox_opts()
            )

            # 如果指定了指数，可以在这里添加指数叠加功能
            if index_name:
                print(f"⚠️ 指数叠加功能暂未实现: {index_name}")

            return kline.render_embed()

        except Exception as e:
            print(f"❌ 生成行业K线图失败: {e}")
            import traceback
            traceback.print_exc()
            return f"<div>生成行业K线图失败: {str(e)}</div>"
    
    @staticmethod
    def get_top_volume_sectors(data: pl.DataFrame, top_n: int = 10, days: int = 5) -> List[str]:
        """获取平均成交量前N的行业"""
        try:
            # 获取最近N个交易日的数据
            recent_dates = sorted(data['日期'].unique().to_list())[-days:]
            
            if len(recent_dates) < days:
                print(f"⚠️ 交易日数据不足，只有 {len(recent_dates)} 天")
            
            # 筛选最近N日数据
            min_date = min(recent_dates)
            max_date = max(recent_dates)
            recent_data = data.filter(
                (data['日期'] >= min_date) & (data['日期'] <= max_date)
            )
            
            # 按行业分组计算平均成交额
            sector_avg_volume = recent_data.group_by('板块名称').agg([
                pl.col('成交额').mean().alias('平均成交量')
            ]).sort('平均成交量', descending=True)
            
            # 返回前N个行业名称
            return sector_avg_volume.head(top_n)['板块名称'].to_list()
            
        except Exception as e:
            print(f"❌ 获取前{top_n}大成交量行业失败: {e}")
            return []
    
    @staticmethod
    def plot_sector_ranking(sector_data: List[Dict[str, Any]], 
                           title: str = "板块排行榜",
                           value_col: str = "涨跌幅",
                           name_col: str = "板块名称",
                           top_n: int = 20) -> str:
        """绘制板块排行榜"""
        try:
            if not sector_data:
                return "<div>没有板块数据</div>"
            
            # 准备数据
            sectors = []
            values = []
            
            for item in sector_data[:top_n]:
                sectors.append(str(item.get(name_col, '')))
                values.append(float(item.get(value_col, 0)))
            
            # 创建柱状图
            bar = Bar(init_opts=ChartConfig.get_common_init_opts())
            bar.add_xaxis(sectors)
            bar.add_yaxis(
                value_col,
                values,
                itemstyle_opts=opts.ItemStyleOpts(
                    color=ChartFormatters.get_change_color_formatter()
                )
            )
            
            bar.set_global_opts(
                title_opts=ChartConfig.get_common_title_opts(title),
                xaxis_opts=opts.AxisOpts(
                    axislabel_opts=opts.LabelOpts(rotate=-45)
                ),
                yaxis_opts=opts.AxisOpts(
                    name=value_col,
                    axislabel_opts=opts.LabelOpts(formatter="{value}%")
                ),
                tooltip_opts=ChartConfig.get_common_tooltip_opts(),
                datazoom_opts=ChartConfig.get_common_datazoom_opts(),
                toolbox_opts=ChartConfig.get_common_toolbox_opts()
            )
            
            return bar.render_embed()
            
        except Exception as e:
            print(f"❌ 生成板块排行榜失败: {e}")
            return f"<div>生成板块排行榜失败: {str(e)}</div>"
    
    @staticmethod
    def plot_sector_distribution(sector_data: List[Dict[str, Any]]) -> str:
        """绘制板块涨跌分布饼图"""
        try:
            if not sector_data:
                return "<div>没有板块数据</div>"
            
            # 统计涨跌分布
            up_count = sum(1 for item in sector_data if float(item.get('涨跌幅', 0)) > 0)
            down_count = sum(1 for item in sector_data if float(item.get('涨跌幅', 0)) < 0)
            flat_count = len(sector_data) - up_count - down_count
            
            # 创建饼图
            pie = Pie(init_opts=ChartConfig.get_common_init_opts(height="400px"))
            pie.add(
                "",
                [
                    ["上涨", up_count],
                    ["下跌", down_count],
                    ["平盘", flat_count]
                ],
                radius=["40%", "75%"],
                center=["50%", "50%"]
            )
            
            pie.set_colors([ChartConfig.COLORS['red'], ChartConfig.COLORS['green'], ChartConfig.COLORS['gray']])
            pie.set_global_opts(
                title_opts=ChartConfig.get_common_title_opts("板块涨跌分布"),
                legend_opts=ChartConfig.get_common_legend_opts("right"),
                tooltip_opts=opts.TooltipOpts(
                    formatter="{a} <br/>{b}: {c} ({d}%)"
                )
            )
            
            return pie.render_embed()
            
        except Exception as e:
            print(f"❌ 生成板块分布图失败: {e}")
            return f"<div>生成板块分布图失败: {str(e)}</div>"

    @staticmethod
    def plot_single_sector_kline(data_fetcher, sector_name: str, overlay_index: str = None, days_range: int = 30) -> str:
        """
        绘制单个板块的K线图（使用通用K线图函数）

        Args:
            data_fetcher: 数据获取器
            sector_name: 板块名称
            overlay_index: 叠加指数名称（已移除，不再支持）
            days_range: 天数范围

        Returns:
            str: 图表HTML字符串
        """
        try:
            print(f"🔧 开始生成板块K线图: {sector_name}, 天数: {days_range}")

            # 从板块数据管理器获取数据
            sector_data = data_fetcher.sector_data_manager.load_sector_data()
            if sector_data.is_empty():
                return f"<div>无法获取板块数据</div>"

            # 过滤指定板块的数据
            sector_kline_data = sector_data.filter(pl.col('板块名称') == sector_name)
            if sector_kline_data.is_empty():
                # 尝试模糊匹配
                sector_kline_data = sector_data.filter(pl.col('板块名称').str.contains(sector_name))
                if sector_kline_data.is_empty():
                    available_sectors = sector_data.select('板块名称').unique().to_series().to_list()[:10]
                    return f"<div>未找到板块 '{sector_name}' 的数据<br>可用板块示例: {', '.join(available_sectors)}</div>"

            # 按日期排序并取最近的数据
            sector_kline_data = sector_kline_data.sort('日期', descending=True).head(days_range)

            print(f"📊 获取到板块 {sector_name} 数据: {sector_kline_data.height} 条记录")

            # 使用通用K线图函数绘制
            return UniversalKlineChart.plot_kline_with_volume(
                sector_kline_data,
                title=f"{sector_name}板块K线图",
                height="400px",  # 板块K线图使用较小高度
                amount_column='成交额'  # 优先使用成交额
            )

        except Exception as e:
            print(f"❌ 生成板块K线图失败: {e}")
            import traceback
            traceback.print_exc()
            return f"<div>生成板块K线图失败: {str(e)}</div>"
