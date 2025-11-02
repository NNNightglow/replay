#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
市场可视化模块
包含市场情绪、连板分析、市场元数据等综合性市场分析图表

作者: AI助手
日期: 2025-01-24
"""

import polars as pl
from pyecharts.charts import Bar, Line, Grid, Gauge, Scatter, Pie
from pyecharts import options as opts
from pyecharts.commons.utils import JsCode
from typing import List, Dict, Any, Optional
import warnings

from .common import ChartConfig, ChartUtils, ChartFormatters

# 屏蔽pandas警告
warnings.filterwarnings('ignore')

class MarketVisualizer:
    """市场可视化器，处理综合性市场分析图表"""
    
    @staticmethod
    def plot_market_sentiment(sentiment_data: dict) -> Dict[str, str]:
        """绘制市场情绪图表，使用pyecharts仪表盘和柱状图"""
        figures = {}
        
        # 1. 市场关键指标仪表盘
        gauge = Gauge()
        gauge.add(
            "红盘率",
            [("红盘率", sentiment_data['red_ratio'])],
            min_=0,
            max_=100,
            split_number=10,
            radius="75%"
        )
        gauge.set_global_opts(
            title_opts=opts.TitleOpts(title="市场红盘率"),
            tooltip_opts=ChartConfig.get_common_tooltip_opts()
        )
        figures['red_ratio_gauge'] = gauge.render_embed()
        
        # 2. 市场情绪柱状图
        bar = Bar()
        categories = ['涨停', '跌停', '上涨', '下跌', '平盘', '强势股', '昨日涨停', '炸板', '大单']
        values = [
                sentiment_data['limit_up_count'],
                sentiment_data['limit_down_count'],
                sentiment_data['up_count'],
                sentiment_data['down_count'],
                sentiment_data['flat_count'],
                sentiment_data['strong_stocks_count'],
                sentiment_data['previous_limit_up_count'],
                sentiment_data['break_limit_up_count'],
                sentiment_data['big_deal_count']
            ]
        
        bar.add_xaxis(categories)
        bar.add_yaxis("数量", values)
        bar.set_global_opts(
            title_opts=opts.TitleOpts(title="市场情绪指标"),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=-45)),
            datazoom_opts=ChartConfig.get_common_datazoom_opts(),
            tooltip_opts=ChartConfig.get_common_tooltip_opts(),
            toolbox_opts=ChartConfig.get_common_toolbox_opts()
        )
        figures['sentiment_bar'] = bar.render_embed()
        
        return figures
    
        
    @staticmethod
    def plot_market_metadata(market_states: pl.DataFrame, market_metadata: pl.DataFrame) -> Dict[str, str]:
        """绘制市场元数据图表，统一使用pyecharts"""
        print("🔧 DEBUG: plot_market_metadata 函数被调用")

        if market_metadata is None or market_metadata.is_empty():
            return {
                'red_ratio': "<div>无红盘率数据</div>",
                'market_amount': "<div>无市场量能数据</div>"
            }

        print("🔧 DEBUG: 开始生成各个图表...")

        result = {
            'red_ratio_and_amount': MarketVisualizer.plot_market_red_ratio_and_amount(market_metadata, width="100%"),
            'limit_up_count': MarketVisualizer.plot_limit_counts(market_metadata),
            'ground_ceiling_count': MarketVisualizer.plot_break_counts(market_states),
            'continuous_limit_up': MarketVisualizer.plot_continuous_limit_ladder_combined(market_states)
        }

        print(f"🔧 DEBUG: plot_market_metadata 返回图表键名: {list(result.keys())}")
        return result
    
    @staticmethod
    def plot_market_red_ratio_and_amount(market_metadata: pl.DataFrame, height: str = "600px", width: str = "100%") -> str:
        """绘制红盘率和市场量能的组合图表"""
        data = ChartUtils.prepare_chart_data(market_metadata)
        if not data:
            return "<div style='text-align:center; padding:50px; color:#666;'>📊 无市场数据</div>"

        # 准备共同的日期数据，确保格式一致 
        dates = [item['日期'].strftime('%Y-%m-%d') if hasattr(item['日期'], 'strftime') else str(item['日期']) for item in data]
        red_ratios = [round(item.get('红盘率', 0), 2) for item in data]
        amounts = [round(item.get('成交总额', 0) / 10000, 2) for item in data]

        # 计算局部极值点的函数
        def find_local_extrema(values, window=3):
            """找到局部极值点"""
            local_max = []
            local_min = []

            for i in range(window, len(values) - window):
                # 检查局部最大值
                is_local_max = all(values[i] >= values[j] for j in range(i-window, i+window+1))
                if is_local_max and values[i] > values[i-1] and values[i] > values[i+1]:
                    local_max.append({"coord": [dates[i], values[i]], "value": values[i]})

                # 检查局部最小值
                is_local_min = all(values[i] <= values[j] for j in range(i-window, i+window+1))
                if is_local_min and values[i] < values[i-1] and values[i] < values[i+1]:
                    local_min.append({"coord": [dates[i], values[i]], "value": values[i]})

            return local_max, local_min

        # 找到市场量能的局部极值
        amount_max, amount_min = find_local_extrema(amounts)

        # 创建红盘率线图（上半部分）
        line = Line(init_opts=opts.InitOpts(theme='light'))
        line.add_xaxis(dates)
        line.add_yaxis(
            "红盘率", 
            red_ratios,
            symbol="circle",
            symbol_size=6,
            is_smooth=True,
            linestyle_opts=opts.LineStyleOpts(width=3, color="#FF6B6B"),
            itemstyle_opts=opts.ItemStyleOpts(color="#FF6B6B"),
            areastyle_opts=opts.AreaStyleOpts(opacity=0.2, color="#FF6B6B"),
        )

        # 创建市场量能折线图（下半部分）
        amount_line = Line(init_opts=opts.InitOpts(theme='light'))
        amount_line.add_xaxis(dates)
        amount_line.add_yaxis(
            "市场量能",
            amounts,
            is_smooth=True,  # 平滑曲线
            symbol="circle",  # 数据点符号
            symbol_size=6,    # 数据点大小
            linestyle_opts=opts.LineStyleOpts(
                color="#4ECDC4",
                width=3
            ),
            itemstyle_opts=opts.ItemStyleOpts(color="#4ECDC4"),
            areastyle_opts=opts.AreaStyleOpts(
                color={
                    "type": "linear",
                    "x": 0, "y": 0, "x2": 0, "y2": 1,
                    "colorStops": [
                        {"offset": 0, "color": "rgba(78, 205, 196, 0.3)"},
                        {"offset": 1, "color": "rgba(78, 205, 196, 0.05)"}
                    ]
                }
            ),
            markpoint_opts=opts.MarkPointOpts(
                data=[
                    # 局部极大值点 - 红色
                    *[opts.MarkPointItem(
                        coord=point["coord"],
                        itemstyle_opts=opts.ItemStyleOpts(color="#FF6B6B"),  # 红色极大值
                        symbol_size=30
                    ) for point in amount_max],
                    # 局部极小值点 - 绿色
                    *[opts.MarkPointItem(
                        coord=point["coord"],
                        itemstyle_opts=opts.ItemStyleOpts(color="#20B2AA"),  # 绿色极小值
                        symbol_size=30
                    ) for point in amount_min]
                ],
                symbol="diamond",
                label_opts=opts.LabelOpts(is_show=False)  # 不显示标签文字
            )
        )

        # 设置红盘率图表配置
        line.set_global_opts(
            xaxis_opts=opts.AxisOpts(
                axislabel_opts=opts.LabelOpts(is_show=False)  # 隐藏上图X轴标签
            ),
            yaxis_opts=opts.AxisOpts(
                name="红盘率 (%)",
                name_location="middle",
                name_gap=40,
                axislabel_opts=opts.LabelOpts(formatter="{value}%")
            ),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
            datazoom_opts=[
                opts.DataZoomOpts(type_="inside", xaxis_index=[0, 1]),
                opts.DataZoomOpts(type_="slider", xaxis_index=[0, 1])
            ],
            legend_opts=opts.LegendOpts(
                pos_right="5%",
                pos_top="10%",
                orient="vertical"
            )
        )

        # 设置市场量能图表配置
        amount_line.set_global_opts(
            xaxis_opts=opts.AxisOpts(
                name="日期",
                axislabel_opts=opts.LabelOpts(rotate=45)
            ),
            yaxis_opts=opts.AxisOpts(
                name="市场量能 (万亿)",
                name_location="middle",
                name_gap=40,
                axislabel_opts=opts.LabelOpts(formatter="{value}万亿")
            ),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
            legend_opts=opts.LegendOpts(
                pos_right="5%",
                pos_top="60%",
                orient="vertical"
            )
        )

        # 使用Grid进行布局组合
        grid = Grid(init_opts=opts.InitOpts(width=width, height=height, theme='light'))
        
        # 红盘率图占据上方60%空间，右侧留出空间给图例
        grid.add(
            line,
            grid_opts=opts.GridOpts(pos_left="10%", pos_right="15%", pos_top="7%", pos_bottom="50%")
        )
        
        # 市场量能图占据下方35%空间，右侧留出空间给图例
        grid.add(
            amount_line,
            grid_opts=opts.GridOpts(pos_left="10%", pos_right="15%", pos_top="60%", pos_bottom="7%")
        )

        return grid.render_embed()

    @staticmethod
    def plot_limit_counts(market_metadata: pl.DataFrame, height: str = "600px") -> str:
        """绘制涨跌停数量统计"""
        data = ChartUtils.prepare_chart_data(market_metadata)
        if not data:
            return "<div>无涨跌停数据</div>"

        line = Line(init_opts=opts.InitOpts(width="100%", height=height, theme="light"))
        dates = [item['日期'].strftime('%Y-%m-%d') if hasattr(item['日期'], 'strftime') else str(item['日期']) for item in data]
        limit_up = [item.get('涨停数', 0) for item in data]
        limit_down = [item.get('跌停数', 0) for item in data]
        
        line.add_xaxis(dates)
        line.add_yaxis(
            "涨停数", 
            limit_up, 
            symbol="circle", 
            symbol_size=8, 
            linestyle_opts=opts.LineStyleOpts(width=2, color="#ef232a"),  # 红色
            itemstyle_opts=opts.ItemStyleOpts(color="#ef232a"),  # 红色
            is_symbol_show=True
        )
        line.add_yaxis(
            "跌停数", 
            limit_down, 
            symbol="triangle", 
            symbol_size=8, 
            linestyle_opts=opts.LineStyleOpts(width=2),  # 保持默认颜色
            is_symbol_show=True
        )
        
        line.set_global_opts(
            xaxis_opts=opts.AxisOpts(name="日期", axislabel_opts=opts.LabelOpts(rotate=45)),
            yaxis_opts=opts.AxisOpts(name="数量"),
            datazoom_opts=ChartConfig.get_common_datazoom_opts(),
            tooltip_opts=ChartConfig.get_common_tooltip_opts(),
            toolbox_opts=ChartConfig.get_common_toolbox_opts(),
            legend_opts=opts.LegendOpts(pos_top="5%")
        )
        
        # 设置标记点，显示最大值和最小值
        line.set_series_opts(
            label_opts=opts.LabelOpts(is_show=False),
            markpoint_opts=opts.MarkPointOpts(
                data=[
                    opts.MarkPointItem(type_="max", name="最大值"),
                    opts.MarkPointItem(type_="min", name="最小值")
                ]
            )
        )
        
        return line.render_embed()
 
    @staticmethod
    def plot_break_counts(market_states: pl.DataFrame) -> str:
        """绘制地天板、天地板和炸板的堆叠柱状图"""
        
        # 识别地天板和天地板股票 [T0](1)
        extreme_data = (
            market_states
            .with_columns([
                # 地天板判断：最低价等于跌停价 且 收盘价等于涨停价
                (
                    (pl.col('最低') == pl.col('跌停价')) & 
                    (pl.col('收盘') == pl.col('涨停价'))
                ).alias('is_地天板'),
                
                # 天地板判断：最高价等于涨停价 且 收盘价等于跌停价
                (
                    (pl.col('最高') == pl.col('涨停价')) & 
                    (pl.col('收盘') == pl.col('跌停价'))
                ).alias('is_天地板')
            ])
            .filter(
                (pl.col('is_地天板') == True) | (pl.col('is_天地板') == True)
            )
        )
        
        # 识别炸板股票 [T1](2)
        break_data = (
            market_states
            .filter(pl.col('炸板') == True)
        )
        
        # 如果没有数据，返回提示信息 [T1](2)
        if extreme_data.is_empty() and break_data.is_empty():
            return "<div style='text-align:center; padding:50px; color:#666;'>📊 无地天板/天地板/炸板数据</div>"
        
        # 获取所有日期
        all_dates = sorted(market_states['日期'].unique().to_list())
        date_strings = [
            d.strftime('%m-%d') if hasattr(d, 'strftime') else str(d)
            for d in all_dates
        ]
        
        # 创建柱状图 [T4](3) - 调整为适合前端容器的尺寸
        bar = Bar(init_opts=opts.InitOpts(width="100%", height="350px", theme="light"))
        bar.add_xaxis(date_strings)
        
        # 添加地天板数据（最上层）
        ground_sky_items = []
        for d in all_dates:
            day_data = extreme_data.filter((pl.col('日期') == d) & (pl.col('is_地天板') == True) & (pl.col('名称') != ''))
            if not day_data.is_empty():
                stock_names = day_data['名称'].to_list()
                count = len(stock_names)
                # 使用股票名称作为名称，数量作为值
                ground_sky_items.append(
                    opts.BarItem(
                        name='、'.join(stock_names),
                        value=count
                    )
                )
            else:
                ground_sky_items.append(
                    opts.BarItem(name="", value=0)
                )
        
        bar.add_yaxis(
            "地天板",
            ground_sky_items,
            stack="all_boards",
            color="#FF4757",  # 设置系列颜色，确保图例颜色一致
            itemstyle_opts=opts.ItemStyleOpts(color="#FF4757"),  # 强制设置项目样式颜色
            label_opts=opts.LabelOpts(
                is_show=True,
                position="inside",
                formatter="{c}",
                font_size=10,
                font_weight="bold",
                color="white"
            )
        )
        
        # 添加天地板数据（中层）
        sky_ground_items = []
        for d in all_dates:
            day_data = extreme_data.filter(
                (pl.col('日期') == d) & (pl.col('is_天地板') == True) & (pl.col('名称') != '')
            )
            if not day_data.is_empty():
                stock_names = day_data['名称'].to_list()
                count = len(stock_names)
                sky_ground_items.append(
                    opts.BarItem(
                        name='、'.join(stock_names),
                        value=count
                    )
                )
            else:
                sky_ground_items.append(
                    opts.BarItem(name="", value=0)
                )
        
        bar.add_yaxis(
            "天地板",
            sky_ground_items,
            stack="all_boards",
            color="#26D0CE",  # 设置系列颜色，确保图例颜色一致
            itemstyle_opts=opts.ItemStyleOpts(color="#26D0CE"),  # 强制设置项目样式颜色
            label_opts=opts.LabelOpts(
                is_show=True,
                position="inside",
                formatter="{c}",
                font_size=10,
                font_weight="bold",
                color="white"
            )
        )
        
        # 添加炸板数据（底层）
        break_items = []
        for d in all_dates:
            day_data = break_data.filter((pl.col('日期') == d)& (pl.col('名称') != ''))
            if not day_data.is_empty():
                stock_names = day_data['名称'].to_list()
                count = len(stock_names)
                break_items.append(
                    opts.BarItem(
                        name='、'.join(stock_names),
                        value=count
                    )
                )
            else:
                break_items.append(
                    opts.BarItem(name="", value=0)
                )
        
        bar.add_yaxis(
            "炸板",
            break_items,
            stack="all_boards",
            color="#FFD700",  # 设置系列颜色，确保图例颜色一致
            itemstyle_opts=opts.ItemStyleOpts(color="#FFD700"),  # 强制设置项目样式颜色
            label_opts=opts.LabelOpts(
                is_show=True,
                position="inside",
                formatter="{c}",
                font_size=10,
                font_weight="bold",
                color="white"
            )
        )
        
        # 设置全局配置 [T4](3)
        bar.set_global_opts(
            xaxis_opts=opts.AxisOpts(
                name="日期",
                axislabel_opts=opts.LabelOpts(rotate=45, font_size=10)
            ),
            yaxis_opts=opts.AxisOpts(
                name="股票数量",
                name_location="middle",
                name_gap=40
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="shadow",
                formatter="{b0}"  # 简单的字符串格式，显示股票名称
            ),
            legend_opts=opts.LegendOpts(pos_top="8%"),
            datazoom_opts=[
                opts.DataZoomOpts(
                    type_="inside",
                    xaxis_index=[0],
                    range_start=0,
                    range_end=100
                ),
                opts.DataZoomOpts(
                    type_="slider",
                    xaxis_index=[0],
                    range_start=0,
                    range_end=100,
                    pos_bottom="5%"
                )
            ]
        )
        
        return bar.render_embed()

    @staticmethod
    def plot_change_distribution(change_distribution: dict) -> str:
        """绘制涨跌幅分布柱状图"""
        if not change_distribution or 'ranges' not in change_distribution:
            return "<div style='text-align:center; padding:50px; color:#666;'>📊 无涨跌幅分布数据</div>"

        ranges = change_distribution['ranges']
        if not ranges:
            return "<div style='text-align:center; padding:50px; color:#666;'>📊 无涨跌幅分布数据</div>"

        # 创建柱状图
        bar = Bar(init_opts=opts.InitOpts(width="100%", height="350px", theme="light"))

        # 准备数据
        x_data = []  # X轴标签（涨跌幅区间）
        y_data = []  # Y轴数据（股票数量）
        colors = [
            '#006400',  # 跌停 - 深绿
            '#2E8B57',  # 大跌 - 绿
            '#3CB371',  # 中跌 - 浅绿
            '#90EE90',  # 小跌 - 很浅绿
            '#98FB98',  # 微跌 - 极浅绿
            '#FFB6C1',  # 微涨 - 极浅红
            '#FA8072',  # 小涨 - 很浅红
            '#FF6347',  # 中涨 - 浅红
            '#B22222',  # 大涨 - 红
            '#8B0000'   # 涨停 - 深红
        ]

        # 构建柱状图数据
        bar_items = []
        for i, range_data in enumerate(ranges):
            x_data.append(range_data['label'])
            count = range_data['count']
            y_data.append(count)

            # 为每个柱子设置颜色
            bar_items.append(
                opts.BarItem(
                    name=range_data['label'],
                    value=count,
                    itemstyle_opts=opts.ItemStyleOpts(
                        color=colors[i % len(colors)]
                    )
                )
            )

        bar.add_xaxis(x_data)
        bar.add_yaxis(
            series_name="股票数量",
            y_axis=bar_items,
            label_opts=opts.LabelOpts(
                is_show=True,
                position="top",
                formatter="{c}只"
            )
        )

        bar.set_global_opts(
            title_opts=opts.TitleOpts(
                pos_left="center",
                title_textstyle_opts=opts.TextStyleOpts(
                    font_size=16,
                    font_weight="bold"
                )
            ),
            xaxis_opts=opts.AxisOpts(
                name="涨跌幅区间",
                name_location="middle",
                name_gap=30,
                axislabel_opts=opts.LabelOpts(
                    rotate=45,  # 旋转45度避免标签重叠
                    font_size=10
                )
            ),
            yaxis_opts=opts.AxisOpts(
                name="股票数量(只)",
                name_location="middle",
                name_gap=40
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                formatter="{b}<br/>{a}: {c}只"
            ),
            legend_opts=opts.LegendOpts(
                is_show=False  # 隐藏图例，因为颜色已经能区分涨跌
            )
        )

        return bar.render_embed()


    # ========== 连板分析功能 ==========
    @staticmethod
    def plot_continuous_limit_ladder_line(market_states: pl.DataFrame) -> str:
        """连板天梯折线图函数，在最高点和次高点显示股票名称"""
        if market_states is None or market_states.is_empty():
            return "<div style='text-align:center; padding:50px; color:#666;'>📊 无连板分布数据</div>"

        try:
            # 检查是否有连板数据
            if '连板数' not in market_states.columns:
                return "<div style='text-align:center; padding:50px; color:#666;'>� 数据中缺少连板信息</div>"

            # 获取多日期的连板数据，并按日期排序
            dates = market_states['日期'].unique().sort().to_list()

            if not dates:
                return "<div style='text-align:center; padding:50px; color:#666;'>📊 无连板日期数据</div>"

            # 计算每日最高板数、次高板数和对应股票名称
            max_boards_per_day = []
            second_boards_per_day = []
            max_stocks_per_day = []
            second_stocks_per_day = []

            for date in dates:
                daily_data = market_states.filter(pl.col('日期') == date)
                if daily_data.is_empty():
                    max_boards_per_day.append(0)
                    second_boards_per_day.append(0)
                    max_stocks_per_day.append('')
                    second_stocks_per_day.append('')
                    continue

                # 获取当日所有连板数，按降序排列
                board_counts = daily_data['连板数'].unique().sort(descending=True).to_list()
                board_counts = [b for b in board_counts if b > 0]  # 过滤掉0

                if not board_counts:
                    max_boards_per_day.append(0)
                    second_boards_per_day.append(0)
                    max_stocks_per_day.append('')
                    second_stocks_per_day.append('')
                    continue

                # 最高板
                max_board = board_counts[0]
                max_boards_per_day.append(max_board)
                max_stocks = daily_data.filter(pl.col('连板数') == max_board)['名称'].to_list()[:2]
                max_stocks_per_day.append('\n'.join(max_stocks) if max_stocks else '')

                # 次高板
                if len(board_counts) > 1:
                    second_board = board_counts[1]
                    second_boards_per_day.append(second_board)
                    second_stocks = daily_data.filter(pl.col('连板数') == second_board)['名称'].to_list()[:2]
                    second_stocks_per_day.append('\n'.join(second_stocks) if second_stocks else '')
                else:
                    second_boards_per_day.append(0)
                    second_stocks_per_day.append('')

            # 如果没有连板数据，返回提示
            if all(board == 0 for board in max_boards_per_day):
                return "<div style='text-align:center; padding:50px; color:#666;'>📊 无连板股票数据</div>"

            # 创建带股票名称标签的数据点
            from pyecharts import options as opts
            from pyecharts.charts import Line

            # 准备最高板数据点，包含股票名称信息
            y_max_data = []
            for i, (board_count, stock_names) in enumerate(zip(max_boards_per_day, max_stocks_per_day)):
                if board_count > 0 and stock_names:
                    # 创建带标签的数据点
                    y_max_data.append(
                        opts.LineItem(
                            name=f"{board_count}板",
                            value=board_count,
                            label_opts=opts.LabelOpts(
                                is_show=True,
                                position="top",
                                formatter=f"{board_count}板\n{stock_names}",
                                font_size=10,
                                font_weight="bold",
                                color="#FF0000",
                                background_color="rgba(255,255,255,0.8)",
                                border_color="#FF0000",
                                border_width=1,
                                padding=[2, 4]
                            )
                        )
                    )
                else:
                    y_max_data.append(board_count)

            # 准备次高板数据点
            y_second_data = []
            for i, (board_count, stock_names) in enumerate(zip(second_boards_per_day, second_stocks_per_day)):
                if board_count > 0 and stock_names:
                    # 创建带标签的数据点
                    y_second_data.append(
                        opts.LineItem(
                            name=f"{board_count}板",
                            value=board_count,
                            label_opts=opts.LabelOpts(
                                is_show=True,
                                position="bottom",
                                formatter=f"{board_count}板\n{stock_names}",
                                font_size=9,
                                font_weight="bold",
                                color="#0066FF",
                                background_color="rgba(255,255,255,0.8)",
                                border_color="#0066FF",
                                border_width=1,
                                padding=[2, 4]
                            )
                        )
                    )
                else:
                    y_second_data.append(board_count if board_count > 0 else None)

            # 将日期转换为字符串格式
            date_strings = [
                date.strftime('%m-%d') if hasattr(date, 'strftime') else str(date)
                for date in dates
            ]

            # 创建折线图
            line = Line(init_opts=opts.InitOpts(
                height="500px",
                width="100%",
                theme='light'
            ))

            line.add_xaxis(date_strings)

            # 添加最高板折线（红色）
            line.add_yaxis(
                "最高连板数",
                y_max_data,
                symbol="circle",
                symbol_size=8,
                linestyle_opts=opts.LineStyleOpts(width=3, color="#FF0000"),
                itemstyle_opts=opts.ItemStyleOpts(color="#FF0000"),
                is_symbol_show=True,
                label_opts=opts.LabelOpts(
                    is_show=True,
                    position="top",
                    font_size=10,
                    font_weight="bold",
                    color="#FF0000"
                )
            )

            # 添加次高板折线（蓝色）
            line.add_yaxis(
                "次高连板数",
                y_second_data,
                symbol="diamond",
                symbol_size=6,
                linestyle_opts=opts.LineStyleOpts(width=3, color="#0066FF"),
                itemstyle_opts=opts.ItemStyleOpts(color="#0066FF"),
                is_symbol_show=True,
                label_opts=opts.LabelOpts(
                    is_show=True,
                    position="bottom",
                    font_size=9,
                    font_weight="bold",
                    color="#0066FF"
                )
            )

            # 设置全局配置 - 修复布局问题
            line.set_global_opts(
                title_opts=opts.TitleOpts(
                    pos_left="center",
                    pos_top="2%"
                ),
                xaxis_opts=opts.AxisOpts(
                    name="日期",
                    name_location="middle",
                    name_gap=30,
                    axislabel_opts=opts.LabelOpts(
                        rotate=30,  # 减少旋转角度
                        font_size=10
                    )
                ),
                yaxis_opts=opts.AxisOpts(
                    name="连板数",
                    name_location="middle",
                    name_gap=50,
                    min_=0
                ),
                tooltip_opts=opts.TooltipOpts(
                    trigger="axis",
                    axis_pointer_type="cross",
                    formatter="{b}<br/>{a}: {c}板"
                ),
                legend_opts=opts.LegendOpts(
                    pos_top="12%",  # 调整图例位置
                    pos_left="center"
                ),
                datazoom_opts=[
                    opts.DataZoomOpts(
                        type_="inside",
                        xaxis_index=[0],
                        range_start=60,  # 默认显示最近40%的数据
                        range_end=100
                    ),
                    opts.DataZoomOpts(
                        type_="slider",
                        xaxis_index=[0],
                        range_start=60,
                        range_end=100,
                        pos_bottom="5%"
                    )
                ],
                toolbox_opts=opts.ToolboxOpts(
                    pos_right="5%",
                    feature=opts.ToolBoxFeatureOpts(
                        save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(),
                        data_zoom=opts.ToolBoxFeatureDataZoomOpts(),
                        restore=opts.ToolBoxFeatureRestoreOpts(),
                    )
                )
            )

            return line.render_embed()

        except Exception as e:
            print(f"连板折线图生成失败: {str(e)}")
            return f"<div style='text-align:center; padding:50px; color:#f56565;'>❌ 连板折线图生成失败: {str(e)}</div>"

    @staticmethod
    def plot_continuous_limit_ladder_stack(market_states: pl.DataFrame) -> str:
        """绘制连板天梯堆叠图（统一高度版），并修复只显示一天/一格的问题"""
        if market_states is None or market_states.is_empty():
            return "<div style='text-align:center; padding:50px; color:#666;'>📊 无连板分布数据</div>"

        try:
            # 检查是否有连板数据
            if '连板数' not in market_states.columns:
                return "<div style='text-align:center; padding:50px; color:#666;'>📊 数据中缺少连板信息</div>"

            # 1. 获得所有日期
            dates = sorted(market_states['日期'].unique().to_list())

            if not dates:
                return "<div style='text-align:center; padding:50px; color:#666;'>📊 无连板日期数据</div>"

            # 2. 计算每日最大连板数（强制转成 Python int）
            daily_max_list = []
            for d in dates:
                dd = market_states.filter(pl.col('日期') == d)
                if dd.is_empty():
                    daily_max_list.append(0)
                else:
                    # 拿到单个标量
                    max_board = dd.select(pl.col('连板数').max()).to_series()[0]
                    daily_max_list.append(int(max_board) if max_board is not None else 0)

            max_boards = max(daily_max_list)
            if max_boards <= 0:
                return "<div style='text-align:center; padding:50px; color:#666;'>📊 无连板股票数据</div>"

            # 3. 格式化 x 轴标签
            date_strings = [
                d.strftime('%m-%d') if hasattr(d, 'strftime') else str(d)
                for d in dates
            ]

            # 4. 颜色映射函数（按连板数分层）
            def pick_color(b):
                if b <= 3:
                    return '#87CEFA'  # 浅蓝色 - 低连板
                elif b <= 6:
                    return '#7B68EE'  # 中紫色 - 中连板
                elif b <= 9:
                    return '#FF69B4'  # 粉红色 - 高连板
                elif b <= 12:
                    return '#DC143C'  # 深红色 - 超高连板
                else:
                    return 'transparent'

            # 5. 创建ECharts堆叠柱状图（统一高度版）
            from pyecharts import options as opts
            from pyecharts.charts import Bar

            bar = Bar(init_opts=opts.InitOpts(
                width="100%",
                height="600px",
                theme='light'
            ))

            bar.add_xaxis(date_strings)

            # 6. 每层都固定高度 1，第二维存真实家数
            for board_num in range(1, max_boards + 1):
                # 6.1 先收集每天这个连板数的真实家数（整数）
                raw_data = []
                for d in dates:
                    dd = market_states.filter(
                        (pl.col('日期') == d) & (pl.col('连板数') == board_num)
                    )
                    cnt = dd.height if not dd.is_empty() else 0
                    raw_data.append(int(cnt))

                color = pick_color(board_num)

                # 6.2 构造每个 BarItem
                items = []
                for idx, cnt in enumerate(raw_data):
                    # 如果当天最高连板数小于当前 board_num，就不画柱子
                    if board_num <= daily_max_list[idx]:
                        val = 1  # 固定高度
                        show_label = cnt > 0
                        label_text = f"{cnt}"
                    else:
                        val = 0
                        show_label = False
                        label_text = ""

                    items.append(
                        opts.BarItem(
                            name=date_strings[idx],
                            value=val,
                            itemstyle_opts=opts.ItemStyleOpts(color=color),
                            label_opts=opts.LabelOpts(
                                is_show=show_label,
                                position="inside",
                                formatter=label_text,
                                font_size=10,
                                font_weight="bold",
                                color="white"
                            )
                        )
                    )

                bar.add_yaxis(
                    series_name=f"{board_num}连板",
                    y_axis=items,
                    stack="连板",
                    label_opts=opts.LabelOpts(
                        is_show=True,
                        position="inside",
                        font_size=10,
                        font_weight="bold",
                        color="white"
                    )
                )

            # 7. 设置全局配置（优化版 - 修复鼠标滚轮缩放）
            bar.set_global_opts(
                title_opts=opts.TitleOpts(
                    subtitle="统一高度版 - 各日期连板股票数量分布",
                    pos_left="center",
                    pos_top="2%"
                ),
                xaxis_opts=opts.AxisOpts(
                    name="日期",
                    name_location="middle",
                    name_gap=30,
                    axislabel_opts=opts.LabelOpts(
                        rotate=30,  # 减少旋转角度
                        font_size=10
                    )
                ),
                yaxis_opts=opts.AxisOpts(
                    name="连板层级",
                    name_location="middle",
                    name_gap=50
                ),
                tooltip_opts=opts.TooltipOpts(
                    trigger="axis",
                    axis_pointer_type="shadow",
                    formatter="{b0}"  # 简化的tooltip格式
                ),
                legend_opts=opts.LegendOpts(
                    pos_top="12%",  # 调整图例位置
                    pos_left="center",
                    orient="horizontal"
                ),
                datazoom_opts=[
                    # 🔧 修复：添加鼠标滚轮缩放支持
                    opts.DataZoomOpts(
                        type_="inside",  # 鼠标滚轮缩放
                        xaxis_index=[0],
                        range_start=60,
                        range_end=100
                    ),
                    opts.DataZoomOpts(
                        type_="slider",  # 滑块缩放
                        xaxis_index=[0],
                        range_start=60,
                        range_end=100,
                        pos_bottom="5%"
                    )
                ],
                toolbox_opts=opts.ToolboxOpts(
                    pos_right="5%",
                    feature=opts.ToolBoxFeatureOpts(
                        save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(),
                        data_zoom=opts.ToolBoxFeatureDataZoomOpts(),
                        restore=opts.ToolBoxFeatureRestoreOpts(),
                    )
                )
            )

            return bar.render_embed()

        except Exception as e:
            print(f"连板堆叠图生成失败: {str(e)}")
            return f"<div style='text-align:center; padding:50px; color:#f56565;'>❌ 连板堆叠图生成失败: {str(e)}</div>"

    @staticmethod
    def plot_continuous_limit_ladder_combined(market_states: pl.DataFrame) -> str:
        """绘制连板分布统计图表 - 组合折线图和堆叠图"""

        print("🔧 DEBUG: plot_continuous_limit_ladder_combined 函数被调用")

        if market_states is None or market_states.is_empty():
            print("🔧 DEBUG: 市场状态数据为空")
            return "<div style='text-align:center; padding:50px; color:#666;'>📊 无连板分布数据</div>"

        try:
            # 检查是否有连板数据
            if '连板数' not in market_states.columns:
                return "<div style='text-align:center; padding:50px; color:#666;'>📊 数据中缺少连板信息</div>"

            print("🔧 DEBUG: 开始生成折线图和堆叠图")

            # 调用折线图函数
            line_chart_html = MarketVisualizer.plot_continuous_limit_ladder_line(market_states)
            print(f"🔧 DEBUG: 折线图生成完成，长度: {len(line_chart_html) if line_chart_html else 0}")

            # 调用堆叠图函数
            stack_chart_html = MarketVisualizer.plot_continuous_limit_ladder_stack(market_states)
            print(f"🔧 DEBUG: 堆叠图生成完成，长度: {len(stack_chart_html) if stack_chart_html else 0}")

            # 如果任一图表生成失败，返回错误信息
            if not line_chart_html or not stack_chart_html:
                return "<div style='text-align:center; padding:50px; color:#f56565;'>❌ 连板图表生成失败</div>"

            # 创建复合HTML结构，包含切换功能
            combined_html = f"""
            <div class="ladder-chart-container" style="width: 100%; min-height: 600px;">
                <div class="chart-controls" style="text-align: center; margin-bottom: 20px;">
                    <button id="lineBtn" class="chart-btn active"
                            style="background: #409eff; color: white; border: none; padding: 10px 20px; margin: 0 10px; border-radius: 5px; cursor: pointer;">
                        折线图
                    </button>
                    <button id="stackBtn" class="chart-btn"
                            style="background: #FF0000; color: white; border: none; padding: 10px 20px; margin: 0 10px; border-radius: 5px; cursor: pointer;">
                        堆叠图
                    </button>
                </div>

                <div id="lineChart" class="chart-content" style="display: block;">
                    {line_chart_html}
                </div>

                <div id="stackChart" class="chart-content" style="display: none;">
                    {stack_chart_html}
                </div>

                <script>
                // 简化的JavaScript，主要用于图表重新渲染
                // 切换逻辑由前端Vue应用处理
                console.log('连板分布统计复合图表已加载');

                // 提供图表重新渲染功能
                window.resizeContinuousLimitCharts = function() {{
                    if (window.echarts) {{
                        const container = document.querySelector('.ladder-chart-container');
                        if (container) {{
                            const charts = container.querySelectorAll('[_echarts_instance_]');
                            charts.forEach(chart => {{
                                const instance = window.echarts.getInstanceByDom(chart);
                                if (instance) {{
                                    instance.resize();
                                    console.log('连板图表已重新渲染');
                                }}
                            }});
                        }}
                    }}
                }};
                </script>

                <style>
                .chart-btn.active {{
                    background: #409eff !important;
                }}
                .chart-btn:hover {{
                    opacity: 0.8;
                }}
                </style>
            </div>
            """

            print("🔧 DEBUG: 复合图表HTML生成完成")
            return combined_html

        except Exception as e:
            print(f"连板分布图表生成失败: {str(e)}")
            return f"<div style='text-align:center; padding:50px; color:#f56c6c;'>📊 连板分布图表生成失败: {str(e)}</div>"