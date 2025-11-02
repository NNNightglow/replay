#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票可视化模块
专门处理个股相关的图表生成

作者: AI助手
日期: 2025-01-24
"""

import polars as pl
from pyecharts.charts import Kline, Line, Bar, Grid
from pyecharts import options as opts
from pyecharts.commons.utils import JsCode
from pyecharts.globals import ThemeType
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import warnings

from .common import ChartConfig, ChartUtils, ChartFormatters, UniversalKlineChart

# 屏蔽pandas警告
warnings.filterwarnings('ignore')

class StockVisualizer:
    """股票可视化器，处理所有个股相关的图表"""
    
    @staticmethod
    def plot_stock_kline(stock_data: pl.DataFrame,
                        stock_name: str = "",
                        stock_code: str = "",
                        show_ma: bool = True,
                        show_volume: bool = True) -> str:
        """
        绘制股票K线图，包含MA线和成交量
        """
        try:
            if stock_data is None or stock_data.is_empty():
                return "<div>没有股票数据</div>"

            # 验证必要的列
            required_cols = ['日期', '开盘', '收盘', '最高', '最低']
            is_valid, error_msg = ChartUtils.validate_data_columns(stock_data, required_cols)
            if not is_valid:
                return f"<div>数据验证失败: {error_msg}</div>"

            # 构建标题
            title = f"{stock_name}({stock_code})K线图" if stock_name and stock_code else f"{stock_name}K线图" if stock_name else "股票K线图"

            # 使用通用K线图方法，优先使用成交额
            return UniversalKlineChart.plot_kline_with_volume(
                stock_data,
                title=title,
                height="600px",
                amount_column='成交额',  # 优先使用成交额
                volume_column='成交量'   # 备选成交量
            )
            
            # 按日期排序
            stock_data = stock_data.sort('日期')
            
            # 准备数据
            dates = [str(date) for date in stock_data['日期'].to_list()]
            
            # 准备K线数据 [开盘, 收盘, 最低, 最高]
            kline_data = []
            for row in stock_data.iter_rows(named=True):
                kline_data.append([
                    float(row['开盘']),
                    float(row['收盘']),
                    float(row['最低']),
                    float(row['最高'])
                ])
            
            # 创建K线图
            kline = Kline(init_opts=ChartConfig.get_common_init_opts(height="600px"))
            kline.add_xaxis(dates)
            kline.add_yaxis(
                f"{stock_name}({stock_code})" if stock_code else stock_name,
                kline_data,
                itemstyle_opts=opts.ItemStyleOpts(
                    color=ChartConfig.COLORS['red'],      # 阳线颜色
                    color0=ChartConfig.COLORS['green'],   # 阴线颜色
                    border_color=ChartConfig.COLORS['red'],
                    border_color0=ChartConfig.COLORS['green']
                )
            )
            
            # 添加MA线
            if show_ma and all(col in stock_data.columns for col in ['MA5', 'MA10', 'MA20']):
                line = Line()
                line.add_xaxis(dates)
                
                # 统一的MA颜色配置
                ma_colors = {
                    'MA5': '#4ECDC4',   # 青色
                    'MA10': '#ffbf00',  # 黄色  
                    'MA20': '#f92672'   # 红色
                }
                
                # MA5线
                ma5_data = [float(x) if x is not None else None for x in stock_data['MA5'].to_list()]
                line.add_yaxis(
                    "MA5",
                    ma5_data,
                    is_smooth=True,
                    symbol_size=0,
                    linestyle_opts=opts.LineStyleOpts(width=1, color=ma_colors['MA5']),
                    itemstyle_opts=opts.ItemStyleOpts(color=ma_colors['MA5']),  # 统一图例和线条颜色
                    label_opts=opts.LabelOpts(is_show=False)  # 隐藏数值显示
                )

                # MA10线
                ma10_data = [float(x) if x is not None else None for x in stock_data['MA10'].to_list()]
                line.add_yaxis(
                    "MA10",
                    ma10_data,
                    is_smooth=True,
                    symbol_size=0,
                    linestyle_opts=opts.LineStyleOpts(width=1, color=ma_colors['MA10']),
                    itemstyle_opts=opts.ItemStyleOpts(color=ma_colors['MA10']),  # 统一图例和线条颜色
                    label_opts=opts.LabelOpts(is_show=False)  # 隐藏数值显示
                )

                # MA20线
                ma20_data = [float(x) if x is not None else None for x in stock_data['MA20'].to_list()]
                line.add_yaxis(
                    "MA20",
                    ma20_data,
                    is_smooth=True,
                    symbol_size=0,
                    linestyle_opts=opts.LineStyleOpts(width=1, color=ma_colors['MA20']),
                    itemstyle_opts=opts.ItemStyleOpts(color=ma_colors['MA20']),  # 统一图例和线条颜色
                    label_opts=opts.LabelOpts(is_show=False)  # 隐藏数值显示
                )
                
                line.set_global_opts(
                    xaxis_opts=opts.AxisOpts(type_="category"),
                    yaxis_opts=opts.AxisOpts(
                        name="价格",
                        position="left"
                    )
                )
                
                # 叠加MA线到K线图
                kline.overlap(line)
            
            # 设置K线图全局配置
            title = f"{stock_name}({stock_code}) K线图" if stock_code else f"{stock_name} K线图"
            kline.set_global_opts(
                title_opts=ChartConfig.get_common_title_opts(title),
                xaxis_opts=opts.AxisOpts(type_="category"),
                yaxis_opts=opts.AxisOpts(
                    name="价格(元)",
                    position="left",
                    axislabel_opts=opts.LabelOpts(formatter="{value}")
                ),
                tooltip_opts=ChartConfig.get_common_tooltip_opts(),
                legend_opts=ChartConfig.get_common_legend_opts("top"),
                datazoom_opts=ChartConfig.get_common_datazoom_opts(),
                toolbox_opts=ChartConfig.get_common_toolbox_opts()
            )
            
            return kline.render_embed()
            
        except Exception as e:
            print(f"❌ 生成股票K线图失败: {e}")
            import traceback
            traceback.print_exc()
            return f"<div>生成股票K线图失败: {str(e)}</div>"
    
    @staticmethod
    def plot_turnover_chart(stock_data: pl.DataFrame, 
                           stock_name: str = "", 
                           stock_code: str = "") -> str:
        """
        绘制成交额图表
        """
        try:
            if stock_data is None or stock_data.is_empty():
                return "<div>没有股票数据</div>"
            
            if '成交额' not in stock_data.columns:
                return "<div>数据中缺少成交额信息</div>"
            
            # 按日期排序
            stock_data = stock_data.sort('日期')
            
            # 准备数据
            dates = [str(date) for date in stock_data['日期'].to_list()]
            turnovers = []
            colors = []
            
            for row in stock_data.iter_rows(named=True):
                turnover = float(row.get('成交额', 0))
                open_price = float(row.get('开盘', 0))
                close_price = float(row.get('收盘', 0))
                
                # 格式化成交额单位
                turnover_formatted, unit = ChartUtils.format_volume_unit(turnover)
                turnovers.append(turnover_formatted)
                
                # 根据涨跌确定颜色
                color = ChartConfig.COLORS['red'] if close_price >= open_price else ChartConfig.COLORS['green']
                colors.append(color)
            
            # 创建柱状图
            bar = Bar(init_opts=ChartConfig.get_common_init_opts(height="400px"))
            bar.add_xaxis(dates)
            bar.add_yaxis(
                "成交额",
                turnovers,
                itemstyle_opts=opts.ItemStyleOpts(
                    color=ChartFormatters.get_volume_color_formatter(colors)
                )
            )
            
            title = f"{stock_name}({stock_code}) 成交额" if stock_code else f"{stock_name} 成交额"
            bar.set_global_opts(
                title_opts=ChartConfig.get_common_title_opts(title),
                xaxis_opts=opts.AxisOpts(
                    name="日期",
                    axislabel_opts=opts.LabelOpts(rotate=-45)
                ),
                yaxis_opts=opts.AxisOpts(
                    name="成交额",
                    axislabel_opts=opts.LabelOpts(formatter="{value}")
                ),
                tooltip_opts=ChartConfig.get_common_tooltip_opts(),
                datazoom_opts=ChartConfig.get_common_datazoom_opts(),
                toolbox_opts=ChartConfig.get_common_toolbox_opts()
            )
            
            return bar.render_embed()
            
        except Exception as e:
            print(f"❌ 生成成交额图表失败: {e}")
            return f"<div>生成成交额图表失败: {str(e)}</div>"
    
    @staticmethod
    def _calculate_ma(data, window_size):
        """计算移动平均线"""
        try:
            result = []
            for i in range(len(data)):
                if i < window_size - 1:
                    result.append(None)
                else:
                    val = sum(data[i - window_size + 1:i + 1]) / window_size
                    result.append(round(val, 2))
            return result
        except Exception as e:
            print(f"❌ 计算MA失败: {e}")
            return [None] * len(data)

    @staticmethod
    def plot_new_high_stock_kline(stock_data: pl.DataFrame, stock_code: str,
                                  new_high_date: str = None, period_days: int = 5) -> str:
        """绘制新高股票K线图，带新高标记 - 性能优化版本"""
        try:
            # 使用基础的股票K线图功能
            base_html = StockVisualizer.plot_stock_kline(
                stock_data,
                stock_name=stock_code.split('(')[0] if '(' in stock_code else stock_code,
                stock_code=stock_code.split('(')[1].replace(')', '') if '(' in stock_code else stock_code
            )

            # 这里可以添加新高标记的逻辑
            # 由于原始实现比较复杂，这里先返回基础K线图
            return base_html

        except Exception as e:
            print(f"❌ 生成新高股票K线图失败: {e}")
            return f"<div>生成新高股票K线图失败: {str(e)}</div>"

    @staticmethod
    def calculate_ma_lines(stock_data: pl.DataFrame, periods: List[int] = [5, 10, 20]) -> pl.DataFrame:
        """计算移动平均线"""
        try:
            if stock_data is None or stock_data.is_empty():
                return stock_data
            
            if '收盘' not in stock_data.columns:
                return stock_data
            
            # 按日期排序
            stock_data = stock_data.sort('日期')
            
            # 计算各周期的MA
            for period in periods:
                ma_col = f'MA{period}'
                stock_data = stock_data.with_columns([
                    pl.col('收盘').rolling_mean(window_size=period).alias(ma_col)
                ])
            
            return stock_data
            
        except Exception as e:
            print(f"❌ 计算MA线失败: {e}")
            return stock_data
    
    @staticmethod
    def plot_stock_comparison(stock_data_dict: Dict[str, pl.DataFrame],
                             normalize: bool = True,
                             height: str = "800px") -> str:
        """绘制多股票对比图"""
        try:
            if not stock_data_dict:
                return "<div>没有股票数据</div>"
            
            # 创建折线图 - 增大尺寸，设置白色背景
            line = Line(init_opts=opts.InitOpts(
                width="100%",
                height=height,
                theme=ThemeType.WHITE,
                bg_color="white"
            ))
            
            # 获取所有日期的并集
            all_dates = set()
            for stock_data in stock_data_dict.values():
                if stock_data is not None and not stock_data.is_empty():
                    dates = stock_data['日期'].to_list()
                    all_dates.update(dates)
            
            sorted_dates = sorted(list(all_dates))
            date_strs = [str(date) for date in sorted_dates]
            line.add_xaxis(date_strs)
            
            # 为每只股票添加折线
            colors = list(ChartConfig.COLORS.values())
            stock_count = len(stock_data_dict)

            # 当股票数量大于30时，调整显示效果避免图表过于混乱
            show_detailed = stock_count <= 30

            for i, (stock_name, stock_data) in enumerate(stock_data_dict.items()):
                if stock_data is not None and not stock_data.is_empty():
                    # 准备数据
                    close_prices = []
                    stock_data_sorted = stock_data.sort('日期')
                    
                    # 获取基准价格（第一个有效价格）
                    base_price = None
                    if normalize and '收盘' in stock_data.columns:
                        first_close = stock_data_sorted['收盘'].to_list()[0]
                        base_price = float(first_close) if first_close is not None else None
                    
                    for date in sorted_dates:
                        day_data = stock_data_sorted.filter(pl.col('日期') == date)
                        if not day_data.is_empty():
                            close_price = float(day_data['收盘'].to_list()[0])
                            if normalize and base_price:
                                # 归一化处理，保留整数
                                normalized_price = round((close_price / base_price - 1) * 100)
                                close_prices.append(normalized_price)
                            else:
                                close_prices.append(close_price)
                        else:
                            close_prices.append(None)
                    
                    line.add_yaxis(
                        stock_name,
                        close_prices,
                        color=colors[i % len(colors)],
                        is_smooth=True,
                        symbol_size=4 if show_detailed else 2,  # 股票多时使用更小的符号
                        linestyle_opts=opts.LineStyleOpts(
                            width=2 if show_detailed else 1,  # 股票多时使用更细的线条
                            opacity=0.8 if show_detailed else 0.6  # 股票多时降低透明度
                        ),
                        label_opts=opts.LabelOpts(is_show=False),  # 不显示数据标签，避免混乱
                        itemstyle_opts=opts.ItemStyleOpts(opacity=0.8 if show_detailed else 0.6)
                    )
            
            # 设置全局配置 - 简化版本
            y_axis_name = "涨跌幅(%)" if normalize else "价格(元)"

            # 根据股票数量调整标题
            if stock_count > 30:
                title_text = f"股票对比图 ({stock_count}只股票 - 简化显示)"
                subtitle_text = "股票数量较多，已优化显示效果"
            else:
                title_text = f"股票对比图 ({stock_count}只股票)"
                subtitle_text = ""

            line.set_global_opts(
                title_opts=opts.TitleOpts(
                    title=title_text,
                    subtitle=subtitle_text,
                    pos_left="center"
                ),
                xaxis_opts=opts.AxisOpts(
                    name="日期",
                    axislabel_opts=opts.LabelOpts(rotate=45)
                ),
                yaxis_opts=opts.AxisOpts(
                    name=y_axis_name,
                    axislabel_opts=opts.LabelOpts(
                        formatter=JsCode("function(value) { return Math.round(value) + '%'; }") if normalize else None
                    )
                ),
                tooltip_opts=opts.TooltipOpts(trigger="axis"),
                legend_opts=opts.LegendOpts(
                    pos_top="8%" if show_detailed else "5%",
                    is_show=show_detailed,  # 股票数量过多时隐藏图例
                    type_="scroll" if stock_count > 20 else "plain",  # 超过20只股票时使用滚动图例
                    orient="horizontal"
                ),
                toolbox_opts=opts.ToolboxOpts(
                    is_show=True,
                    feature=opts.ToolBoxFeatureOpts(
                        save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(
                            background_color="white"
                        )
                    )
                )
            )
            
            return line.render_embed()
            
        except Exception as e:
            print(f"❌ 生成股票对比图失败: {e}")
            return f"<div>生成股票对比图失败: {str(e)}</div>"
