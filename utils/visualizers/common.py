#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用可视化模块
提供通用的图表配置、工具函数和基础类

作者: AI助手
日期: 2025-01-24
"""

import polars as pl
from pyecharts import options as opts
from pyecharts.commons.utils import JsCode
from pyecharts.globals import ThemeType
from typing import List, Dict, Any, Optional
from pyecharts.charts import Kline, Line, Bar, Grid
from datetime import datetime
import warnings

# 屏蔽pandas警告
warnings.filterwarnings('ignore')

class ChartConfig:
    """图表配置类，提供通用的图表配置和工具函数"""
    
    # 通用颜色配置 - 扩展到30种颜色支持30只股票对比
    COLORS = {
        'red': '#ef232a',      # 红色 - 上涨
        'green': '#14b143',    # 绿色 - 下跌
        'blue': '#3742fa',     # 蓝色
        'orange': '#ffa502',   # 橙色
        'purple': '#a4b0be',   # 紫色
        'pink': '#ff6b81',     # 粉色
        'cyan': '#4ecdc4',     # 青色
        'yellow': '#ffbf00',   # 黄色
        'magenta': '#f92672',  # 洋红色
        'gray': '#747d8c',     # 灰色
        # 扩展颜色 - 支持更多股票对比
        'lime': '#32ff7e',     # 酸橙色
        'indigo': '#4834d4',   # 靛蓝色
        'teal': '#00d2d3',     # 青绿色
        'amber': '#ffb142',    # 琥珀色
        'violet': '#8c7ae6',   # 紫罗兰色
        'rose': '#ff5252',     # 玫瑰色
        'emerald': '#00b894',  # 翡翠色
        'sky': '#74b9ff',      # 天空蓝
        'coral': '#fd79a8',    # 珊瑚色
        'mint': '#00b8a9',     # 薄荷色
        'gold': '#fdcb6e',     # 金色
        'lavender': '#a29bfe', # 薰衣草色
        'salmon': '#e17055',   # 鲑鱼色
        'turquoise': '#00cec9',# 绿松石色
        'crimson': '#e84393',  # 深红色
        'olive': '#6c5ce7',    # 橄榄色
        'navy': '#2d3436',     # 海军蓝
        'maroon': '#a0522d',   # 栗色
        'bronze': '#cd7f32',   # 青铜色
        'silver': '#bdc3c7'    # 银色
    }
    
    # K线图颜色配置 - 阳线空心，阴线实心
    KLINE_COLORS = [
        {"up": "transparent", "down": "#ff4757", "up_border": "#ff4757", "down_border": "#ff4757"},  # 红色系
        {"up": "transparent", "down": "#3742fa", "up_border": "#3742fa", "down_border": "#3742fa"},  # 蓝色系
        {"up": "transparent", "down": "#2ed573", "up_border": "#2ed573", "down_border": "#2ed573"},  # 绿色系
        {"up": "transparent", "down": "#ffa502", "up_border": "#ffa502", "down_border": "#ffa502"},  # 橙色系
        {"up": "transparent", "down": "#a4b0be", "up_border": "#a4b0be", "down_border": "#a4b0be"},  # 灰色系
        {"up": "transparent", "down": "#ff6b81", "up_border": "#ff6b81", "down_border": "#ff6b81"},  # 粉色系
        {"up": "transparent", "down": "#70a1ff", "up_border": "#70a1ff", "down_border": "#70a1ff"},  # 浅蓝系
        {"up": "transparent", "down": "#7bed9f", "up_border": "#7bed9f", "down_border": "#7bed9f"},  # 浅绿系
        {"up": "transparent", "down": "#eccc68", "up_border": "#eccc68", "down_border": "#eccc68"},  # 黄色系
        {"up": "transparent", "down": "#ff7675", "up_border": "#ff7675", "down_border": "#ff7675"},  # 珊瑚色系
    ]
    
    @staticmethod
    def get_common_init_opts(width: str = "100%", height: str = "600px", theme: str = ThemeType.MACARONS) -> opts.InitOpts:
        """获取通用的初始化选项"""
        return opts.InitOpts(
            width=width,
            height=height,
            theme=theme
        )
    
    @staticmethod
    def get_common_title_opts(title: str, subtitle: str = None) -> opts.TitleOpts:
        """获取通用的标题选项"""
        return opts.TitleOpts(
            title=title,
            subtitle=subtitle,
            pos_left="center",
            title_textstyle_opts=opts.TextStyleOpts(font_size=16, font_weight="bold")
        )
    
    @staticmethod
    def get_common_legend_opts(position: str = "top") -> opts.LegendOpts:
        """获取通用的图例选项"""
        if position == "top":
            return opts.LegendOpts(
                pos_top="5%",
                pos_left="center",
                orient="horizontal"
            )
        elif position == "right":
            return opts.LegendOpts(
                pos_right="2%",
                pos_top="15%",
                orient="vertical"
            )
        else:
            return opts.LegendOpts()
    
    @staticmethod
    def get_common_tooltip_opts(trigger: str = "axis") -> opts.TooltipOpts:
        """获取通用的提示框选项"""
        return opts.TooltipOpts(
            trigger=trigger,
            axis_pointer_type="cross"
        )
    
    @staticmethod
    def get_common_toolbox_opts() -> opts.ToolboxOpts:
        """获取通用的工具箱选项"""
        return opts.ToolboxOpts(
            is_show=True,
            feature=opts.ToolBoxFeatureOpts(
                save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(),
                data_zoom=opts.ToolBoxFeatureDataZoomOpts(),
                restore=opts.ToolBoxFeatureRestoreOpts(),
            )
        )
    
    @staticmethod
    def get_common_datazoom_opts() -> List[opts.DataZoomOpts]:
        """获取通用的数据缩放选项"""
        return [
            opts.DataZoomOpts(
                is_show=True,
                type_="slider",
                range_start=0,
                range_end=100
            ),
            opts.DataZoomOpts(
                is_show=True,
                type_="inside"
            )
        ]
    
    @staticmethod
    def get_kline_color_config(index: int) -> Dict[str, str]:
        """获取K线图颜色配置"""
        return ChartConfig.KLINE_COLORS[index % len(ChartConfig.KLINE_COLORS)]

class ChartUtils:
    """图表工具类，提供数据处理和格式化功能"""
    
    @staticmethod
    def prepare_chart_data(data: pl.DataFrame, date_col: str = '日期') -> List[Dict[str, Any]]:
        """准备图表数据，将Polars DataFrame转换为字典列表，并按日期排序"""
        try:
            if data is None or data.is_empty():
                return []

            # 确保日期格式
            if date_col in data.columns and data[date_col].dtype == pl.Utf8:
                try:
                    data = data.with_columns([
                        pl.col(date_col).str.strptime(pl.Date, '%Y-%m-%d').alias(date_col)
                    ])
                except Exception as e:
                    print(f"转换日期列为日期类型时出错: {str(e)}")

            # 按日期排序（左边旧日期，右边新日期）
            if date_col in data.columns:
                data = data.sort(date_col)

            return data.to_dicts()
        except Exception as e:
            print(f"❌ 数据准备失败: {e}")
            return []
    
    @staticmethod
    def format_volume_unit(value: float) -> tuple:
        """格式化成交量单位"""
        if value >= 100000000:  # 大于1亿
            return value / 100000000, "亿"
        elif value >= 10000000:  # 大于1千万
            return value / 10000000, "千万"
        elif value >= 10000:  # 大于1万
            return value / 10000, "万"
        else:
            return value, ""
    
    @staticmethod
    def validate_data_columns(data: pl.DataFrame, required_columns: List[str]) -> tuple:
        """验证数据列是否完整"""
        if data is None or data.is_empty():
            return False, "数据为空"
        
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            return False, f"缺少必要列: {missing_columns}"
        
        return True, "数据验证通过"
    
    @staticmethod
    def extract_chart_content(html_content: str) -> str:
        """提取图表的核心内容，去除完整HTML文档结构"""
        if not html_content or not isinstance(html_content, str):
            return html_content
        
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 查找图表容器div
            chart_div = soup.find('div', {'id': lambda x: x and x.startswith('chart_')})
            if chart_div:
                # 获取div和相关的script标签
                result_parts = [str(chart_div)]
                
                # 查找相关的script标签
                for script in soup.find_all('script'):
                    if script.string and 'chart_' in script.string:
                        result_parts.append(str(script))
                
                return '\n'.join(result_parts)
            else:
                # 如果找不到特定的图表div，返回body内容
                body = soup.find('body')
                return str(body) if body else html_content
                
        except Exception as e:
            print(f"⚠️ 提取图表内容失败: {e}")
            return html_content
    
    @staticmethod
    def create_kline_data_from_change_pct(data: List[Dict[str, Any]],
                                         change_col: str = '涨跌幅',
                                         amplitude_col: str = '振幅') -> List[List[float]]:
        """基于涨跌幅创建K线数据（累计涨跌幅，以0%为起点）"""
        kline_data = []
        cumulative_change = 0.0  # 以0%为起点

        for row in data:
            change_pct = float(row.get(change_col, 0))
            amplitude = float(row.get(amplitude_col, abs(change_pct)))

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

        return kline_data


class UniversalKlineChart:
    """通用K线图绘制器"""

    @staticmethod
    def plot_kline_with_volume(data, title: str = None, height: str = "600px",
                              volume_column: str = None, amount_column: str = None) -> str:
        """通用K线图绘制方法，支持指数、板块、个股

        Args:
            data: 数据，包含日期、开盘价、收盘价、最高价、最低价等列
            title: 图表标题，默认为None
            height: 图表高度，默认为"600px"
            volume_column: 成交量列名，如'成交量'
            amount_column: 成交额列名，如'成交额'

        Returns:
            生成的HTML图表代码
        """


        # 确保数据按日期排序
        if hasattr(data, 'sort'):
            data = data.sort('日期')
            data_list = data.to_dicts()
        else:
            # 如果是pandas DataFrame
            data = data.sort_values('日期')
            data_list = data.to_dict('records')

        # 准备数据
        dates = [d['日期'].strftime('%Y-%m-%d') if isinstance(d['日期'], datetime) else str(d['日期']) for d in data_list]

        # 准备K线数据 [open, close, low, high]
        k_data = []
        for d in data_list:
            open_val = d.get('开盘', d.get('开盘价', 0))
            close_val = d.get('收盘', d.get('收盘价', 0))
            low_val = d.get('最低', d.get('最低价', 0))
            high_val = d.get('最高', d.get('最高价', 0))
            # 处理None值
            try:
                open_val = float(open_val) if open_val is not None else 0.0
                close_val = float(close_val) if close_val is not None else 0.0
                low_val = float(low_val) if low_val is not None else 0.0
                high_val = float(high_val) if high_val is not None else 0.0
                k_data.append([open_val, close_val, low_val, high_val])
            except (ValueError, TypeError):
                # 如果转换失败，跳过这条数据
                continue

        # 准备成交量/成交额数据
        volumes = []
        volume_label = "成交量"
        volume_unit = ""

        if amount_column and amount_column in (data.columns if hasattr(data, 'columns') else [d.keys() for d in data_list][0]):
            # 优先使用成交额，转换为亿元单位
            for d in data_list:
                amount = d.get(amount_column, 0)
                volumes.append(float(amount) / 100000000)  # 转换为亿元
            volume_label = "成交额"
            volume_unit = "亿"
        elif volume_column and volume_column in (data.columns if hasattr(data, 'columns') else [d.keys() for d in data_list][0]):
            # 使用成交量，转换为万手单位
            for d in data_list:
                vol = d.get(volume_column, 0)
                volumes.append(float(vol) / 10000)  # 转换为万手
            volume_label = "成交量"
            volume_unit = "万手"
        else:
            # 默认尝试查找成交量或成交额字段
            for d in data_list:
                amount = d.get('成交额', 0)
                if amount > 0:
                    volumes.append(float(amount) / 100000000)  # 转换为亿元
                    volume_label = "成交额"
                    volume_unit = "亿"
                else:
                    vol = d.get('成交量', d.get('volume', d.get('vol', 0)))
                    volumes.append(float(vol) / 10000)  # 转换为万手
                    volume_label = "成交量"
                    volume_unit = "万手"

        # 计算涨跌情况，用于确定成交量颜色
        color_list = []
        for i in range(len(k_data)):
            if k_data[i][1] > k_data[i][0]:  # close > open
                color_list.append(1)  # 上涨为1
            else:
                color_list.append(-1)  # 下跌或平盘为-1

        # 准备数据缩放选项，用于同时控制K线图和成交量图
        datazoom_opts = [
            opts.DataZoomOpts(
                type_="inside",
                xaxis_index=[0, 1],  # 同时控制K线图(0)和成交量图(1)
                range_start=70,  # 默认显示最后30%的数据
                range_end=100
            ),
            opts.DataZoomOpts(
                type_="slider",
                xaxis_index=[0, 1],  # 同时控制K线图(0)和成交量图(1)
                range_start=70,  # 默认显示最后30%的数据
                range_end=100,
                pos_bottom="5%"
            ),
        ]

        # 创建K线图
        kline = Kline()
        kline.add_xaxis(dates)
        kline.add_yaxis(
            "K线",
            k_data,
            itemstyle_opts=opts.ItemStyleOpts(
                color="#ef232a",  # 上涨为红色
                color0="#14b143",  # 下跌为绿色
                border_color="#ef232a",
                border_color0="#14b143",
            ),
        )

        # 设置K线图全局选项
        kline.set_global_opts(
            title_opts=opts.TitleOpts(
                title=title if title else "K线图",
                pos_left="center",
            ),
            legend_opts=opts.LegendOpts(
                pos_right="0%",
                pos_top="5%",
                orient="vertical"
            ),
            xaxis_opts=opts.AxisOpts(
                type_="category",
                is_scale=True,
                boundary_gap=False,
                axisline_opts=opts.AxisLineOpts(is_on_zero=False),
                splitline_opts=opts.SplitLineOpts(is_show=False),
                split_number=20,
                min_="dataMin",
                max_="dataMax",
            ),
            yaxis_opts=opts.AxisOpts(
                is_scale=True,
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="cross",
            ),
            datazoom_opts=datazoom_opts,
            toolbox_opts=opts.ToolboxOpts(
                is_show=True,
                feature={
                    "saveAsImage": {},
                    "dataZoom": {},
                    "dataView": {},
                    "restore": {},
                }
            ),
        )

        # 计算移动平均线
        ma_list = [5, 10, 20]  # 只显示主要的MA线
        ma_series = {}

        for ma in ma_list:
            ma_data = []
            for i in range(len(k_data)):
                if i < ma - 1:
                    ma_data.append(None)
                else:
                    ma_sum = sum([k_data[j][1] for j in range(i - ma + 1, i + 1)])
                    ma_data.append(round(ma_sum / ma, 2))
            ma_series[f'MA{ma}'] = ma_data

        # 添加移动平均线
        line = Line()
        line.add_xaxis(dates)

        colors = ['#4ECDC4', '#ffbf00', '#f92672']  # MA5青色, MA10黄色, MA20红色
        for i, ma in enumerate(ma_list):
            line.add_yaxis(
                f"MA{ma}",
                ma_series[f'MA{ma}'],
                is_smooth=True,
                is_symbol_show=False,
                symbol_size=0,  # 确保不显示符号
                linestyle_opts=opts.LineStyleOpts(width=2, opacity=0.8, color=colors[i]),
                itemstyle_opts=opts.ItemStyleOpts(color=colors[i]),  # 统一图例和线条颜色
                label_opts=opts.LabelOpts(is_show=False),  # 隐藏数值标签
            )

        # 将均线叠加到K线图上
        overlap_kline = kline.overlap(line)

        # 创建成交量/成交额图
        bar = Bar()
        bar.add_xaxis(dates)
        bar.add_yaxis(
            f"{volume_label}({volume_unit})",
            volumes,
            label_opts=opts.LabelOpts(is_show=False),
            itemstyle_opts=opts.ItemStyleOpts(
                color=JsCode(
                    f"""function(params) {{
    var colorList = {color_list};
    if (colorList[params.dataIndex] > 0) {{
        return '#ef232a';  // 上涨红色
    }} else {{
        return '#14b143';  // 下跌绿色
    }}
}}"""
                )
            ),
        )

        # 成交量图设置
        bar.set_global_opts(
            xaxis_opts=opts.AxisOpts(
                type_="category",
                is_scale=True,
                grid_index=1,
                boundary_gap=False,
                axisline_opts=opts.AxisLineOpts(is_on_zero=False),
                axistick_opts=opts.AxisTickOpts(is_show=False),
                splitline_opts=opts.SplitLineOpts(is_show=False),
                axislabel_opts=opts.LabelOpts(is_show=False),
                split_number=20,
                min_="dataMin",
                max_="dataMax",
            ),
            yaxis_opts=opts.AxisOpts(
                is_scale=True,
                grid_index=1,
                splitline_opts=opts.SplitLineOpts(is_show=True),
                axislabel_opts=opts.LabelOpts(is_show=True, formatter=f"{{value}} {volume_unit}"),
            ),
            legend_opts=opts.LegendOpts(is_show=False),
        )

        # 创建网格布局
        grid = Grid(init_opts=opts.InitOpts(
            width="100%",
            height=height,
            animation_opts=opts.AnimationOpts(animation=False),
        ))

        # 添加K线图和成交量图到网格
        grid.add(
            overlap_kline,
            grid_opts=opts.GridOpts(
                pos_left="10%",
                pos_right="8%",
                pos_top="10%",
                height="60%"
            ),
        )

        grid.add(
            bar,
            grid_opts=opts.GridOpts(
                pos_left="10%",
                pos_right="8%",
                pos_top="75%",
                height="15%"
            ),
        )

        return grid.render_embed()  # 返回嵌入式HTML代码


class ChartFormatters:
    """图表格式化器类，提供各种数据格式化函数"""
    
    @staticmethod
    def get_percentage_formatter() -> JsCode:
        """获取百分比格式化器"""
        return JsCode("""
        function(params) {
            return params.value + '%';
        }
        """)
    
    @staticmethod
    def get_currency_formatter(unit: str = "亿") -> JsCode:
        """获取货币格式化器"""
        return JsCode(f"""
        function(params) {{
            return params.value + '{unit}';
        }}
        """)
    
    @staticmethod
    def get_change_color_formatter() -> JsCode:
        """获取涨跌颜色格式化器"""
        return JsCode("""
        function(params) {
            return params.value >= 0 ? '#ef232a' : '#14b143';
        }
        """)
    
    @staticmethod
    def get_volume_color_formatter(colors: List[str]) -> JsCode:
        """获取成交量颜色格式化器"""
        return JsCode(f"""
        function(params) {{
            var colors = {colors};
            return colors[params.dataIndex % colors.length];
        }}
        """)
