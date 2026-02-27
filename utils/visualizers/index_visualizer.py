"""
指数可视化模块

提供指数K线图、多指数对比等可视化功能
"""

import warnings
warnings.filterwarnings('ignore')

import polars as pl
import pandas as pd
from pyecharts.charts import Kline, Line, Bar, Grid
from pyecharts import options as opts
from pyecharts.commons.utils import JsCode
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from .common import ChartConfig, ChartUtils, ChartFormatters, UniversalKlineChart


class IndexVisualizer:
    """指数可视化器"""

    @staticmethod
    def _safe_float(value, default: float = 0.0) -> float:
        """安全转换为float，兼容None/空字符串/非法值"""
        try:
            if value is None:
                return default
            if isinstance(value, str):
                value = value.strip()
                if value == "":
                    return default
            return float(value)
        except Exception:
            return default
    
    @staticmethod
    def plot_index_kline(index_data: pl.DataFrame, title: str = None, height: str = "600px") -> str:
        """绘制指数K线图，红绿K线对应红绿色成交量
        
        Args:
            index_data: 指数数据，包含日期、开盘价、收盘价、最高价、最低价、成交量等列
            title: 图表标题，默认为None
            height: 图表高度，默认为"600px"
            
        Returns:
            生成的HTML图表代码
        """
        # 使用通用K线图方法，指定成交量列
        return UniversalKlineChart.plot_kline_with_volume(
            index_data, 
            title=title if title else "指数K线图", 
            height=height,
            volume_column='成交量'
        )

    @staticmethod
    def get_multi_index_kline_options(index_data_dict: Dict[str, pl.DataFrame]) -> dict:
        """
        生成多指数K线图的ECharts配置 - 每个指数单独显示
        """
        try:
            print(f"🎨 开始生成多指数K线图ECharts配置，收到 {len(index_data_dict)} 个指数数据")

            if not index_data_dict:
                return None

            # 为每个指数生成单独的图表配置
            charts = []

            for index_name, data in index_data_dict.items():
                print(f"📊 处理指数: {index_name}, 数据行数: {data.height}")

                # 确保数据按日期排序
                data = data.sort('日期')
                
                # 转换为字典列表，方便处理
                data_list = data.to_dicts()
                
                # 准备数据
                dates = [d['日期'].strftime('%Y-%m-%d') if isinstance(d['日期'], datetime) else d['日期'] for d in data_list]
                
                # 准备K线数据 [open, close, low, high]
                k_data = []
                for d in data_list:
                    open_val = d.get('开盘', d.get('开盘价', 0))
                    close_val = d.get('收盘', d.get('收盘价', 0))
                    low_val = d.get('最低', d.get('最低价', 0))
                    high_val = d.get('最高', d.get('最高价', 0))
                    k_data.append([float(open_val), float(close_val), float(low_val), float(high_val)])
                
                # 准备成交额数据（包含颜色信息，单位：亿元）
                amounts = []
                for d in data_list:
                    # 优先使用成交额/amount；若缺失则用 收盘*成交量 近似
                    amt = d.get('成交额', d.get('amount'))
                    if amt is None:
                        vol_fallback = d.get('成交量', d.get('volume', d.get('vol')))
                        close_price = d.get('收盘', d.get('收盘价'))
                        try:
                            if vol_fallback is not None and close_price is not None:
                                amt = float(vol_fallback) * float(close_price)
                            else:
                                amt = 0.0
                        except Exception:
                            amt = 0.0

                    open_val = d.get('开盘', d.get('开盘价', 0))
                    close_val = d.get('收盘', d.get('收盘价', 0))
                    
                    # 根据K线涨跌确定成交额颜色
                    color = '#ef232a' if float(close_val) >= float(open_val) else '#14b143'
                    
                    amounts.append({
                        'value': float(amt) / 100000000,  # 转换为亿元
                        'itemStyle': {'color': color}
                    })
                
                # 计算移动平均线
                ma_list = [5, 10, 20]
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
                
                # 生成单个指数的图表配置
                chart_config = {
                    'title': f'{index_name}指数K线图',
                    'dates': dates,
                    'kline_data': k_data,
                    'amount_data': amounts,
                    'ma_data': ma_series
                }
                
                charts.append(chart_config)
            
            print(f"🎉 多指数K线图ECharts配置生成完成，包含 {len(charts)} 个独立图表")

            # 为每个图表生成完整的ECharts配置
            echarts_configs = []

            for chart_config in charts:
                echarts_option = {
                    'title': {
                        'text': chart_config['title'],
                        'left': 'center'
                    },
                    'axisPointer': {
                        'type': 'cross',
                        'link': [{'xAxisIndex': [0, 1]}],
                        'label': { 'show': True }
                    },
                    'tooltip': {
                        'trigger': 'axis',
                        'triggerOn': 'mousemove|click',
                        'show': True,
                        'showContent': True,
                        'confine': True,
                        'appendToBody': True,
                        'axisPointer': {
                            'type': 'cross'
                        },
                        'formatter': {
                            '__js_function__': """
                            function (params) {
                                try {
                                  if (!params || !params.length) return '';
                                  var axisValue = params[0].axisValue;
                                  // 确保日期为 yyyy-mm-dd
                                  var dv = String(axisValue);
                                  if (/^\d{8}$/.test(dv)) {
                                    axisValue = dv.slice(0,4) + '-' + dv.slice(4,6) + '-' + dv.slice(6,8);
                                  }
                                  var lines = [axisValue];
                                  var kParam = null;
                                  for (var i = 0; i < params.length; i++) {
                                      if (params[i] && params[i].seriesType === 'candlestick') { kParam = params[i]; break; }
                                  }
                                  // 兼容性回退：有些情况下seriesType识别异常，尝试通过数据形状识别K线
                                  if (!kParam) {
                                      for (var i = 0; i < params.length; i++) {
                                          var d = params[i] && params[i].data;
                                          var vtmp = (d && d.value) ? d.value : d;
                                          if (Array.isArray(vtmp) && vtmp.length >= 4) { kParam = params[i]; break; }
                                      }
                                  }
                                  if (kParam) {
                                      var raw = kParam.data;
                                      var v = (raw && raw.value) ? raw.value : raw;
                                      if (Array.isArray(v) && v.length >= 4) {
                                        var open = Number(v[0]), close = Number(v[1]), low = Number(v[2]), high = Number(v[3]);
                                        var changePct = (open ? ((close - open) / open * 100) : null);
                                        lines.push('开盘: ' + (isFinite(open) ? open : '-'));
                                        lines.push('收盘: ' + (isFinite(close) ? close : '-'));
                                        lines.push('最低: ' + (isFinite(low) ? low : '-'));
                                        lines.push('最高: ' + (isFinite(high) ? high : '-'));
                                        if (changePct != null && isFinite(changePct)) {
                                            var cp = Math.round(changePct * 100) / 100;
                                            lines.push('涨跌幅: ' + cp.toFixed(2) + '%');
                                        }
                                      }
                                  }
                                  // 补充其他系列（均线、成交量）
                                  for (var j = 0; j < params.length; j++) {
                                      var p = params[j];
                                      if (p.seriesType !== 'candlestick' && p.seriesName !== '成交额') {
                                          lines.push(p.seriesName + ': ' + (p.value == null ? '-' : p.value));
                                      }
                                      if (p.seriesName === '成交额') {
                                          lines.push('成交额: ' + (p.value == null ? '-' : p.value) + '亿');
                                      }
                                  }
                                  return lines.join('<br/>');
                                } catch (e) {
                                  // 如果formatter异常，至少返回日期，避免无内容
                                  try {
                                    var fallback = (params && params.length) ? params[0].axisValue : '';
                                    var s = String(fallback);
                                    if (/^\d{8}$/.test(s)) {
                                      return s.slice(0,4) + '-' + s.slice(4,6) + '-' + s.slice(6,8);
                                    }
                                    return s;
                                  } catch(_) {
                                    return '';
                                  }
                                }
                            }
                            """
                        }
                    },
                    'legend': {
                        'data': ['K线', 'MA5', 'MA10', 'MA20', '成交额'],
                        'top': 30
                    },
                    'grid': [
                        {
                            'left': '10%',
                            'right': '8%',
                            'height': '62%'
                        },
                        {
                            'left': '10%',
                            'right': '8%',
                            'top': '80%',
                            'height': '12%',
                            'bottom': '6%'
                        }
                    ],
                    'xAxis': [
                        {
                            'type': 'category',
                            'data': chart_config['dates'],
                            'scale': True,
                            'boundaryGap': False,
                            'axisLine': {'onZero': False},
                            'splitLine': {'show': False},
                            'min': 'dataMin',
                            'max': 'dataMax'
                        },
                        {
                            'type': 'category',
                            'gridIndex': 1,
                            'data': chart_config['dates'],
                            'scale': True,
                            'boundaryGap': False,
                            'axisLine': {'onZero': False},
                            'axisTick': {'show': False},
                            'splitLine': {'show': False},
                            'axisLabel': {'show': False},
                            'min': 'dataMin',
                            'max': 'dataMax'
                        }
                    ],
                    'yAxis': [
                        {
                            'scale': True,
                            'splitArea': {'show': True}
                        },
                        {
                            'scale': True,
                            'gridIndex': 1,
                            'splitNumber': 2,
                            'name': '成交额(亿)',
                            'axisLabel': {
                                'show': True,
                                'formatter': '{value}亿'
                            },
                            'axisLine': {'show': True},
                            'axisTick': {'show': True},
                            'splitLine': {'show': True}
                        }
                    ],
                    'dataZoom': [
                        {
                            'type': 'inside',
                            'xAxisIndex': [0, 1],
                            'start': 70,
                            'end': 100
                        },
                        {
                            'show': True,
                            'xAxisIndex': [0, 1],
                            'type': 'slider',
                            'top': '94%',
                            'start': 70,
                            'end': 100
                        }
                    ],
                    'series': [
                        {
                            'name': 'K线',
                            'type': 'candlestick',
                            'data': chart_config['kline_data'],
                            'itemStyle': {
                                'color': '#ef232a',
                                'color0': '#14b143',
                                'borderColor': '#ef232a',
                                'borderColor0': '#14b143'
                            }
                        },
                        {
                            'name': 'MA5',
                            'type': 'line',
                            'data': chart_config['ma_data']['MA5'],
                            'smooth': True,
                            'symbol': 'none',  # 去掉圆圈标示
                            'lineStyle': {
                                'color': '#4ECDC4',  # 青色
                                'width': 1,
                                'opacity': 0.8
                            },
                            'itemStyle': {
                                'color': '#4ECDC4'  # 统一图例颜色
                            }
                        },
                        {
                            'name': 'MA10',
                            'type': 'line',
                            'data': chart_config['ma_data']['MA10'],
                            'smooth': True,
                            'symbol': 'none',  # 去掉圆圈标示
                            'lineStyle': {
                                'color': '#ffbf00',  # 黄色
                                'width': 1,
                                'opacity': 0.8
                            },
                            'itemStyle': {
                                'color': '#ffbf00'  # 统一图例颜色
                            }
                        },
                        {
                            'name': 'MA20',
                            'type': 'line',
                            'data': chart_config['ma_data']['MA20'],
                            'smooth': True,
                            'symbol': 'none',  # 去掉圆圈标示
                            'lineStyle': {
                                'color': '#f92672',  # 红色
                                'width': 1,
                                'opacity': 0.8
                            },
                            'itemStyle': {
                                'color': '#f92672'  # 统一图例颜色
                            }
                        },
                        {
                            'name': '成交额',
                            'type': 'bar',
                            'xAxisIndex': 1,
                            'yAxisIndex': 1,
                            'data': chart_config['amount_data'],
                            'tooltip': {
                                'valueFormatter': {
                                    '__js_function__': 'function (val) { return (val == null ? "-" : (Number(val).toFixed(2) + "亿")); }'
                                }
                            }
                        }
                    ]
                }

                echarts_configs.append({
                    'name': chart_config['title'],
                    'option': echarts_option
                })

            return {
                'type': 'multiple',
                'charts': echarts_configs,
                'total_count': len(echarts_configs)
            }
            
        except Exception as e:
            print(f"❌ 生成多指数K线图ECharts配置失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    @staticmethod
    def plot_multi_index_kline(index_data_dict: Dict[str, pl.DataFrame]) -> str:
        """
        绘制多指数K线图 - 每个指数单独显示
        """
        try:
            print(f"🎨 开始绘制多指数K线图，收到 {len(index_data_dict)} 个指数数据")

            if not index_data_dict:
                return "<div>无指数数据</div>"

            # 为每个指数生成一个K线图
            chart_htmls = []

            for index_name, data in index_data_dict.items():
                print(f"📊 处理指数: {index_name}, 数据行数: {data.height}")

                # 为每个指数生成一个K线图
                print(f"🔄 开始生成 {index_name} 的K线图...")
                chart_html = IndexVisualizer.plot_index_kline(
                    data,
                    title=f"{index_name}指数K线图",
                    height="600px"
                )

                if chart_html:
                    print(f"✅ {index_name} K线图生成成功，HTML长度: {len(chart_html)}")
                    chart_htmls.append(chart_html)
                else:
                    print(f"❌ {index_name} K线图生成失败")

            if not chart_htmls:
                return "<div>无法生成K线图</div>"

            # 将所有图表组合成一个HTML
            combined_html = '\n'.join(chart_htmls)
            print(f"🎉 多指数K线图生成完成，总HTML长度: {len(combined_html)}, 包含 {len(chart_htmls)} 个图表")
            
            return combined_html
            
        except Exception as e:
            print(f"❌ 绘制多指数K线图失败: {e}")
            import traceback
            traceback.print_exc()
            return f"<div>生成多指数K线图失败: {str(e)}</div>"

    @staticmethod
    def calculate_ma(data: List[float], window_size: int) -> List[float]:
        """计算移动平均线"""
        result = []
        for i in range(len(data)):
            if i < window_size - 1:
                result.append(None)
            else:
                val = sum(data[i - window_size + 1:i + 1]) / window_size
                result.append(round(val, 2))
        return result

    @staticmethod
    def plot_market_volume_chart(current_data: pl.DataFrame, previous_data: pl.DataFrame, 
                                comparison_data: Dict, height: str = "400px") -> str:
        """绘制市场量能图 - 包含折线图和差分柱状图
        
        Args:
            current_data: 当日分钟成交额数据
            previous_data: 前日分钟成交额数据
            comparison_data: 对比统计数据
            height: 图表高度
            
        Returns:
            HTML图表代码
        """
        try:
            print(f"🎨 开始绘制市场量能图...")
            
            from pyecharts.charts import Line, Bar, Grid
            from pyecharts import options as opts
            from pyecharts.commons.utils import JsCode
            
            # 处理时间轴数据
            current_times = []
            current_volumes = []
            previous_volumes = []
            volume_diff = []
            
            # 获取当日数据
            current_dict = current_data.to_dicts()
            previous_dict = previous_data.to_dicts()
            
            # 兼容累计列命名差异
            cum_cols = ("累计总成交额", "总累计成交额")
            total_col = "总成交额"

            # 创建时间->成交额的映射
            previous_volume_map = {}
            for row in previous_dict:
                time_str = row['时间']
                # 处理datetime对象或字符串
                if hasattr(time_str, 'strftime'):
                    # datetime对象
                    time_part = time_str.strftime('%H:%M')
                elif isinstance(time_str, str):
                    # 字符串
                    if ' ' in time_str:
                        time_part = time_str.split(' ')[1][:5]  # HH:MM
                    else:
                        time_part = time_str[:5] if len(time_str) >= 5 else time_str
                else:
                    continue
                # 优先使用累计成交额
                previous_raw = None
                for c in cum_cols:
                    previous_raw = row.get(c)
                    if previous_raw is not None:
                        break
                if previous_raw is None:
                    previous_raw = row.get(total_col, 0)
                previous_vol = IndexVisualizer._safe_float(previous_raw, 0.0)
                previous_volume_map[time_part] = previous_vol
            
            # 处理当日数据并计算差值
            for row in current_dict:
                time_str = row['时间']
                # 处理datetime对象或字符串
                if hasattr(time_str, 'strftime'):
                    # datetime对象
                    time_part = time_str.strftime('%H:%M')
                elif isinstance(time_str, str):
                    # 字符串
                    if ' ' in time_str:
                        time_part = time_str.split(' ')[1][:5]  # HH:MM
                    else:
                        time_part = time_str[:5] if len(time_str) >= 5 else time_str
                else:
                    continue
                
                # 优先使用累计成交额
                current_raw = None
                for c in cum_cols:
                    current_raw = row.get(c)
                    if current_raw is not None:
                        break
                if current_raw is None:
                    current_raw = row.get(total_col, 0)
                current_vol = IndexVisualizer._safe_float(current_raw, 0.0)
                previous_vol = previous_volume_map.get(time_part, 0)
                
                current_times.append(time_part)
                current_volumes.append(round(current_vol, 2))
                previous_volumes.append(round(previous_vol, 2))
                volume_diff.append(round(current_vol - previous_vol, 2))
            
            if not current_times:
                return "<div>无可用的市场量能数据</div>"
            
            # 创建折线图 - 当日和昨日成交额对比
            line_chart = Line(init_opts=opts.InitOpts(width="100%", height=height))
            line_chart.add_xaxis(current_times)
            
            # 当日成交额折线
            line_chart.add_yaxis(
                series_name="今日累计成交额",
                y_axis=current_volumes,
                symbol="none",
                label_opts=opts.LabelOpts(is_show=False),
                tooltip_opts=opts.TooltipOpts(
                    formatter=JsCode("function(params){ return params.name + '<br/>' + params.seriesName + ': ' + params.value + '亿元'; }")
                )
            )
            
            # 昨日成交额折线
            line_chart.add_yaxis(
                series_name="昨日累计成交额",
                y_axis=previous_volumes,
                symbol="none", 
                label_opts=opts.LabelOpts(is_show=False),
                tooltip_opts=opts.TooltipOpts(
                    formatter=JsCode("function(params){ return params.name + '<br/>' + params.seriesName + ': ' + params.value + '亿元'; }")
                )
            )
            
            # 对x轴数据按时间排序，并同步重排各序列
            try:
                combined = list(zip(current_times, current_volumes, previous_volumes, volume_diff))
                combined.sort(key=lambda x: x[0])  # HH:MM 字符串可直接排序
                current_times, current_volumes, previous_volumes, volume_diff = [list(t) for t in zip(*combined)] if combined else ([], [], [], [])
            except Exception:
                pass

            line_chart.set_global_opts(
                title_opts=opts.TitleOpts(
                    title="市场量能对比（累计成交额）- 5分钟间隔",
                    subtitle=f"今日累计: {comparison_data['current_total']:.2f}亿 | 昨日累计: {comparison_data['previous_total']:.2f}亿 | 变化: {comparison_data['change_amount']:.2f}亿({comparison_data['change_pct']:.2f}%)",
                    pos_left="center"
                ),
                legend_opts=opts.LegendOpts(pos_top="8%"),
                xaxis_opts=opts.AxisOpts(
                    name="时间",
                    type_="category",
                    axislabel_opts=opts.LabelOpts(rotate=45, font_size=10)
                ),
                yaxis_opts=opts.AxisOpts(
                    name="成交额(亿元)",
                    type_="value",
                    axislabel_opts=opts.LabelOpts(formatter="{value}亿")
                ),
                tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
                toolbox_opts=opts.ToolboxOpts(
                    is_show=True,
                    feature={
                        "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(is_show=True),
                        "restore": opts.ToolBoxFeatureRestoreOpts(is_show=True),
                        "dataView": opts.ToolBoxFeatureDataViewOpts(is_show=True),
                        "dataZoom": opts.ToolBoxFeatureDataZoomOpts(is_show=True),
                        "magicType": opts.ToolBoxFeatureMagicTypeOpts(is_show=True, type_=["line", "bar"])
                    }
                )
            )
            
            # 设置线条样式 - 为不同系列设置不同颜色
            line_chart.set_series_opts(
                linestyle_opts=opts.LineStyleOpts(width=2)
            )
            
            # 单独设置每个系列的颜色
            line_chart.set_series_opts(
                linestyle_opts=opts.LineStyleOpts(width=2, color="#e74c3c"),  # 今日成交额红色
                series_name="今日成交额"
            )
            line_chart.set_series_opts(
                linestyle_opts=opts.LineStyleOpts(width=2, color="#95a5a6"),  # 昨日成交额灰色
                series_name="昨日成交额"
            )
            
            # 创建差分柱状图 - 今日减昨日的差值
            bar_chart = Bar(init_opts=opts.InitOpts(width="100%", height="250px"))
            bar_chart.add_xaxis(current_times)
            
            # 设置差额柱状图 - 根据差额正负设置颜色
            bar_data = []
            for i, diff in enumerate(volume_diff):
                color = "#ef232a" if diff > 0 else "#14b143"  # 差额大于0红色，小于0绿色
                bar_data.append({
                    'value': diff,
                    'itemStyle': {'color': color}
                })
            
            bar_chart.add_yaxis(
                series_name="成交额差值",
                y_axis=bar_data,
                label_opts=opts.LabelOpts(is_show=False),
                tooltip_opts=opts.TooltipOpts(
                    formatter=JsCode("function(params){ return params.name + '<br/>' + params.seriesName + ': ' + params.value + '亿元'; }")
                )
            )
            
            bar_chart.set_global_opts(
                title_opts=opts.TitleOpts(
                    title="成交额差分 - 5分钟间隔",
                    subtitle="今日减昨日成交额差值",
                    pos_left="center"
                ),
                legend_opts=opts.LegendOpts(pos_top="8%"),
                xaxis_opts=opts.AxisOpts(
                    name="时间",
                    type_="category",
                    axislabel_opts=opts.LabelOpts(rotate=45, font_size=10)
                ),
                yaxis_opts=opts.AxisOpts(
                    name="差值(亿元)",
                    type_="value",
                    axislabel_opts=opts.LabelOpts(formatter="{value}亿")
                ),
                tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross")
            )
            
            # 使用Grid将两个图表垂直排列
            grid = Grid(init_opts=opts.InitOpts(width="100%", height="650px"))
            grid.add(
                line_chart,
                grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", pos_top="15%", pos_bottom="55%")
            )
            grid.add(
                bar_chart,
                grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", pos_top="60%", pos_bottom="8%")
            )
            
            html_content = grid.render_embed()
            print(f"✅ 市场量能图绘制完成")
            return html_content
            
        except Exception as e:
            print(f"❌ 绘制市场量能图失败: {e}")
            import traceback
            traceback.print_exc()
            return f"<div>绘制市场量能图失败: {str(e)}</div>"

    @staticmethod 
    def get_market_volume_chart_options(current_data: pl.DataFrame, previous_data: pl.DataFrame,
                                      comparison_data: Dict) -> Dict:
        """生成市场量能图的ECharts配置
        
        Args:
            current_data: 当日分钟成交额数据
            previous_data: 前日分钟成交额数据  
            comparison_data: 对比统计数据
            
        Returns:
            ECharts配置字典
        """
        try:
            print(f"🎨 开始生成市场量能图ECharts配置...")
            
            # 处理时间轴数据
            current_times = []
            current_volumes = []
            previous_volumes = []
            volume_diff = []
            
            # 获取当日数据
            current_dict = current_data.to_dicts()
            previous_dict = previous_data.to_dicts()
            
            # 兼容累计列命名差异
            cum_cols = ("累计总成交额", "总累计成交额")
            total_col = "总成交额"

            # 创建时间->成交额的映射
            previous_volume_map = {}
            for row in previous_dict:
                time_str = row['时间']
                # 处理datetime对象或字符串
                if hasattr(time_str, 'strftime'):
                    # datetime对象
                    time_part = time_str.strftime('%H:%M')
                elif isinstance(time_str, str):
                    # 字符串
                    if ' ' in time_str:
                        time_part = time_str.split(' ')[1][:5]  # HH:MM
                    else:
                        time_part = time_str[:5] if len(time_str) >= 5 else time_str
                else:
                    continue
                # 优先使用累计成交额
                previous_raw = None
                for c in cum_cols:
                    previous_raw = row.get(c)
                    if previous_raw is not None:
                        break
                if previous_raw is None:
                    previous_raw = row.get(total_col, 0)
                previous_vol = IndexVisualizer._safe_float(previous_raw, 0.0)
                previous_volume_map[time_part] = previous_vol
            
            # 处理当日数据并计算差值
            for row in current_dict:
                time_str = row['时间']
                # 处理datetime对象或字符串
                if hasattr(time_str, 'strftime'):
                    # datetime对象
                    time_part = time_str.strftime('%H:%M')
                elif isinstance(time_str, str):
                    # 字符串
                    if ' ' in time_str:
                        time_part = time_str.split(' ')[1][:5]  # HH:MM
                    else:
                        time_part = time_str[:5] if len(time_str) >= 5 else time_str
                else:
                    continue
                
                # 优先使用累计成交额
                current_raw = None
                for c in cum_cols:
                    current_raw = row.get(c)
                    if current_raw is not None:
                        break
                if current_raw is None:
                    current_raw = row.get(total_col, 0)
                current_vol = IndexVisualizer._safe_float(current_raw, 0.0)
                previous_vol = previous_volume_map.get(time_part, 0)
                
                current_times.append(time_part)
                current_volumes.append(round(current_vol, 2))
                previous_volumes.append(round(previous_vol, 2))
                volume_diff.append(round(current_vol - previous_vol, 2))
            
            if not current_times:
                return None
            
            # 生成ECharts配置（确保时间轴升序且不启用缩放）
            try:
                combined = list(zip(current_times, current_volumes, previous_volumes, volume_diff))
                combined.sort(key=lambda x: x[0])
                current_times, current_volumes, previous_volumes, volume_diff = [list(t) for t in zip(*combined)] if combined else ([], [], [], [])
            except Exception:
                pass
            echarts_option = {
                'title': [
                    {
                        'text': '市场量能对比',
                        'subtext': f"今日累计: {comparison_data['current_total']:.2f}亿 | 昨日累计: {comparison_data['previous_total']:.2f}亿 | 变化: {comparison_data['change_amount']:.2f}亿({comparison_data['change_pct']:.2f}%)",
                        'left': 'center',
                        'top': '2%'
                    },
                    {
                        'text': '成交额差分',
                        'left': 'center',
                        'top': '55%',
                        'textStyle': {'fontSize': 14}
                    }
                ],
                'tooltip': {
                    'trigger': 'axis',
                    'axisPointer': {'type': 'cross'}
                },
                'legend': {
                    'data': ['今日累计成交额', '昨日累计成交额', '成交额差值'],
                    'show': False
                },
                'grid': [
                    {
                        'left': '10%',
                        'right': '8%',
                        'top': '15%',
                        'bottom': '55%'
                    },
                    {
                        'left': '10%', 
                        'right': '8%',
                        'top': '60%',
                        'bottom': '5%'
                    }
                ],
                'xAxis': [
                    {
                        'type': 'category',
                        'data': current_times,
                        'axisLabel': {'rotate': 45, 'fontSize': 10}
                    },
                    {
                        'type': 'category',
                        'gridIndex': 1,
                        'data': current_times,
                        'axisLabel': {'rotate': 45, 'fontSize': 10}
                    }
                ],
                'yAxis': [
                    {
                        'type': 'value',
                        'name': '成交额(亿元)',
                        'axisLabel': {'formatter': '{value}亿'}
                    },
                    {
                        'type': 'value',
                        'gridIndex': 1,
                        'name': '差值(亿元)',
                        'axisLabel': {'formatter': '{value}亿'}
                    }
                ],
                # 不启用 dataZoom，完整展示时间轴
                'series': [
                    {
                        'name': '今日累计成交额',
                        'type': 'line',
                        'data': current_volumes,
                        'symbol': 'none',
                        'lineStyle': {'width': 2, 'color': '#e74c3c'},
                        'smooth': True
                    },
                    {
                        'name': '昨日累计成交额',
                        'type': 'line',
                        'data': previous_volumes,
                        'symbol': 'none',
                        'lineStyle': {'width': 2, 'color': '#95a5a6'},
                        'smooth': True
                    },
                    {
                        'name': '成交额差值',
                        'type': 'bar',
                        'xAxisIndex': 1,
                        'yAxisIndex': 1, 
                        'data': [
                            {
                                'value': diff,
                                'itemStyle': {
                                    'color': '#ef232a' if diff > 0 else '#14b143'  # 差额大于0红色，小于0绿色
                                }
                            } for diff in volume_diff
                        ]
                    }
                ]
            }
            
            print(f"✅ 市场量能图ECharts配置生成完成")
            return echarts_option
            
        except Exception as e:
            print(f"❌ 生成市场量能图ECharts配置失败: {e}")
            import traceback
            traceback.print_exc()
            return None
