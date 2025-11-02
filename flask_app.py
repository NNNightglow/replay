#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票分析系统 - Flask后端应用
"""

import os
import sys
import warnings
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np
import polars as pl

# 屏蔽pandas警告
warnings.filterwarnings('ignore')
pd.set_option('mode.chained_assignment', None)

# 配置pyecharts CDN为更可靠的CDN
from pyecharts.globals import CurrentConfig
CurrentConfig.ONLINE_HOST = "https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/"

# 禁用系统代理
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''
# 导入Flask相关模块
from flask import Flask, request, jsonify, render_template_string
# 使用手动CORS配置，不依赖flask_cors包

# 导入原项目的核心模块
from utils.data_fetcher import (
    create_data_fetcher, DataFetcher,
)
from utils.analyzer import MarketAnalyzer
from utils.visualizer_manager import VisualizerManager
from utils.visualizers.index_visualizer import IndexVisualizer
from utils.levels import (
    compute_key_levels_from_market_states,
    read_levels_cache,
    write_levels_cache,
    DEFAULT_CACHE_PATH,
)
import re
from bs4 import BeautifulSoup

# 创建Flask应用
app = Flask(__name__)

def validate_date_parameter(date_param):
    """
    统一的日期参数验证函数
    
    Args:
        date_param: 从request.args.get('date')获取的日期参数
        
    Returns:
        tuple: (is_valid, processed_date, error_message)
        - is_valid: 是否为有效日期
        - processed_date: 处理后的日期对象或None
        - error_message: 错误信息（如果有）
    """
    if not date_param:
        return True, None, None
    
    # 去除空格
    date_param = date_param.strip()
    
    # 如果是空字符串，返回None
    if not date_param:
        return True, None, None
    
    # 如果是无效值，返回None但不报错（静默忽略）
    if date_param.lower() in ['invalid', 'null', 'undefined', 'none']:
        print(f"⚠️ 忽略无效日期参数: {date_param}，使用最新数据")
        return True, None, None
    
    # 尝试解析日期
    try:
        # 支持 YYYY-MM-DD 格式
        if '-' in date_param and len(date_param) == 10:
            date_obj = datetime.strptime(date_param, '%Y-%m-%d').date()
            return True, date_obj, None
        # 支持 YYYYMMDD 格式
        elif len(date_param) == 8 and date_param.isdigit():
            date_obj = datetime.strptime(date_param, '%Y%m%d').date()
            return True, date_obj, None
        else:
            return False, None, f'日期格式错误: {date_param}，请使用 YYYY-MM-DD 或 YYYYMMDD 格式'
    except ValueError as e:
        return False, None, f'日期格式错误: {date_param}，错误: {str(e)}'

# 手动添加CORS支持
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

def extract_chart_content(html_content):
    """提取图表的div和script部分，去除完整HTML文档结构"""
    if not html_content or not isinstance(html_content, str):
        return html_content

    try:
        import re

        # 🔧 修复：优先检查连板天梯复合结构（在检查DOCTYPE之前）
        if 'ladder-chart-container' in html_content:
            print("🔧 DEBUG: 检测到连板天梯复合结构，特殊处理")

            # 连板天梯复合结构的特殊处理
            # 直接返回完整内容，因为这已经是我们需要的复合结构
            print(f"🔧 DEBUG: 连板天梯复合结构完整返回，长度: {len(html_content)}")
            return html_content

        # 如果不包含完整HTML文档，直接返回
        if '<!DOCTYPE html>' not in html_content:
            print("🔧 DEBUG: 非完整HTML文档，直接返回")
            return html_content

        print("🔧 DEBUG: 检测到完整HTML文档，提取body内容")

        # 使用正则表达式提取body内容
        # 提取body标签内的内容
        body_match = re.search(r'<body[^>]*>(.*?)</body>', html_content, re.DOTALL)
        if body_match:
            body_content = body_match.group(1).strip()
            print(f"🔧 DEBUG: 提取body内容，长度: {len(body_content)}")

            # 如果body内容不为空，返回body内容
            if body_content:
                return body_content

        print("🔧 DEBUG: 无法提取body，尝试提取div和script")

        # 如果无法提取body，尝试提取div和script
        # 2. 通用div提取（支持id或class属性）
        div_pattern = r'<div[^>]*(?:id="[^"]*"|class="[^"]*")[^>]*>.*?</div>'
        script_pattern = r'<script[^>]*>.*?</script>'

        divs = re.findall(div_pattern, html_content, re.DOTALL)
        scripts = re.findall(script_pattern, html_content, re.DOTALL)

        print(f"🔧 DEBUG: 找到div数: {len(divs)}, script数: {len(scripts)}")

        # 过滤出包含echarts或showChart的script
        chart_scripts = [script for script in scripts if 'echarts.init' in script or 'showChart' in script]
        print(f"🔧 DEBUG: 图表script数: {len(chart_scripts)}")

        if divs and chart_scripts:
            result_parts = divs + chart_scripts
            result = '\n'.join(result_parts)
            print(f"🔧 DEBUG: 组合div和script，最终长度: {len(result)}")
            return result

        # 如果都失败了，返回原内容
        print("🔧 DEBUG: 所有提取方法失败，返回原内容")
        return html_content

    except Exception as e:
        print(f"提取图表内容失败: {str(e)}")
        return html_content

# 全局变量 - 优化后只保留必要的变量
data_fetcher = None
market_analyzer = None
market_states = None
stock_data = None
stock_metadata = None  # 添加股票元数据全局变量
index_data = None
market_metadata = None
sector_data = None


def apply_stock_filters(market_states_data: pl.DataFrame,
                       include_st: bool = False,
                       include_main_board: bool = True,
                       include_20cm: bool = True,
                       include_30cm: bool = True) -> pl.DataFrame:
    """应用股票筛选条件，与Streamlit版本保持一致"""
    if market_states_data is None or market_states_data.is_empty():
        return market_states_data

    filtered_data = market_states_data

    # ST股票筛选
    if not include_st:
        filtered_data = filtered_data.filter(
            ~pl.col('名称').str.contains("ST", literal=False)
        )

    # 股票类型筛选
    stock_conditions = []
    symbol_col = '代码'

    if include_main_board:
        # 主板股票：通常以000、001、002、600、601、603、605开头
        main_board_condition = (
            pl.col(symbol_col).str.starts_with("00") |
            pl.col(symbol_col).str.starts_with("60")
        )
        stock_conditions.append(main_board_condition)

    if include_20cm:
        # 20CM股票：通常是创业板（30开头）和科创板（68开头）
        cm20_condition = (
            pl.col(symbol_col).str.starts_with("30") |
            pl.col(symbol_col).str.starts_with("68")
        )
        stock_conditions.append(cm20_condition)

    if include_30cm:
        # 30CM股票：通常是北交所（430、830开头）
        cm30_condition = (
            pl.col(symbol_col).str.starts_with("4") |
            pl.col(symbol_col).str.starts_with("8") |
            pl.col(symbol_col).str.starts_with("9")
        )
        stock_conditions.append(cm30_condition)

    # 如果有筛选条件，应用到数据上
    if stock_conditions:
        # 使用OR逻辑连接所有条件（因为用户可能选择多个类型）
        combined_condition = stock_conditions[0]
        for condition in stock_conditions[1:]:
            combined_condition = combined_condition | condition

        # 应用股票类型筛选
        filtered_data = filtered_data.filter(combined_condition)

    return filtered_data

def init_system():
    """初始化系统组件 - 优化版本，避免重复初始化"""
    global data_fetcher, market_analyzer, market_states
    global stock_data, index_data, market_metadata, sector_data

    try:
        # 1. 初始化DataFetcher（包含所有数据管理器）
        print("🚀 初始化DataFetcher...")
        data_fetcher = DataFetcher()

        # 2. 执行自动元数据更新检查
        print("🔄 检查并更新元数据...")
        try:
            data_fetcher.check_and_update_metadata(
                progress_callback=lambda current, total, message: print(f"  📊 [{current}/{total}] {message}")
            )
            print("✅ 元数据更新检查完成")
        except Exception as e:
            print(f"⚠️ 元数据更新检查失败: {e}")
            print("继续使用现有数据...")

        # 3. 通过DataFetcher的内部管理器加载数据（避免重复创建）
        print("正在加载股票元数据...")
        global stock_metadata
        stock_metadata = data_fetcher.stock_metadata_manager.load_metadata()
        if stock_metadata is None or stock_metadata.is_empty():
            print("⚠️ 未能加载股票元数据")
        else:
            print(f"✅ 成功加载股票元数据: {stock_metadata.height} 条记录")
            print(f"股票元数据列: {stock_metadata.columns[:10]}...")
            print(f"股票元数据完整列: {stock_metadata.columns}")

        print("正在加载指数元数据...")
        index_data = data_fetcher.index_metadata_manager.load_metadata()
        if index_data is None or index_data.is_empty():
            print("⚠️ 未能加载指数元数据")
        else:
            print(f"✅ 成功加载指数元数据: {index_data.height} 条记录")
            print(f"指数元数据列: {index_data.columns[:10]}...")
            if '名称' in index_data.columns:
                unique_indices = index_data['名称'].unique()
                print(f"可用指数数量: {len(unique_indices)}")
                print(f"指数示例: {unique_indices[:5].to_list()}...")

        print("正在加载市场元数据...")
        market_metadata = data_fetcher.market_metadata_manager.load_metadata()
        if market_metadata is None or market_metadata.is_empty():
            print("⚠️ 未能加载市场元数据")
        else:
            print(f"✅ 成功加载市场元数据: {market_metadata.height} 条记录")
            print(f"市场元数据列: {market_metadata.columns}")

        print("正在加载市场状态数据...")
        market_states = data_fetcher.market_metadata_manager.load_market_states()
        if market_states is None or market_states.is_empty():
            print("⚠️ 未能加载市场状态数据")
        else:
            print(f"✅ 成功加载市场状态数据: {market_states.height} 条记录")
            print(f"市场状态数据列: {market_states.columns[:10]}...")

        # 5. 加载统一板块数据（行业+概念）
        print("正在加载板块数据...")
        sector_data = data_fetcher.sector_data_manager.load_sector_data(include_sectors=True, include_concepts=True)

        if sector_data is None or sector_data.is_empty():
            print("⚠️ 未能加载板块数据")
        else:
            print(f"✅ 成功加载统一板块数据: {sector_data.height} 条记录")

            # 确保板块数据包含关键技术指标（含30/90日涨跌幅）
            required_cols = {'涨跌幅', '5日涨跌幅', '10日涨跌幅', '30日涨跌幅', '90日涨跌幅'}
            if any(col not in sector_data.columns for col in required_cols):
                print("📊 为板块数据添加/补全技术指标(含30/90日涨跌幅)...")
                sector_data = data_fetcher.sector_data_manager._calculate_technical_indicators(sector_data)
                print(f"✅ 技术指标添加完成，数据行数: {sector_data.height}")

            # 显示板块类型分布
            if '板块类型' in sector_data.columns:
                type_stats = sector_data.group_by("板块类型").agg([
                    pl.count().alias("数量")
                ])
                print(f"板块类型分布: {type_stats.to_dicts()}")

        # 6. 初始化分析器和可视化器
        market_analyzer = MarketAnalyzer

        print("✅ 系统初始化完成")
        return True

    except Exception as e:
        print(f"❌ 系统初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return False


@app.route('/api/admin/update/<string:target>', methods=['POST'])
def manual_data_update(target):
    """手动触发数据更新，弥补自动更新失败的情况"""
    global data_fetcher, stock_metadata, index_data, sector_data, market_states, market_metadata

    if data_fetcher is None:
        init_success = init_system()
        if not init_success or data_fetcher is None:
            return jsonify({
                'success': False,
                'message': '系统初始化失败，无法执行手动更新',
                'timestamp': datetime.now().isoformat()
            })

    normalized_target = target.lower().replace('_', '-').strip()
    start_time = datetime.now()
    update_success = False
    message = ''

    try:
        if normalized_target == 'stocks':
            update_success = data_fetcher.stock_metadata_manager.update_metadata()
            if update_success:
                stock_metadata = data_fetcher.stock_metadata_manager.load_metadata()
                message = '股票数据更新成功'
            else:
                message = '股票数据更新失败'
        elif normalized_target == 'sectors':
            update_success = data_fetcher.sector_data_manager.update_sector_data()
            if update_success:
                sector_data = data_fetcher.sector_data_manager.load_sector_data(
                    include_sectors=True,
                    include_concepts=True
                )
                message = '板块数据更新成功'
            else:
                message = '板块数据更新失败'
        elif normalized_target == 'indices':
            update_success = data_fetcher.index_metadata_manager.update_metadata()
            if update_success:
                index_data = data_fetcher.index_metadata_manager.load_metadata()
                message = '指数数据更新成功'
            else:
                message = '指数数据更新失败'
        elif normalized_target in ('market-states', 'marketstates', 'stock-states', 'stockstates'):
            update_success = data_fetcher.market_metadata_manager.update_metadata()
            if update_success:
                market_states = data_fetcher.market_metadata_manager.load_market_states()
                market_metadata = data_fetcher.market_metadata_manager.load_metadata()
                message = '股票状态数据更新成功'
            else:
                message = '股票状态数据更新失败'
        else:
            return jsonify({
                'success': False,
                'message': f'未知的更新类型: {target}',
                'timestamp': datetime.now().isoformat()
            })

        duration = round((datetime.now() - start_time).total_seconds(), 2)

        response_payload = {
            'success': update_success,
            'message': message,
            'updated_type': normalized_target,
            'duration_seconds': duration,
            'timestamp': datetime.now().isoformat()
        }

        if not update_success:
            response_payload['detail'] = '请检查日志获取更多信息'

        return jsonify(response_payload)

    except Exception as e:
        print(f"手动更新 {target} 时出错: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'message': f'手动更新 {target} 失败: {str(e)}',
            'updated_type': normalized_target,
            'timestamp': datetime.now().isoformat()
        })


@app.route('/')
def index():
    """主页面"""
    return render_template_string('''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>股票分析系统 - Flask后端</title>
        <style>
            body {
                font-family: 'Microsoft YaHei', Arial, sans-serif;
                margin: 0;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                color: white;
            }
            .container {
                max-width: 800px;
                margin: 0 auto;
                background: rgba(255, 255, 255, 0.1);
                padding: 30px;
                border-radius: 15px;
                backdrop-filter: blur(10px);
                box-shadow: 0 8px 32px rgba(31, 38, 135, 0.37);
            }
            h1 {
                text-align: center;
                margin-bottom: 30px;
                font-size: 2.5em;
                text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            }
            .status {
                text-align: center;
                padding: 15px;
                background: rgba(76, 175, 80, 0.2);
                border-radius: 10px;
                margin: 20px 0;
            }
            .api-section {
                margin: 20px 0;
                padding: 20px;
                background: rgba(255, 255, 255, 0.1);
                border-radius: 10px;
                border-left: 4px solid #4CAF50;
            }
            .api-section h2 {
                margin-top: 0;
                color: #4CAF50;
            }
            .api-list {
                list-style: none;
                padding: 0;
            }
            .api-list li {
                margin: 10px 0;
                padding: 10px;
                background: rgba(255, 255, 255, 0.1);
                border-radius: 5px;
                font-family: 'Courier New', monospace;
            }
            .method {
                color: #FFD700;
                font-weight: bold;
            }
            .endpoint {
                color: #87CEEB;
            }
            .description {
                color: #98FB98;
                font-style: italic;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🚀 股票分析系统 Flask后端</h1>
            
            <div class="status">
                <h2>✅ 后端服务运行中</h2>
                <p>Flask API服务已启动，端口: 5000</p>
                <p>前端访问地址: <a href="http://localhost:8080" style="color: #FFD700;">http://localhost:8080</a></p>
                <p>📁 数据缓存: 与Streamlit共享data_cache目录</p>
            </div>

            <div class="api-section">
                <h2>📊 可用API接口</h2>
                <ul class="api-list">
                    <li><span class="method">GET</span> <span class="endpoint">/api/market/overview</span> - <span class="description">市场概览</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/market/sentiment</span> - <span class="description">市场情绪数据</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/market/metadata</span> - <span class="description">市场元数据</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/market/sentiment/charts</span> - <span class="description">市场情绪图表（支持股票筛选）</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/market/latest</span> - <span class="description">最新交易日数据</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/market/metadata/export</span> - <span class="description">导出市场元数据</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/market/sectors</span> - <span class="description">行业板块分析</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/market/sectors/charts</span> - <span class="description">行业板块图表</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/market/concepts</span> - <span class="description">概念板块分析</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/market/concepts/charts</span> - <span class="description">概念板块图表</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/market/indices</span> - <span class="description">指数数据</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/indices/analysis</span> - <span class="description">指数分析（北交所微盘股）</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/indices/kline</span> - <span class="description">多指数K线图</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/indices/available</span> - <span class="description">可用指数列表</span></li>
                    <li><span class="method">POST</span> <span class="endpoint">/api/stocks/new-high</span> - <span class="description">新高股票</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/stocks/{code}/kline</span> - <span class="description">K线数据</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/analysis/heima</span> - <span class="description">黑马分析</span></li>
                    <li><span class="method">POST</span> <span class="endpoint">/api/analysis/baima</span> - <span class="description">白马分析</span></li>
                    <li><span class="method">GET</span> <span class="endpoint">/api/system/status</span> - <span class="description">系统状态</span></li>
                </ul>
            </div>

            <div style="text-align: center; margin-top: 30px; opacity: 0.8;">
                <p>🎯 Vue.js + Flask 架构 | 💡 使用replay虚拟环境</p>
                <p>📂 共享data_cache目录，避免重复数据</p>
            </div>
        </div>
    </body>
    </html>
    ''')

@app.route('/api/market/overview')
def market_overview():
    """获取市场概览数据"""
    try:
        # 获取市场情绪数据
        sentiment_data = data_fetcher.get_market_sentiment()
        if sentiment_data is None:
            return jsonify({
                'success': False,
                'error': '无法获取市场情绪数据',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 分析市场情绪
        analyzed_data = MarketAnalyzer.analyze_market_sentiment(sentiment_data)

        return jsonify({
            'success': True,
            'data': analyzed_data,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/stocks/levels', methods=['GET'])
def get_stock_levels():
    """获取个股关键位（带Parquet缓存）
    params:
      - code: 股票代码（必填，6位）
      - date: 日期(YYYY-MM-DD)，缺省为最新交易日（以全局market_states最大日期推断）
      - window: 窗口天数，默认3650（近十年）
      - method_ver: 算法版本，默认'v1'
    """
    try:
        stock_code = request.args.get('code')
        if not stock_code:
            return jsonify({'success': False, 'message': '缺少参数 code'}), 400

        window_days = int(request.args.get('window', 3650))
        method_ver = request.args.get('method_ver', 'v1')
        date_param = request.args.get('date')

        # 解析日期，默认取market_states最大日期
        if date_param:
            ok, selected_date, err = validate_date_parameter(date_param)
            if not ok:
                return jsonify({'success': False, 'message': err}), 400
            if selected_date is None:
                if market_states is None or market_states.is_empty():
                    selected_date = datetime.now().date()
                else:
                    selected_date = market_states.select(pl.col('日期').max()).to_series()[0]
        else:
            if market_states is None or market_states.is_empty():
                selected_date = datetime.now().date()
            else:
                selected_date = market_states.select(pl.col('日期').max()).to_series()[0]

        # 尝试读取缓存
        cache_df = read_levels_cache(DEFAULT_CACHE_PATH)
        date_str = selected_date.strftime('%Y-%m-%d') if hasattr(selected_date, 'strftime') else str(selected_date)
        cached = cache_df.filter(
            (pl.col('code') == str(stock_code).zfill(6)) &
            (pl.col('date') == date_str) &
            (pl.col('window_days') == window_days) &
            (pl.col('method_ver') == method_ver)
        )

        if not cached.is_empty():
            row = cached.to_dicts()[0]
            levels_value = row.get('levels')
            if isinstance(levels_value, str):
                import json
                try:
                    levels_parsed = json.loads(levels_value)
                except Exception:
                    levels_parsed = []
            else:
                levels_parsed = levels_value
            return jsonify({
                'success': True,
                'data': {
                    'code': row.get('code'),
                    'date': row.get('date'),
                    'window_days': int(row.get('window_days', window_days)),
                    'method_ver': row.get('method_ver', method_ver),
                    'levels': levels_parsed or [],
                    'ath': row.get('ath'),
                    'current': row.get('current')
                },
                'cached': True,
                'timestamp': datetime.now().isoformat()
            })

        # 未命中缓存 -> 计算
        if market_states is None or market_states.is_empty():
            return jsonify({'success': False, 'message': '无法获取市场状态数据'}), 500

        result = compute_key_levels_from_market_states(
            market_states=market_states,
            code=str(stock_code),
            selected_date=selected_date,
            window_days=window_days,
            method_ver=method_ver
        )

        # 写入缓存（levels以JSON字符串存储）
        import json
        write_levels_cache({
            'code': result['code'],
            'date': result['date'],
            'window_days': result['window_days'],
            'method_ver': result['method_ver'],
            'levels': json.dumps(result.get('levels', []), ensure_ascii=False),
            'ath': result.get('ath'),
            'current': result.get('current'),
            'updated_at': datetime.now().isoformat()
        }, DEFAULT_CACHE_PATH)

        return jsonify({
            'success': True,
            'data': result,
            'cached': False,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

def get_market_sentiment_from_metadata(date_str):
    """从market_metadata获取市场情绪数据"""
    try:
        # 转换日期格式
        if '-' in date_str:
            target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            target_date = datetime.strptime(date_str, '%Y%m%d').date()

        # 优先使用data_fetcher获取市场元数据
        current_market_metadata = None

        # 直接使用全局的market_metadata数据
        if market_metadata is None or market_metadata.is_empty():
            print(f"❌ market_metadata未初始化或为空")
            return None

        current_market_metadata = market_metadata

        # 筛选指定日期的数据
        target_data = current_market_metadata.filter(pl.col('日期') == target_date)

        if target_data.height == 0:
            print(f"❌ 未找到日期 {target_date} 的market_metadata数据")
            # 尝试获取最近的数据
            latest_data = current_market_metadata.sort('日期', descending=True).head(1)
            if not latest_data.is_empty():
                print(f"⚠️ 使用最新日期的数据: {latest_data['日期'][0]}")
                target_data = latest_data
            else:
                return None

        # 转换为字典格式
        data = target_data.to_dicts()[0]

        # 构造返回数据格式（兼容原有格式）
        result = {
            'red_ratio': round(data.get('红盘率', 0), 2),
            'up_count': data.get('上涨股票数', 0),
            'down_count': data.get('总股票数', 0) - data.get('上涨股票数', 0),
            'flat_count': 0,  # market_metadata中没有平盘数据
            'total_count': data.get('总股票数', 0),
            'limit_up_count': data.get('涨停数', 0),
            'limit_down_count': data.get('跌停数', 0),
            'blown_count': data.get('炸板数', 0),
            'total_amount': data.get('成交总额', 0),
            'date': target_date.strftime('%Y%m%d')
        }

        print(f"✅ 从market_metadata获取到 {target_date} 的数据: 红盘率{result['red_ratio']}%, 总股票数{result['total_count']}")
        return result

    except Exception as e:
        print(f"❌ 从market_metadata获取数据失败: {e}")
        return None

@app.route('/api/market/sentiment')
def market_sentiment():
    """获取市场情绪数据"""
    try:
        # 获取日期参数
        date = request.args.get('date')

        # 转换日期格式：从 YYYY-MM-DD 转换为 YYYYMMDD
        if date and '-' in date:
            date_for_akshare = date.replace('-', '')
            date_for_metadata = date
        else:
            date_for_akshare = date
            date_for_metadata = f"{date[:4]}-{date[4:6]}-{date[6:8]}" if date else None

        # 从market_metadata获取数据
        sentiment_data = None
        if date_for_metadata:
            sentiment_data = get_market_sentiment_from_metadata(date_for_metadata)


        if sentiment_data is None:
            return jsonify({
                'success': False,
                'error': '无法获取市场情绪数据',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 获取上个交易日数据用于对比
        previous_data = None
        try:
            # 获取当前日期
            from datetime import datetime as dt, timedelta
            if date_for_metadata:
                current_date = dt.strptime(date_for_metadata, '%Y-%m-%d').date()
            else:
                current_date = dt.now().date()

            # 从全局market_metadata中查找上个交易日
            if market_metadata is not None and not market_metadata.is_empty():
                # 获取所有小于当前日期的交易日，按日期降序排列
                previous_dates = market_metadata.filter(
                    pl.col('日期') < current_date
                ).sort('日期', descending=True)

                if previous_dates.height > 0:
                    # 获取最近的交易日数据
                    prev_date = previous_dates.row(0, named=True)['日期']
                    prev_date_str = prev_date.strftime('%Y-%m-%d')

                    # 获取上个交易日的完整数据
                    prev_sentiment_data = get_market_sentiment_from_metadata(prev_date_str)
                    if prev_sentiment_data:
                        previous_data = prev_sentiment_data
                        print(f"🔧 DEBUG: 获取到上个交易日({prev_date})数据用于对比")

        except Exception as e:
            print(f"🔧 DEBUG: 获取上个交易日数据失败: {str(e)}")

        # 分析市场情绪（包含对比数据）
        print(f"🔧 DEBUG: sentiment_data内容: {sentiment_data}")
        import sys
        sys.stdout.flush()

        # 如果sentiment_data来自我们的market_metadata函数，直接使用并计算对比
        if sentiment_data and 'red_ratio' in sentiment_data and sentiment_data['red_ratio'] > 0:
            print(f"🔧 DEBUG: 使用market_metadata数据，计算与上个交易日的对比")
            analyzed_data = sentiment_data.copy()

            # 计算与上个交易日的变化
            changes = {
                'limit_up_change': 0,
                'limit_down_change': 0,
                'red_ratio_change': 0,
                'total_amount_change': 0,
                'total_amount_change_pct': 0
            }

            if previous_data:
                print(f"🔧 DEBUG: 计算对比数据，当前: {sentiment_data['red_ratio']}%, 上个交易日: {previous_data.get('red_ratio', 0)}%")

                # 计算各项变化
                current_limit_up = sentiment_data.get('limit_up_count', 0)
                previous_limit_up = previous_data.get('limit_up_count', 0)
                changes['limit_up_change'] = current_limit_up - previous_limit_up

                current_limit_down = sentiment_data.get('limit_down_count', 0)
                previous_limit_down = previous_data.get('limit_down_count', 0)
                changes['limit_down_change'] = current_limit_down - previous_limit_down

                current_red_ratio = sentiment_data.get('red_ratio', 0)
                previous_red_ratio = previous_data.get('red_ratio', 0)
                changes['red_ratio_change'] = round(current_red_ratio - previous_red_ratio, 2)

                # 计算成交额变化
                current_amount = sentiment_data.get('total_amount', 0)
                previous_amount = previous_data.get('total_amount', 0)
                changes['total_amount_change'] = round(current_amount - previous_amount, 2)

                if previous_amount > 0:
                    changes['total_amount_change_pct'] = round((current_amount - previous_amount) / previous_amount * 100, 2)

                print(f"🔧 DEBUG: 详细对比计算:")
                print(f"  涨停: {current_limit_up} - {previous_limit_up} = {changes['limit_up_change']}")
                print(f"  跌停: {current_limit_down} - {previous_limit_down} = {changes['limit_down_change']}")
                print(f"  红盘率: {current_red_ratio}% - {previous_red_ratio}% = {changes['red_ratio_change']}%")
                print(f"  成交额: {current_amount:.2f} - {previous_amount:.2f} = {changes['total_amount_change']:.2f}亿")
                print(f"🔧 DEBUG: 最终计算得到的变化: {changes}")
            else:
                print(f"🔧 DEBUG: 没有上个交易日数据，无法计算对比")

            # 添加兼容字段
            analyzed_data.update({
                'strong_stocks_count': 0,
                'previous_limit_up_count': 0,
                'break_limit_up_count': analyzed_data.get('blown_count', 0),
                'big_deal_count': 0,
                'break_ratio': 0,
                'changes': changes
            })
        else:
            # 使用原有的分析函数
            analyzed_data = MarketAnalyzer.analyze_market_sentiment(sentiment_data, previous_data)

        print(f"🔧 DEBUG: analyzed_data内容: {analyzed_data}")

        # 添加涨跌幅分布数据（使用market_states数据）
        if market_states is not None and not market_states.is_empty():
            # 转换日期格式进行筛选
            if date_for_metadata:
                try:
                    from datetime import datetime as dt
                    target_date_obj = dt.strptime(date_for_metadata, '%Y-%m-%d').date()

                    # 筛选指定日期的数据
                    daily_data = market_states.filter(pl.col('日期') == target_date_obj)

                    if not daily_data.is_empty():
                        # 计算涨跌幅分布
                        change_distribution = MarketAnalyzer._calculate_change_distribution(daily_data, '涨跌幅')
                        analyzed_data['change_distribution'] = change_distribution
                        print(f"🔧 DEBUG: 添加涨跌幅分布数据，总股票数: {change_distribution.get('total_count', 0)}")
                    else:
                        print(f"🔧 DEBUG: 未找到日期 {target_date} 的市场数据")
                except Exception as e:
                    print(f"🔧 DEBUG: 处理涨跌幅分布数据失败: {str(e)}")

        return jsonify({
            'success': True,
            'data': analyzed_data,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        print(f"获取市场情绪数据失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/market/change-distribution')
def market_change_distribution():
    """获取市场涨跌幅分布数据"""
    try:
        # 获取日期参数
        date = request.args.get('date')

        if market_states is None or market_states.is_empty():
            return jsonify({
                'success': False,
                'error': '市场数据未加载',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 转换日期格式
        if date:
            if '-' in date:
                # YYYY-MM-DD 格式
                target_date = date
            else:
                # YYYYMMDD 格式，转换为 YYYY-MM-DD
                target_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        else:
            # 使用最新日期
            target_date = market_states['日期'].max().strftime('%Y-%m-%d')

        try:
            from datetime import datetime as dt
            target_date_obj = dt.strptime(target_date, '%Y-%m-%d').date()

            # 筛选指定日期的数据
            daily_data = market_states.filter(pl.col('日期') == target_date_obj)

            if daily_data.is_empty():
                return jsonify({
                    'success': False,
                    'error': f'未找到日期 {target_date} 的市场数据',
                    'timestamp': datetime.now().isoformat()
                }), 404

            # 计算涨跌幅分布
            change_distribution = MarketAnalyzer._calculate_change_distribution(daily_data, '涨跌幅')

            return jsonify({
                'success': True,
                'data': {
                    'date': target_date,
                    'distribution': change_distribution
                },
                'timestamp': datetime.now().isoformat()
            })

        except Exception as e:
            return jsonify({
                'success': False,
                'error': f'处理日期数据失败: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }), 500

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/market/metadata')
def market_metadata():
    """获取市场元数据分析"""
    try:
        # 获取日期参数
        date = request.args.get('date')
        days_back = int(request.args.get('days_back', 30))  # 默认30天

        # 获取市场元数据
        if data_fetcher is None:
            return jsonify({
                'success': False,
                'error': '数据获取器未初始化',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 获取市场元数据
        market_metadata = data_fetcher.market_metadata_manager.load_metadata()
        if market_metadata is None or market_metadata.is_empty():
            return jsonify({
                'success': False,
                'error': '无法获取市场元数据',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 按日期过滤数据
        if days_back > 0:
            # 获取最近N天的数据
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days_back)

            # 确保日期列存在
            date_col = '日期' if '日期' in market_metadata.columns else 'date'
            if date_col in market_metadata.columns:
                market_metadata = market_metadata.filter(
                    pl.col(date_col) >= pl.lit(start_date)
                ).sort(date_col, descending=True)

        # 转换为JSON格式
        metadata_dict = market_metadata.to_dicts()

        # 计算统计信息
        stats = {
            'total_days': len(metadata_dict),
            'avg_red_ratio': round(market_metadata.select('红盘率').mean().item(), 2) if '红盘率' in market_metadata.columns else 0,
            'avg_limit_up': round(market_metadata.select('涨停数').mean().item(), 2) if '涨停数' in market_metadata.columns else 0,
            'avg_limit_down': round(market_metadata.select('跌停数').mean().item(), 2) if '跌停数' in market_metadata.columns else 0,
            'avg_amount': round(market_metadata.select('成交总额').mean().item() / 100000000, 2) if '成交总额' in market_metadata.columns else 0,  # 转换为亿元
        }

        return jsonify({
            'success': True,
            'data': {
                'metadata': metadata_dict,
                'stats': stats
            },
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        print(f"获取市场元数据失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/market/sentiment/charts')
def market_sentiment_charts():
    """获取市场情绪图表 ，支持股票筛选"""
    try:
        print("🔧 DEBUG: API /api/market/sentiment/charts 被调用", flush=True)
        # 获取参数
        date = request.args.get('date')
        days_back = int(request.args.get('days_back', 30))
        chart_type = request.args.get('chart_type', 'all')  # all, red_ratio, limit_counts, break_counts
        print(f"🔧 DEBUG: 参数 days_back={days_back}, chart_type={chart_type}")

        # 新增：股票筛选参数
        include_st = request.args.get('include_st', 'false').lower() == 'true'
        include_main_board = request.args.get('include_main_board', 'true').lower() == 'true'
        include_20cm = request.args.get('include_20cm', 'true').lower() == 'true'
        include_30cm = request.args.get('include_30cm', 'true').lower() == 'true'

        # 自定义日期范围
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')

        # 获取市场元数据
        if data_fetcher is None:
            return jsonify({
                'success': False,
                'error': '数据获取器未初始化',
                'timestamp': datetime.now().isoformat()
            }), 500

        market_metadata = data_fetcher.market_metadata_manager.load_metadata()
        if market_metadata is None or market_metadata.is_empty():
            return jsonify({
                'success': False,
                'error': '无法获取市场元数据',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 处理日期范围
        if start_date_str and end_date_str:
            # 使用自定义日期范围
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        elif date:
            # 如果提供了date参数，以该日期为中心计算范围
            if '-' in date:
                center_date = datetime.strptime(date, '%Y-%m-%d').date()
            else:
                center_date = datetime.strptime(f"{date[:4]}-{date[4:6]}-{date[6:8]}", '%Y-%m-%d').date()

            # 以选择的日期为结束日期，向前推days_back天
            end_date = center_date
            start_date = end_date - timedelta(days=days_back)
            print(f"🔧 DEBUG: 使用指定日期 {date}，日期范围: {start_date} 到 {end_date}")
        else:
            # 使用当前日期作为默认
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days_back)
            print(f"🔧 DEBUG: 使用默认日期范围: {start_date} 到 {end_date}")

        # 按日期过滤市场元数据
        date_col = '日期' if '日期' in market_metadata.columns else 'date'
        if date_col in market_metadata.columns:
            market_metadata = market_metadata.filter(
                (pl.col(date_col) >= pl.lit(start_date)) &
                (pl.col(date_col) <= pl.lit(end_date))
            ).sort(date_col, descending=True)

        # 获取并筛选market_states数据
        filtered_market_states = None
        if market_states is not None and not market_states.is_empty():
            # 先按日期过滤
            filtered_market_states = market_states.filter(
                (pl.col('日期') >= pl.lit(start_date)) &
                (pl.col('日期') <= pl.lit(end_date))
            )

            # 应用股票类型筛选
            filtered_market_states = apply_stock_filters(
                filtered_market_states,
                include_st, include_main_board, include_20cm, include_30cm
            )
            print(f"筛选后的市场状态数据: {filtered_market_states.height} 条记录")
        else:
            print("⚠️ 市场状态数据不可用，某些图表可能无法生成")

        # 生成完整的市场情绪图表
        charts = {}

        try:
            print("🔧 DEBUG: 开始生成市场元数据图表...", flush=True)
            # 使用VisualizerManager生成完整的市场元数据图表
            market_charts = VisualizerManager.plot_market_metadata(filtered_market_states, market_metadata)
            charts.update(market_charts)
            print(f"🔧 DEBUG: 市场元数据图表生成完成，包含: {list(market_charts.keys())}", flush=True)
        except Exception as e:
            print(f"🔧 DEBUG: 生成市场元数据图表失败: {str(e)}", flush=True)
            import traceback
            traceback.print_exc()

        # 添加涨跌幅分布图
        if date and filtered_market_states is not None:
            try:
                # 转换日期格式
                if '-' in date:
                    target_date = date
                else:
                    target_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"

                print(f"🔧 DEBUG: 处理涨跌幅分布图，目标日期: {target_date}")
                target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()

                # 筛选指定日期的数据
                daily_data = filtered_market_states.filter(pl.col('日期') == target_date_obj)
                print(f"🔧 DEBUG: 筛选到的数据行数: {daily_data.height}")

                if not daily_data.is_empty():
                    # 计算涨跌幅分布
                    change_distribution = MarketAnalyzer._calculate_change_distribution(daily_data, '涨跌幅')
                    print(f"🔧 DEBUG: 涨跌幅分布计算结果: {change_distribution.get('total_count', 0)} 只股票")

                    # 生成涨跌幅分布图
                    change_distribution_chart = VisualizerManager.plot_market_change_distribution(change_distribution.get('ranges', []))
                    charts['change_distribution'] = change_distribution_chart
                    print(f"🔧 DEBUG: 生成涨跌幅分布图成功，图表长度: {len(change_distribution_chart) if change_distribution_chart else 0}")
                else:
                    print(f"🔧 DEBUG: 未找到日期 {target_date} 的数据")
                    charts['change_distribution'] = "<div style='text-align:center; padding:50px; color:#666;'>📊 该日期无市场数据</div>"
            except Exception as e:
                print(f"🔧 DEBUG: 生成涨跌幅分布图失败: {str(e)}")
                import traceback
                traceback.print_exc()
                charts['change_distribution'] = f"<div style='text-align:center; padding:50px; color:#f56565;'>❌ 涨跌幅分布图生成失败: {str(e)}</div>"

        # 提取图表内容，去除完整HTML文档结构
        print(f"🔧 DEBUG: 生成的图表键名: {list(charts.keys())}")
        extracted_charts = {}
        for key, chart_html in charts.items():
            if chart_html:
                print(f"🔧 DEBUG: 处理图表 {key}, 原始长度: {len(chart_html)}")
                extracted_chart = extract_chart_content(chart_html)
                print(f"🔧 DEBUG: 处理后长度: {len(extracted_chart)}")
                extracted_charts[key] = extracted_chart
            else:
                extracted_charts[key] = "<div>图表生成失败</div>"

        return jsonify({
            'success': True,
            'data': {
                'charts': extracted_charts,
                'chart_type': chart_type,
                'days_back': days_back,
                'date_range': {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat()
                },
                'filters': {
                    'include_st': include_st,
                    'include_main_board': include_main_board,
                    'include_20cm': include_20cm,
                    'include_30cm': include_30cm
                }
            },
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        print(f"生成市场情绪图表失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/market/sectors')
def market_sectors():
    """获取合并的板块分析数据"""
    try:
        # 使用全局板块管理器获取数据
        if data_fetcher is None:
            return jsonify({
                'success': False,
                'error': '数据获取器未初始化',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 获取筛选参数
        include_sectors = request.args.get('include_sectors', 'true').lower() == 'true'
        include_concepts = request.args.get('include_concepts', 'true').lower() == 'true'
        days_back = int(request.args.get('days_back', 30))
        date_str = request.args.get('date')  # 获取指定日期参数

        # 获取行业和概念数据
        print(f"🔧 API调用参数: include_sectors={include_sectors}, include_concepts={include_concepts}, days_back={days_back}, date={date_str}")

        # 使用统一的方法获取板块数据
        combined_data = data_fetcher.get_combined_sectors_summary(
            date_str=date_str,
            include_sectors=include_sectors,
            include_concepts=include_concepts,
            days_back=days_back
        )

        # 调试信息
        if combined_data.get('top_sectors'):
            print(f"🔧 API返回数据: {len(combined_data['top_sectors'])} 个板块")
            if combined_data['top_sectors']:
                first_sector = combined_data['top_sectors'][0]
                print(f"🔧 第一个板块字段: {list(first_sector.keys())}")
                print(f"🔧 第一个板块类型: {first_sector.get('板块类型')}")
        else:
            print("🔧 API返回数据为空")

        return jsonify({
            'success': True,
            'data': combined_data,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        print(f"获取板块分析失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/market/sectors/kline')
def market_sectors_kline():
    """获取行业K线图 - 平均五日成交量前十的行业"""
    try:
        # 获取查询参数
        date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
        days_range = int(request.args.get('days_range', 30))
        index_name = request.args.get('index', None)

        print(f"🔧 生成行业K线图: date={date_str}, days={days_range}, index={index_name}")


        # 获取行业K线图
        kline_chart = VisualizerManager.plot_sector_kline(
            data_fetcher,
            date_str=date_str,
            days_range=days_range,
            index_name=index_name
        )

        return jsonify({
            'success': True,
            'data': {
                'chart_html': kline_chart,
                'date': date_str,
                'days_range': days_range,
                'index': index_name
            }
        })

    except Exception as e:
        print(f"❌ 生成行业K线图失败: {e}")
        import traceback
        traceback.print_exc()

        # 检查是否是polars相关错误
        error_msg = str(e)
        if "polars" in error_msg.lower() and "string" in error_msg.lower():
            print(f"🔧 DEBUG: polars版本相关错误: {error_msg}")

        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/market/sectors/custom-period')
def market_sectors_custom_period():
    """获取板块自定义区间涨跌幅"""
    try:
        # 获取查询参数
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        include_sectors = request.args.get('include_sectors', 'true').lower() == 'true'
        include_concepts = request.args.get('include_concepts', 'true').lower() == 'true'

        if not start_date or not end_date:
            return jsonify({
                'success': False,
                'error': '请提供开始日期和结束日期',
                'timestamp': datetime.now().isoformat()
            }), 400

        # 验证日期格式
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({
                'success': False,
                'error': '日期格式错误，请使用YYYY-MM-DD格式',
                'timestamp': datetime.now().isoformat()
            }), 400

        if start_dt >= end_dt:
            return jsonify({
                'success': False,
                'error': '开始日期必须早于结束日期',
                'timestamp': datetime.now().isoformat()
            }), 400

        # 检查日期范围
        diff_days = (end_dt - start_dt).days
        if diff_days > 365:
            return jsonify({
                'success': False,
                'error': '自定义区间不能超过1年',
                'timestamp': datetime.now().isoformat()
            }), 400

        if data_fetcher is None:
            return jsonify({
                'success': False,
                'error': '数据获取器未初始化',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 获取自定义区间板块涨跌幅数据
        custom_data = data_fetcher.get_sectors_custom_period(
            start_date=start_date,
            end_date=end_date,
            include_sectors=include_sectors,
            include_concepts=include_concepts
        )

        return jsonify({
            'success': True,
            'data': custom_data,
            'period': f"{start_date} 至 {end_date}",
            'days': diff_days,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        print(f"❌ 获取自定义区间板块数据失败: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/analysis/baima/intervals', methods=['GET'])
def get_baima_preset_intervals():
    """获取白马分析的预设时间区间"""
    try:
        end_date = datetime.now().date()

        preset_intervals = [
            {
                'name': '最近30天',
                'start_date': (end_date - timedelta(days=30)).strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'description': '短期表现分析'
            },
            {
                'name': '最近90天',
                'start_date': (end_date - timedelta(days=90)).strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'description': '季度表现分析'
            },
            {
                'name': '最近180天',
                'start_date': (end_date - timedelta(days=180)).strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'description': '半年表现分析'
            },
            {
                'name': '本年度',
                'start_date': f'{end_date.year}-01-01',
                'end_date': end_date.strftime('%Y-%m-%d'),
                'description': '年度表现分析'
            },
            {
                'name': '去年同期',
                'start_date': f'{end_date.year-1}-01-01',
                'end_date': f'{end_date.year-1}-12-31',
                'description': '去年全年表现'
            },
            {
                'name': '最近一年',
                'start_date': (end_date - timedelta(days=365)).strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'description': '年度滚动表现'
            }
        ]

        return jsonify({
            'success': True,
            'data': {
                'preset_intervals': preset_intervals,
                'current_date': end_date.strftime('%Y-%m-%d')
            },
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/stocks/new-high', methods=['GET', 'POST'])
def new_high_stocks():
    """获取新高股票数据"""
    try:
        if request.method == 'POST':
            data = request.get_json() or {}
            period = data.get('period', 5)
            date = data.get('date')
            limit = data.get('limit', 2000)
            exclude_st = data.get('exclude_st', True)
            include_non_main_board = data.get('include_non_main_board', False)
        else:  # GET方法
            period = int(request.args.get('period', 5))
            date = request.args.get('date')
            limit = int(request.args.get('limit', 2000))
            exclude_st = request.args.get('exclude_st', 'true').lower() == 'true'
            include_non_main_board = request.args.get('include_non_main_board', 'false').lower() == 'true'

        # 使用全局的market_states数据
        if market_states is None or market_states.is_empty():
            return jsonify({
                'success': False,
                'error': '无法获取市场数据',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 分析新高股票
        new_high_data = MarketAnalyzer.analyze_new_high_stocks(
            market_states,
            days=period,
            selected_date=date,
            exclude_st=exclude_st,
            include_non_main_board=include_non_main_board
        )

        return jsonify({
            'success': True,
            'data': {
                'stocks': new_high_data[:limit],  # 前端期望stocks字段
                'total': len(new_high_data),
                'period': period,
                'date': date
            },
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/stocks/kline', methods=['GET', 'POST'])
def get_stocks_kline():
    """股票K线图API - 支持单个或多个股票，使用市场状态数据"""
    try:
        if request.method == 'GET':
            # GET请求：单个股票
            stock_code = request.args.get('code')
            if not stock_code:
                return jsonify({
                    'success': False,
                    'error': '缺少股票代码参数',
                    'timestamp': datetime.now().isoformat()
                }), 400

            stock_codes = [stock_code]
            days_back = int(request.args.get('days', 45))
            format_type = request.args.get('format', 'data')
            selected_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))

        else:
            # POST请求：多个股票
            data = request.get_json() or {}
            stock_codes = data.get('codes', [])
            if not stock_codes:
                return jsonify({
                    'success': False,
                    'error': '缺少股票代码列表',
                    'timestamp': datetime.now().isoformat()
                }), 400

            days_back = data.get('days', 45)
            format_type = data.get('format', 'data')
            selected_date = data.get('date', datetime.now().strftime('%Y-%m-%d'))

        # 转换日期
        if isinstance(selected_date, str):
            selected_date = datetime.strptime(selected_date, '%Y-%m-%d').date()
        start_date = selected_date - timedelta(days=days_back)

        # 使用全局市场状态数据
        if market_states is None or market_states.is_empty():
            return jsonify({
                'success': False,
                'error': '无法获取市场状态数据',
                'timestamp': datetime.now().isoformat()
            }), 500

        result = {}

        for stock_code in stock_codes:
            # 确保股票代码为6位数字（0填充）
            stock_code = str(stock_code).zfill(6)

            # 从市场状态数据中筛选股票数据
            stock_data = market_states.filter(
                (pl.col('代码') == stock_code) &
                (pl.col('日期') >= start_date) &
                (pl.col('日期') <= selected_date)
            ).sort('日期')

            if stock_data.is_empty():
                result[stock_code] = {
                    'success': False,
                    'error': f'未找到股票代码 {stock_code} 的数据'
                }
                continue

            # 根据format_type返回不同格式
            if format_type == 'data':
                # 转换为前端期望的格式
                kline_data = stock_data.to_pandas().to_dict('records')
                formatted_data = []
                for record in kline_data:
                    formatted_record = {
                        'date': record['日期'].strftime('%Y-%m-%d') if hasattr(record['日期'], 'strftime') else str(record['日期']),
                        'open': float(record.get('开盘', 0)),
                        'close': float(record.get('收盘', 0)),
                        'high': float(record.get('最高', 0)),
                        'low': float(record.get('最低', 0)),
                        'volume': int(record.get('成交量', 0)),
                        'amount': float(record.get('成交额', 0)),
                        'ma5': float(record.get('MA5', 0)) if record.get('MA5') and record.get('MA5') != 0 else None,
                        'ma10': float(record.get('MA10', 0)) if record.get('MA10') and record.get('MA10') != 0 else None,
                        'ma20': float(record.get('MA20', 0)) if record.get('MA20') and record.get('MA20') != 0 else None,
                    }
                    formatted_data.append(formatted_record)

                result[stock_code] = {
                    'success': True,
                    'data': {
                        'kline_data': formatted_data,
                        'stock_code': stock_code,
                        'total_records': len(formatted_data)
                    }
                }

            elif format_type == 'chart':
                # 生成K线图HTML
                chart_html = VisualizerManager.plot_stock_kline(
                    stock_data,
                    stock_name=stock_code,
                    stock_code=stock_code
                )
                result[stock_code] = {
                    'success': True,
                    'data': {
                        'chart_html': chart_html,
                        'stock_code': stock_code
                    }
                }

            elif format_type == 'both':
                # 返回数据和图表
                kline_data = stock_data.to_pandas().to_dict('records')
                formatted_data = []
                for record in kline_data:
                    formatted_record = {
                        'date': record['日期'].strftime('%Y-%m-%d') if hasattr(record['日期'], 'strftime') else str(record['日期']),
                        'open': float(record.get('开盘', 0)),
                        'close': float(record.get('收盘', 0)),
                        'high': float(record.get('最高', 0)),
                        'low': float(record.get('最低', 0)),
                        'volume': int(record.get('成交量', 0)),
                        'amount': float(record.get('成交额', 0)),
                        'ma5': float(record.get('MA5', 0)) if record.get('MA5') and record.get('MA5') != 0 else None,
                        'ma10': float(record.get('MA10', 0)) if record.get('MA10') and record.get('MA10') != 0 else None,
                        'ma20': float(record.get('MA20', 0)) if record.get('MA20') and record.get('MA20') != 0 else None,
                    }
                    formatted_data.append(formatted_record)

                chart_html = VisualizerManager.plot_stock_kline(
                    stock_data,
                    stock_name=stock_code,
                    stock_code=stock_code
                )

                result[stock_code] = {
                    'success': True,
                    'data': {
                        'kline_data': formatted_data,
                        'stock_code': stock_code,
                        'total_records': len(formatted_data)
                    },
                    'chart_html': chart_html
                }

        # 如果只有一个股票，直接返回该股票的数据
        if len(stock_codes) == 1:
            single_result = result[stock_codes[0]]
            # 如果是图表格式，需要调整数据结构
            if format_type == 'chart' and single_result.get('success'):
                return jsonify({
                    'success': True,
                    'data': {
                        'chart_html': single_result.get('data', {}).get('chart_html', ''),
                        'stock_code': stock_codes[0]
                    },
                    'timestamp': datetime.now().isoformat()
                })
            else:
                return jsonify({
                    'success': True,
                    'data': single_result,
                    'timestamp': datetime.now().isoformat()
                })
        else:
            return jsonify({
                'success': True,
                'data': result,
                'timestamp': datetime.now().isoformat()
            })

    except Exception as e:
        print(f"统一股票K线API失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/sectors', methods=['GET'])
def get_sectors():
    """统一的板块数据API - 支持行业、概念或两者"""
    try:
        # 获取查询参数
        sector_type = request.args.get('type', 'both')  # 'sectors', 'concepts', 'both'
        include_sectors = request.args.get('include_sectors', 'true').lower() == 'true'
        include_concepts = request.args.get('include_concepts', 'true').lower() == 'true'
        target_date = request.args.get('date')  # 新增：支持指定日期

        print(f"🔍 板块数据API调用: type={sector_type}, include_sectors={include_sectors}, include_concepts={include_concepts}, date={target_date}")

        # 使用全局板块数据
        if sector_data is None or sector_data.is_empty():
            return jsonify({
                'success': False,
                'error': '板块数据未加载',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 根据类型筛选数据
        if sector_type == 'sectors' or (include_sectors and not include_concepts):
            # 只返回行业板块
            filtered_data = sector_data.filter(pl.col('板块类型') == '行业')
        elif sector_type == 'concepts' or (include_concepts and not include_sectors):
            # 只返回概念板块
            filtered_data = sector_data.filter(pl.col('板块类型') == '概念')
        else:
            # 返回所有板块
            filtered_data = sector_data

        if filtered_data.is_empty():
            return jsonify({
                'success': False,
                'error': f'{sector_type}板块数据为空',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 如果指定了日期，则获取该日期的数据；否则获取最新日期的数据
        if target_date:
            try:
                # 解析目标日期
                if isinstance(target_date, str):
                    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
                else:
                    target_date_obj = target_date
                
                # 查找最接近目标日期的数据
                available_dates = filtered_data['日期'].unique().sort()
                if available_dates.is_empty():
                    return jsonify({
                        'success': False,
                        'error': '没有可用的板块数据',
                        'timestamp': datetime.now().isoformat()
                    }), 500
                
                # 找到最接近的日期（如果目标日期不存在，则使用最接近的）
                target_data = filtered_data.filter(pl.col('日期') == target_date_obj)
                if target_data.is_empty():
                    # 找到最接近的日期
                    closest_date = None
                    min_diff = float('inf')
                    for date_val in available_dates:
                        diff = abs((date_val - target_date_obj).days)
                        if diff < min_diff:
                            min_diff = diff
                            closest_date = date_val
                    
                    if closest_date:
                        print(f"📅 目标日期 {target_date} 不存在，使用最接近的日期: {closest_date}")
                        target_data = filtered_data.filter(pl.col('日期') == closest_date)
                        target_date_obj = closest_date
                    else:
                        return jsonify({
                            'success': False,
                            'error': f'未找到日期 {target_date} 附近的板块数据',
                            'timestamp': datetime.now().isoformat()
                        }), 500
                
                result_data = target_data
                result_date = target_date_obj
                
            except Exception as e:
                print(f"⚠️ 解析目标日期失败: {e}，使用最新日期")
                # 如果日期解析失败，回退到最新日期
                result_date = filtered_data['日期'].max()
                result_data = filtered_data.filter(pl.col('日期') == result_date)
        else:
            # 获取最新日期的数据
            result_date = filtered_data['日期'].max()
            result_data = filtered_data.filter(pl.col('日期') == result_date)

        return jsonify({
            'success': True,
            'data': result_data.to_dicts(),
            'total_count': result_data.height,
            'latest_date': result_date.strftime('%Y-%m-%d'),
            'type': sector_type,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        print(f"❌ 板块数据API错误: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


def get_sectors_internal(sector_type):
    """内部方法：获取板块数据"""
    try:
        if sector_data is None or sector_data.is_empty():
            return jsonify({
                'success': False,
                'error': '板块数据未加载',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 根据类型筛选数据
        if sector_type == 'sectors':
            filtered_data = sector_data.filter(pl.col('板块类型') == '行业')
        elif sector_type == 'concepts':
            filtered_data = sector_data.filter(pl.col('板块类型') == '概念')
        else:
            filtered_data = sector_data

        if filtered_data.is_empty():
            return jsonify({
                'success': False,
                'error': f'{sector_type}板块数据为空',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 获取最新日期的数据
        latest_date = filtered_data['日期'].max()
        latest_data = filtered_data.filter(pl.col('日期') == latest_date)

        return jsonify({
            'success': True,
            'data': latest_data.to_dicts(),
            'total_count': latest_data.height,
            'latest_date': latest_date.strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/stocks/search')
def search_stocks():
    """搜索股票（根据代码或名称）"""
    try:
        query = request.args.get('query', '').strip()
        if not query:
            return jsonify({
                'success': False,
                'error': '搜索关键词不能为空',
                'timestamp': datetime.now().isoformat()
            }), 400

        # 使用全局市场状态数据搜索股票
        if market_states is None or market_states.is_empty():
            return jsonify({
                'success': False,
                'error': '市场数据未加载',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 获取最新日期的股票数据
        latest_date = market_states['日期'].max()
        latest_stocks = market_states.filter(pl.col('日期') == latest_date)

        # 检查数据列
        print(f"🔧 DEBUG: 市场状态数据列: {market_states.columns}")

        # 使用股票元数据进行搜索（因为市场状态数据没有代码列）
        if stock_metadata is None or stock_metadata.is_empty():
            return jsonify({
                'success': False,
                'error': '股票元数据未加载',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 获取最新日期的股票元数据
        latest_date = stock_metadata['日期'].max()
        latest_stocks = stock_metadata.filter(pl.col('日期') == latest_date)

        # 搜索匹配的股票（代码或名称包含查询词）
        if '代码' in latest_stocks.columns:
            matched_stocks = latest_stocks.filter(
                (pl.col('代码').str.contains(query, literal=True)) |
                (pl.col('名称').str.contains(query, literal=True))
            ).head(20)  # 限制返回20个结果
        else:
            # 如果没有代码列，只按名称搜索
            matched_stocks = latest_stocks.filter(
                pl.col('名称').str.contains(query, literal=True)
            ).head(20)

        # 转换为字典格式
        results = []
        for row in matched_stocks.iter_rows(named=True):
            results.append({
                '代码': row.get('代码', ''),
                '名称': row['名称'],
                '最新价': row.get('收盘', 0),
                '涨跌幅': row.get('涨跌幅', 0),
                '行业': row.get('行业', ''),
                '市值': row.get('总市值', 0)
            })

        return jsonify({
            'success': True,
            'data': results,
            'total': len(results),
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        print(f"❌ 搜索股票失败: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/stocks/comparison', methods=['POST'])
def get_stock_comparison():
    """获取多股票对比K线图"""
    try:
        data = request.get_json() or {}
        stock_codes = data.get('stock_codes', [])
        days_back = data.get('days_back')
        start_date_str = data.get('start_date')
        end_date_str = data.get('end_date')
        normalize = data.get('normalize', True)  # 是否归一化（以涨跌幅为纵坐标）

        if not stock_codes:
            return jsonify({
                'success': False,
                'error': '股票代码列表不能为空',
                'timestamp': datetime.now().isoformat()
            }), 400

        if len(stock_codes) > 30:
            return jsonify({
                'success': False,
                'error': '最多只能对比30只股票',
                'timestamp': datetime.now().isoformat()
            }), 400

        # 确定时间范围
        if start_date_str and end_date_str:
            # 使用自定义时间范围
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

                if start_date >= end_date:
                    return jsonify({
                        'success': False,
                        'error': '开始日期必须早于结束日期',
                        'timestamp': datetime.now().isoformat()
                    }), 400

                # 检查时间范围是否合理（不超过2年）
                if (end_date - start_date).days > 730:
                    return jsonify({
                        'success': False,
                        'error': '时间范围不能超过2年',
                        'timestamp': datetime.now().isoformat()
                    }), 400

            except ValueError:
                return jsonify({
                    'success': False,
                    'error': '日期格式错误，请使用YYYY-MM-DD格式',
                    'timestamp': datetime.now().isoformat()
                }), 400
        elif days_back:
            # 使用天数回溯
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days_back)
        else:
            # 默认30天
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=30)

        # 获取每只股票的数据
        stock_data_dict = {}

        for stock_code in stock_codes:
            try:
                # 确保股票代码为6位数字（0填充）
                stock_code = str(stock_code).zfill(6)

                # 使用股票元数据获取股票数据（因为市场状态数据没有代码列）
                if '代码' in stock_metadata.columns:
                    stock_data = stock_metadata.filter(
                        (pl.col('代码') == stock_code) &
                        (pl.col('日期') >= start_date) &
                        (pl.col('日期') <= end_date)
                    ).sort('日期')
                else:
                    # 如果没有代码列，尝试按名称匹配
                    stock_data = stock_metadata.filter(
                        (pl.col('名称').str.contains(stock_code, literal=True)) &
                        (pl.col('日期') >= start_date) &
                        (pl.col('日期') <= end_date)
                    ).sort('日期')

                if not stock_data.is_empty():
                    # 获取股票名称
                    stock_name = stock_data['名称'].to_list()[0] if '名称' in stock_data.columns else stock_code
                    stock_data_dict[f"{stock_name}({stock_code})"] = stock_data
                else:
                    print(f"⚠️ 未找到股票 {stock_code} 的数据")

            except Exception as e:
                print(f"❌ 获取股票 {stock_code} 数据失败: {e}")
                continue

        if not stock_data_dict:
            return jsonify({
                'success': False,
                'error': '未找到任何有效的股票数据',
                'timestamp': datetime.now().isoformat()
            }), 404

        # 使用VisualizerManager生成对比图
        chart_html = VisualizerManager.plot_stock_comparison(
            stock_data_dict,
            normalize=normalize,
            height="800px"
        )

        # 构建时间范围描述
        if start_date_str and end_date_str:
            time_range_desc = f"{start_date_str} 至 {end_date_str}"
            time_range_type = "custom"
        else:
            time_range_desc = f"最近{days_back or 30}天"
            time_range_type = "preset"

        return jsonify({
            'success': True,
            'data': {
                'chart_html': chart_html,
                'stock_count': len(stock_data_dict),
                'stocks': list(stock_data_dict.keys()),
                'normalize': normalize,
                'time_range': {
                    'type': time_range_type,
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'description': time_range_desc,
                    'days_back': days_back
                }
            },
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        print(f"❌ 生成股票对比图失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/sectors/comparison', methods=['POST', 'OPTIONS'])
def get_sector_comparison():
    """获取多个板块（行业/概念）对比折线图（可选归一化为涨跌幅%）"""
    try:
        # CORS 预检请求快速返回
        if request.method == 'OPTIONS':
            return jsonify({'success': True}), 200
        if data_fetcher is None or getattr(data_fetcher, 'sector_data_manager', None) is None:
            return jsonify({
                'success': False,
                'error': '数据获取器未初始化',
                'timestamp': datetime.now().isoformat()
            }), 500

        data = request.get_json() or {}
        sector_names = data.get('sector_names', [])
        days_back = data.get('days_back')
        start_date_str = data.get('start_date')
        end_date_str = data.get('end_date')
        normalize = data.get('normalize', True)  # True: 涨跌幅(%)  False: 价格

        if not sector_names or not isinstance(sector_names, list):
            return jsonify({
                'success': False,
                'error': '板块名称列表不能为空',
                'timestamp': datetime.now().isoformat()
            }), 400

        if len(sector_names) > 30:
            return jsonify({
                'success': False,
                'error': '最多只能对比30个板块',
                'timestamp': datetime.now().isoformat()
            }), 400

        # 确定时间范围
        if start_date_str and end_date_str:
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

                if start_date >= end_date:
                    return jsonify({
                        'success': False,
                        'error': '开始日期必须早于结束日期',
                        'timestamp': datetime.now().isoformat()
                    }), 400

                # 限制最长区间（与股票一致：最多2年）
                if (end_date - start_date).days > 730:
                    return jsonify({
                        'success': False,
                        'error': '时间范围不能超过2年',
                        'timestamp': datetime.now().isoformat()
                    }), 400

                # 通过 days_back + target_date 方式加载更小范围的数据
                computed_days_back = (end_date - start_date).days
                target_date_for_loading = end_date.strftime('%Y-%m-%d')

                sector_all = data_fetcher.sector_data_manager.load_sector_data(
                    days_back=computed_days_back,
                    target_date=target_date_for_loading
                )
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': '日期格式错误，请使用YYYY-MM-DD格式',
                    'timestamp': datetime.now().isoformat()
                }), 400
        else:
            # 使用 days_back 回溯，默认30天
            end_date = datetime.now().date()
            if not days_back:
                days_back = 30
            start_date = end_date - timedelta(days=days_back)

            sector_all = data_fetcher.sector_data_manager.load_sector_data(
                days_back=days_back,
                target_date=end_date.strftime('%Y-%m-%d')
            )

        if sector_all is None or sector_all.is_empty():
            return jsonify({
                'success': False,
                'error': '无法获取板块数据',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 组装每个板块的数据
        sector_data_dict = {}
        for name in sector_names:
            try:
                df = sector_all.filter(
                    (pl.col('板块名称') == name) &
                    (pl.col('日期') >= start_date) &
                    (pl.col('日期') <= end_date)
                ).sort('日期')
                if not df.is_empty():
                    sector_data_dict[name] = df
                else:
                    print(f"⚠️ 未找到板块 {name} 的数据")
            except Exception as e:
                print(f"❌ 处理板块 {name} 失败: {e}")
                continue

        if not sector_data_dict:
            return jsonify({
                'success': False,
                'error': '未找到任何有效的板块数据',
                'timestamp': datetime.now().isoformat()
            }), 404

        # 直接复用股票对比可视化（按收盘价/涨跌幅绘制折线）
        chart_html = VisualizerManager.plot_stock_comparison(
            sector_data_dict,
            normalize=normalize,
            height="800px"
        )

        # 构建时间范围描述
        time_range_desc = f"{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}"
        time_range_type = 'custom' if start_date_str and end_date_str else 'preset'

        return jsonify({
            'success': True,
            'data': {
                'chart_html': chart_html,
                'sector_count': len(sector_data_dict),
                'sectors': list(sector_data_dict.keys()),
                'normalize': normalize,
                'time_range': {
                    'type': time_range_type,
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'description': time_range_desc,
                    'days_back': days_back if time_range_type == 'preset' else (end_date - start_date).days
                }
            },
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        print(f"❌ 生成板块对比图失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/sectors/names', methods=['GET'])
def get_sectors_names():
    """板块名称API - 支持行业、概念或两者"""
    try:
        if data_fetcher is None:
            return jsonify({
                'success': False,
                'error': '数据获取器未初始化',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 获取查询参数
        sector_type = request.args.get('type', 'both')  # 'sectors', 'concepts', 'both'

        # 使用统一的get_sector_names方法
        result = data_fetcher.sector_data_manager.get_sector_names(sector_type)

        # 如果只要一种类型，简化返回结构
        if sector_type == 'sectors':
            return jsonify({
                'success': True,
                'data': {
                    'names': result['sector_names'],
                    'total_count': result['sector_count'],
                    'type': 'sectors'
                },
                'timestamp': datetime.now().isoformat()
            })
        elif sector_type == 'concepts':
            return jsonify({
                'success': True,
                'data': {
                    'names': result['concept_names'],
                    'total_count': result['concept_count'],
                    'type': 'concepts'
                },
                'timestamp': datetime.now().isoformat()
            })
        else:
            # 返回两种类型
            return jsonify({
                'success': True,
                'data': result,
                'timestamp': datetime.now().isoformat()
            })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/market/indices')
def get_market_indices():
    """获取指数数据"""
    try:
        # 使用已加载的指数元数据
        if index_metadata is None or index_metadata.is_empty():
            return jsonify({
                'success': False,
                'error': '指数数据未加载',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 获取最近30天的指数数据
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)

        # 筛选日期范围内的数据
        filtered_data = index_metadata.filter(
            (pl.col('日期') >= start_date) & (pl.col('日期') <= end_date)
        )

        # 转换为字典格式
        index_data = filtered_data.to_dicts()

        return jsonify({
            'success': True,
            'data': index_data,
            'total_count': len(index_data),
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/indices/analysis', methods=['GET'])
def get_index_analysis():
    """获取指数分析数据，包括北交所微盘股分析和策略建议"""
    try:
        # 获取查询参数
        date_str = request.args.get('date', datetime.now().strftime('%Y%m%d'))

        # 获取北证50和微盘股数据
        beijing_data = MarketAnalyzer.get_beijing_microcap_analysis(date_str)

        # 获取策略建议
        strategy = MarketAnalyzer.get_trading_strategy(beijing_data)

        return jsonify({
            'success': True,
            'data': {
                'beijing_data': beijing_data,
                'strategy': strategy,
                'analysis_date': date_str
            },
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/indices/available', methods=['GET'])
def get_available_indices():
    """获取可用的指数列表"""
    try:
        # 获取可用指数列表
        available_indices = MarketAnalyzer.get_available_indices()

        return jsonify({
            'success': True,
            'data': {
                'available_indices': available_indices,
                'total_count': len(available_indices)
            },
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/market/volume', methods=['GET'])
def get_market_volume():
    """获取市场量能数据"""
    try:
        # 获取查询参数
        current_date = request.args.get('date')
        previous_date = request.args.get('previous_date')
        
        if not current_date:
            current_date = datetime.now().strftime('%Y-%m-%d')
        
        # 标准化日期格式
        if len(current_date) == 8:
            current_date = f"{current_date[:4]}-{current_date[4:6]}-{current_date[6:]}"
        
        print(f"📊 获取市场量能数据: current_date={current_date}, previous_date={previous_date}")
        
        # 获取指数数据管理器实例
        index_manager = data_fetcher.index_metadata_manager
        
        # 获取市场量能对比数据（使用3分钟聚合）
        volume_data = index_manager.get_market_volume_comparison(current_date, previous_date)
        
        if volume_data is None:
            return jsonify({
                'success': False,
                'error': '无法获取市场量能数据',
                'timestamp': datetime.now().isoformat()
            }), 404
        
        # 转换DataFrame为字典格式
        current_data_dict = volume_data['current_data'].to_dicts()
        previous_data_dict = volume_data['previous_data'].to_dicts()
        
        # 生成图表
        from utils.visualizers.index_visualizer import IndexVisualizer
        
        # 生成HTML图表
        chart_html = IndexVisualizer.plot_market_volume_chart(
            volume_data['current_data'],
            volume_data['previous_data'],
            volume_data['comparison_data']
        )
        
        # 生成ECharts配置
        chart_options = IndexVisualizer.get_market_volume_chart_options(
            volume_data['current_data'],
            volume_data['previous_data'],
            volume_data['comparison_data']
        )
        
        return jsonify({
            'success': True,
            'data': {
                'current_data': current_data_dict,
                'previous_data': previous_data_dict,
                'comparison_data': volume_data['comparison_data'],
                'chart_html': chart_html,
                'chart_options': chart_options,
                'metadata': {
                    'current_date': current_date,
                    'previous_date': volume_data['comparison_data']['previous_date'],
                    'total_current_records': len(current_data_dict),
                    'total_previous_records': len(previous_data_dict)
                }
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"❌ 获取市场量能数据失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/indices/kline', methods=['GET', 'POST'])
def get_indices_kline():
    """指数K线图API - 支持单个或多个指数"""
    try:
        if request.method == 'GET':
            # GET请求：单个指数
            index_name = request.args.get('index_name')
            if not index_name:
                return jsonify({
                    'success': False,
                    'error': '缺少指数名称参数',
                    'timestamp': datetime.now().isoformat()
                }), 400

            indices = [index_name]
            date_str = request.args.get('date', datetime.now().strftime('%Y%m%d'))
            days_range = int(request.args.get('days_range', 30))
            format_type = request.args.get('format', 'data')

        else:
            # POST请求：多个指数
            data = request.get_json() or {}
            indices = data.get('indices', [])
            if not indices:
                return jsonify({
                    'success': False,
                    'error': '缺少指数列表',
                    'timestamp': datetime.now().isoformat()
                }), 400

            date_str = data.get('date', datetime.now().strftime('%Y%m%d'))
            days_range = data.get('days_range', 30)
            format_type = data.get('format', 'data')

        result = {}

        if format_type == 'data':
            # 返回数据格式
            for index_name in indices:
                # 获取指数数据
                index_data = data_fetcher.index_metadata_manager.get_index_data(
                    index_name,
                    start_date=None,
                    end_date=None
                )

                if index_data is None or index_data.is_empty():
                    result[index_name] = {
                        'success': False,
                        'error': f'未找到指数 {index_name} 的数据'
                    }
                    continue

                # 转换为前端期望的格式
                index_data_dict = index_data.to_dicts()
                result[index_name] = {
                    'success': True,
                    'data': index_data_dict
                }

        elif format_type == 'chart':
            # 返回图表格式
            if len(indices) == 1:
                # 单个指数图表
                chart_html = IndexVisualizer.plot_index_kline(
                    data_fetcher.index_metadata_manager.get_index_data(indices[0]),
                    title=indices[0]
                )
                result[indices[0]] = {
                    'success': True,
                    'chart_html': chart_html
                }
            else:
                # 多个指数图表
                chart_result = MarketAnalyzer.get_multi_index_kline_data(
                    indices, date_str, days_range
                )
                if chart_result and 'chart_html' in chart_result:
                    # 为了兼容前端期望的数据结构，包装在kline_data中
                    result = {
                        'success': True,
                        'kline_data': {
                            'chart_html': chart_result['chart_html'],
                            'chart_options': chart_result.get('chart_options'),
                            'success_count': chart_result.get('success_count', 0)
                        },
                        'success_count': chart_result.get('success_count', 0)
                    }
                else:
                    result = {
                        'success': False,
                        'error': '生成多指数图表失败'
                    }

        elif format_type == 'both':
            # 返回数据和图表
            for index_name in indices:
                index_data = data_fetcher.index_metadata_manager.get_index_data(
                    index_name,
                    start_date=None,
                    end_date=None
                )

                if index_data is None or index_data.is_empty():
                    result[index_name] = {
                        'success': False,
                        'error': f'未找到指数 {index_name} 的数据'
                    }
                    continue

                # 生成图表
                chart_html = IndexVisualizer.plot_index_kline(
                    index_data,
                    title=index_name
                )

                result[index_name] = {
                    'success': True,
                    'data': index_data.to_dicts(),
                    'chart_html': chart_html
                }

        # 如果只有一个指数，直接返回该指数的数据
        if len(indices) == 1 and format_type != 'chart':
            return jsonify({
                'success': True,
                'data': result[indices[0]],
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'success': True,
                'data': result,
                'timestamp': datetime.now().isoformat()
            })

    except Exception as e:
        print(f"统一指数K线API失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/analysis/heima')
def heima_analysis():
    """黑马分析"""
    try:
        if market_states is None or market_states.is_empty():
            return jsonify({
                'success': False,
                'error': '无法获取市场数据',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 获取过滤参数
        date = request.args.get('date')
        exclude_st = request.args.get('exclude_st', 'true').lower() == 'true'
        include_non_main_board = request.args.get('include_non_main_board', 'false').lower() == 'true'

        print(f"🔧 DEBUG: 黑马分析参数 - date: {date}, exclude_st: {exclude_st}, include_non_main_board: {include_non_main_board}")

        # 使用MarketAnalyzer进行黑马分析
        heima_data = MarketAnalyzer.analyze_heima_stocks(
            market_states,
            date=date,
            exclude_st=exclude_st,
            include_non_main_board=include_non_main_board
        )

        return jsonify({
            'success': True,
            'data': heima_data,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/test', methods=['GET'])
def test_endpoint():
    """测试端点"""
    print("🔧 DEBUG: 测试端点被调用")
    return jsonify({'message': 'Flask服务器正常工作', 'timestamp': datetime.now().isoformat()})

@app.route('/api/market/latest-date', methods=['GET'])
def get_latest_market_date():
    """获取最新可用的市场数据日期"""
    try:
        if market_states is None or market_states.is_empty():
            return jsonify({
                'success': False,
                'error': '无法获取市场数据',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 获取最新日期
        latest_date = market_states['日期'].max()

        # 确保日期格式正确
        if hasattr(latest_date, 'strftime'):
            latest_date_str = latest_date.strftime('%Y-%m-%d')
        else:
            latest_date_str = str(latest_date)

        return jsonify({
            'success': True,
            'data': {
                'latest_date': latest_date_str,
                'current_date': datetime.now().date().strftime('%Y-%m-%d')
            },
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/analysis/baima', methods=['POST'])
def baima_analysis():
    """白马分析 - 支持多时间区间对比"""
    print(f"🔧 DEBUG: 收到白马分析请求！！！")
    try:
        data = request.get_json() or {}
        print(f"🔧 DEBUG: 白马分析请求数据: {data}")

        # 获取基本参数
        min_market_cap = data.get('min_market_cap', 100)  # 最小市值（亿）
        exclude_st = data.get('exclude_st', True)
        include_non_main_board = data.get('include_non_main_board', False)

        # 获取时间区间参数
        intervals = data.get('intervals', [])

        # 检查是否是旧格式的请求（start_date, end_date）
        start_date_param = data.get('start_date')
        end_date_param = data.get('end_date')

        print(f"🔧 DEBUG: start_date_param={start_date_param}, end_date_param={end_date_param}, intervals={intervals}")

        if start_date_param and end_date_param and not intervals:
            # 兼容旧格式，创建单个区间
            intervals = [
                {
                    'start_date': start_date_param,
                    'end_date': end_date_param,
                    'name': '自定义区间'
                }
            ]
            print(f"🔧 DEBUG: 创建自定义区间: {intervals}")

        # 如果没有提供区间，使用默认区间
        if not intervals:
            end_date = datetime.now().date()
            intervals = [
                {
                    'start_date': (end_date - timedelta(days=30)).strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'name': '最近30天'
                },
                {
                    'start_date': (end_date - timedelta(days=90)).strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'name': '最近90天'
                },
                {
                    'start_date': f'{end_date.year}-01-01',
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'name': '本年度'
                }
            ]

        # 获取股票筛选条件
        include_main_board = data.get('include_main_board', True)  # 主板股票
        include_kcb_cyb = data.get('include_kcb_cyb', True)  # 科创板/创业板
        include_bjs = data.get('include_bjs', False)  # 北交所

        print(f"🔧 DEBUG: 白马分析参数")
        print(f"  - min_market_cap: {min_market_cap}")
        print(f"  - exclude_st: {exclude_st}")
        print(f"  - include_non_main_board: {include_non_main_board}")
        print(f"  - intervals: {len(intervals)}个区间")
        print(f"  - 板块筛选: 主板={include_main_board}, 科创板/创业板={include_kcb_cyb}, 北交所={include_bjs}")

        if market_states is None or market_states.is_empty():
            return jsonify({
                'success': False,
                'error': '无法获取市场数据',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 使用MarketAnalyzer中的白马分析
        baima_data = MarketAnalyzer.analyze_baima_stocks(
            market_states,
            intervals=intervals,
            min_market_cap=min_market_cap,
            exclude_st=exclude_st,
            include_main_board=include_main_board,
            include_kcb_cyb=include_kcb_cyb,
            include_bjs=include_bjs
        )

        # 调试：检查返回的数据结构
        if baima_data.get('stocks'):
            sample_stock = baima_data['stocks'][0]
            print(f"🔧 DEBUG Flask: 示例股票字段: {list(sample_stock.keys())}")
            if '行业' in sample_stock:
                print(f"🔧 DEBUG Flask: 示例行业: {sample_stock['行业']}")

        return jsonify({
            'success': True,
            'data': baima_data,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        print(f"❌ 白马分析失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/market/metadata/export')
def export_market_metadata():
    """导出市场元数据 - 支持CSV和Excel格式"""
    try:
        # 获取参数
        export_format = request.args.get('format', 'csv').lower()  # csv 或 excel
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')

        # 获取市场元数据
        if data_fetcher is None:
            return jsonify({
                'success': False,
                'error': '数据获取器未初始化',
                'timestamp': datetime.now().isoformat()
            }), 500

        market_metadata = data_fetcher.market_metadata_manager.load_metadata()
        if market_metadata is None or market_metadata.is_empty():
            return jsonify({
                'success': False,
                'error': '无法获取市场元数据',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 处理日期范围
        if start_date_str and end_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

            # 按日期过滤数据
            date_col = '日期' if '日期' in market_metadata.columns else 'date'
            if date_col in market_metadata.columns:
                market_metadata = market_metadata.filter(
                    (pl.col(date_col) >= pl.lit(start_date)) &
                    (pl.col(date_col) <= pl.lit(end_date))
                )
        else:
            # 默认导出所有数据
            start_date = market_metadata['日期'].min()
            end_date = market_metadata['日期'].max()

        # 转换为pandas DataFrame
        export_df = market_metadata.to_pandas()

        if export_format == 'excel':
            # 导出为Excel
            import io
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                export_df.to_excel(writer, index=False, sheet_name='市场元数据')

            filename = f"market_metadata_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"

            return jsonify({
                'success': True,
                'data': {
                    'filename': filename,
                    'content': buffer.getvalue().hex(),  # 转换为hex字符串传输
                    'format': 'excel',
                    'records_count': len(export_df)
                },
                'timestamp': datetime.now().isoformat()
            })
        else:
            # 导出为CSV
            csv_content = export_df.to_csv(index=False)
            filename = f"market_metadata_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"

            return jsonify({
                'success': True,
                'data': {
                    'filename': filename,
                    'content': csv_content,
                    'format': 'csv',
                    'records_count': len(export_df)
                },
                'timestamp': datetime.now().isoformat()
            })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/market/latest')
def get_latest_market_data():
    """获取最新交易日市场数据"""
    try:
        # 获取市场元数据
        if data_fetcher is None:
            return jsonify({
                'success': False,
                'error': '数据获取器未初始化',
                'timestamp': datetime.now().isoformat()
            }), 500

        market_metadata = data_fetcher.market_metadata_manager.load_metadata()
        if market_metadata is None or market_metadata.is_empty():
            return jsonify({
                'success': False,
                'error': '无法获取市场元数据',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 获取最新交易日数据
        latest_data = market_metadata.filter(pl.col('日期') == market_metadata['日期'].max())
        if latest_data.is_empty():
            return jsonify({
                'success': False,
                'error': '无最新交易日数据',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 提取需要显示的指标
        latest_record = latest_data.to_dicts()[0]
        latest_date = latest_record['日期']

        # 获取各项指标，支持中英文列名
        indicators = {
            'date': latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date),
            'red_ratio': latest_record.get('红盘率', latest_record.get('red_ratio', 0)),
            'limit_up_count': latest_record.get('涨停数', latest_record.get('limit_up_count', 0)),
            'limit_down_count': latest_record.get('跌停数', latest_record.get('limit_down_count', 0)),
            'break_count': latest_record.get('炸板数', latest_record.get('break_count', 0)),
            'total_amount': latest_record.get('成交总额', latest_record.get('total_amount', 0)),
            'total_stocks': latest_record.get('总股票数', latest_record.get('total_stocks', 0)),
            'up_stocks': latest_record.get('上涨股票数', latest_record.get('up_stocks', 0)),
            'down_stocks': latest_record.get('下跌股票数', latest_record.get('down_stocks', 0))
        }

        return jsonify({
            'success': True,
            'data': indicators,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# ===============================
# 节假日和交易日相关API (Holiday and Trading Day APIs)
# ===============================

@app.route('/api/holidays/non-trading-days', methods=['GET'])
def get_non_trading_days():
    """
    获取指定月份的非交易日信息
    支持前端日期选择器标记节假日和周末
    """
    try:
        from utils.holiday_utils import get_non_trading_days_in_month, get_holiday_info
        
        # 获取参数
        year = request.args.get('year', type=int)
        month = request.args.get('month', type=int)
        
        # 如果没有指定年月，使用当前年月
        if not year or not month:
            current_date = datetime.now()
            year = year or current_date.year
            month = month or current_date.month
        
        # 验证参数
        if year < 2020 or year > 2030:
            return jsonify({
                'success': False,
                'error': '年份必须在2020-2030之间',
                'code': 'INVALID_YEAR'
            }), 400
            
        if month < 1 or month > 12:
            return jsonify({
                'success': False,
                'error': '月份必须在1-12之间',
                'code': 'INVALID_MONTH'
            }), 400
        
        # 获取非交易日信息
        non_trading_days = get_non_trading_days_in_month(year, month)
        
        return jsonify({
            'success': True,
            'data': {
                'year': year,
                'month': month,
                'non_trading_days': non_trading_days,
                'count': len(non_trading_days)
            },
            'message': f'已获取{year}年{month}月的非交易日信息'
        })
        
    except Exception as e:
        logger.error(f"获取非交易日信息失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'获取非交易日信息失败: {str(e)}',
            'code': 'FETCH_NON_TRADING_DAYS_ERROR'
        }), 500

@app.route('/api/holidays/check-date', methods=['GET'])
def check_date_trading_status():
    """
    检查指定日期是否为交易日
    """
    try:
        from utils.holiday_utils import get_holiday_info
        
        # 获取日期参数
        date_str = request.args.get('date')
        
        if not date_str:
            return jsonify({
                'success': False,
                'error': '请提供日期参数（格式：YYYY-MM-DD）',
                'code': 'MISSING_DATE'
            }), 400
        
        # 获取日期详细信息
        date_info = get_holiday_info(date_str)
        
        return jsonify({
            'success': True,
            'data': date_info,
            'message': f'{date_str}的交易日状态查询成功'
        })
        
    except Exception as e:
        logger.error(f"检查交易日状态失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'检查交易日状态失败: {str(e)}',
            'code': 'CHECK_TRADING_STATUS_ERROR'
        }), 500

@app.route('/api/holidays/range', methods=['GET'])
def get_non_trading_days_range():
    """
    获取指定日期范围内的非交易日
    """
    try:
        from utils.holiday_utils import china_holiday_util
        
        # 获取参数
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({
                'success': False,
                'error': '请提供start_date和end_date参数（格式：YYYY-MM-DD）',
                'code': 'MISSING_DATE_RANGE'
            }), 400
        
        # 获取日期范围内的非交易日
        non_trading_days = china_holiday_util.get_non_trading_days_in_range(start_date, end_date)
        
        # 获取详细信息
        detailed_info = []
        for day in non_trading_days:
            info = china_holiday_util.get_holiday_info(day)
            detailed_info.append({
                'date': day.strftime('%Y-%m-%d'),
                'type': 'holiday' if info['is_holiday'] else 'weekend',
                'name': info['holiday_name'] or '周末',
                'weekday': info['weekday_name']
            })
        
        return jsonify({
            'success': True,
            'data': {
                'start_date': start_date,
                'end_date': end_date,
                'non_trading_days': detailed_info,
                'count': len(detailed_info)
            },
            'message': f'已获取{start_date}至{end_date}期间的非交易日信息'
        })
        
    except Exception as e:
        logger.error(f"获取日期范围非交易日失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'获取日期范围非交易日失败: {str(e)}',
            'code': 'FETCH_RANGE_NON_TRADING_DAYS_ERROR'
        }), 500

@app.route('/api/system/status')
def system_status():
    """获取系统状态"""
    try:
        status = {
            'data_fetcher': data_fetcher is not None,
            'market_states': market_states is not None and (not market_states.is_empty() if market_states is not None else False),
            'market_metadata_manager': data_fetcher is not None and hasattr(data_fetcher, 'market_metadata_manager'),
            'stock_metadata_manager': data_fetcher is not None and hasattr(data_fetcher, 'stock_metadata_manager'),
            'data_cache_exists': os.path.exists("data_cache"),
            'stock_daily_exists': os.path.exists("data_cache/stock_daily"),
            'market_states_count': market_states.height if (market_states is not None and not market_states.is_empty()) else 0,
            'timestamp': datetime.now().isoformat()
        }

        return jsonify({
            'success': True,
            'data': status,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/money-effect')
def get_money_effect():
    """获取赚钱效应分析数据"""
    try:
        # 获取参数
        date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
        analysis_type = request.args.get('type', 'all')  # 'all'=全部股票前300, 'strong'=近期强势股
        exclude_st = request.args.get('exclude_st', 'true').lower() == 'true'
        include_non_main_board = request.args.get('include_non_main_board', 'false').lower() == 'true'

        print(f"🔍 开始赚钱效应分析: date={date_str}, type={analysis_type}, exclude_st={exclude_st}, include_non_main_board={include_non_main_board}")

        # 转换日期格式
        if '-' in date_str:
            date_str = date_str.replace('-', '')

        # 检查系统是否初始化
        if market_states is None:
            return jsonify({
                'success': False,
                'message': '系统未初始化',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 获取赚钱效应分析数据，传入已加载的market_states避免重复加载
        money_effect_data = MarketAnalyzer.get_money_effect_analysis(
            date_str,
            analysis_type,
            market_states,
            exclude_st=exclude_st,
            include_non_main_board=include_non_main_board
        )

        return jsonify({
            'success': True,
            'stocks': money_effect_data.get('stocks', []),
            'stats': money_effect_data.get('stats', {}),
            'message': money_effect_data.get('message', f'找到 {len(money_effect_data.get("stocks", []))} 只符合条件的股票')
        })

    except Exception as e:
        print(f"❌ 赚钱效应分析失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


@app.route('/api/sectors/<sector_name>/kline')
def get_sector_kline(sector_name):
    """获取单个板块K线图"""
    try:
        days_range = int(request.args.get('days_range', 30))
        format_type = request.args.get('format', 'chart')  # 支持 'chart' 和 'data' 格式
        target_date = request.args.get('date')  # 支持指定日期

        print(f"🔍 生成板块K线图: {sector_name}, 天数: {days_range}, 格式: {format_type}, 日期: {target_date}")

        if format_type == 'data':
            # 返回原始数据，让前端用原生ECharts渲染
            sector_data = data_fetcher.get_sector_kline_data(sector_name, days_range, target_date)
            
            if sector_data is None or sector_data.is_empty():
                return jsonify({
                    'success': False,
                    'error': f'未找到板块 {sector_name} 的数据'
                })
            
            # 转换为前端可用的格式
            formatted_data = []
            for record in sector_data.to_pandas().to_dict('records'):
                formatted_record = {
                    'date': record['日期'].strftime('%Y-%m-%d') if hasattr(record['日期'], 'strftime') else str(record['日期']),
                    'open': float(record.get('开盘', 0)),
                    'close': float(record.get('收盘', 0)),
                    'high': float(record.get('最高', 0)),
                    'low': float(record.get('最低', 0)),
                    'volume': int(record.get('成交量', 0)),
                    'amount': float(record.get('成交额', 0)),
                    'change_pct': float(record.get('涨跌幅', 0)),
                }
                formatted_data.append(formatted_record)
            
            return jsonify({
                'success': True,
                'data': {
                    'sector_name': sector_name,
                    'days_range': days_range,
                    'kline_data': formatted_data,
                    'total_records': len(formatted_data)
                },
                'timestamp': datetime.now().isoformat()
            })
        
        else:
            # 原有的HTML格式（保持兼容性）
            chart_html = VisualizerManager.plot_single_sector_kline(
                data_fetcher,
                sector_name=sector_name,
                overlay_index=None,  # 不再支持叠加指数
                days_range=days_range
            )

            return jsonify({
                'success': True,
                'data': {
                    'sector_name': sector_name,
                    'days_range': days_range,
                    'chart_html': chart_html
                },
                'timestamp': datetime.now().isoformat()
            })

    except Exception as e:
        print(f"❌ 生成板块K线图失败: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# 简易内存缓存：板块成分股（sector_name + date）
_sector_stocks_cache = {}
_SECTOR_STOCKS_CACHE_TTL_SECONDS = 60

# 缓存所有股票的最新一行行情，减少每次分组/聚合开销
_latest_market_cache = {
    'ts': 0.0,
    'map': None  # code -> row dict
}
_LATEST_MARKET_CACHE_TTL_SECONDS = 30

@app.route('/api/sectors/<sector_name>/stocks')
def get_sector_stocks(sector_name):
    """获取板块成分股"""
    try:
        if data_fetcher is None:
            return jsonify({
                'success': False,
                'error': '数据获取器未初始化',
                'timestamp': datetime.now().isoformat()
            }), 500

        # 获取日期参数
        target_date = request.args.get('date')
        # 可选：时间区间参数（JSON字符串，包含若干 {start_date,end_date,name,key}）
        intervals_param = request.args.get('intervals')
        print(f"🔍 正在获取板块 '{sector_name}' 的成分股，目标日期: {target_date}, intervals: {bool(intervals_param)}")

        # URL解码板块名称
        import urllib.parse
        sector_name = urllib.parse.unquote(sector_name)

        # 缓存命中检查
        cache_key = (sector_name, target_date or 'latest')
        now_ts = datetime.now().timestamp()
        cached = _sector_stocks_cache.get(cache_key)
        if cached and now_ts - cached['ts'] < _SECTOR_STOCKS_CACHE_TTL_SECONDS:
            return jsonify({
                'success': True,
                'data': cached['data'],
                'count': len(cached['data']),
                'sector_name': sector_name,
                'timestamp': datetime.now().isoformat(),
                'cached': True
            })

        # 获取板块成分股，使用与板块数据相同的数据源
        print(f"🔍 正在获取板块 '{sector_name}' 的成分股...")

        # 获取当前使用的板块数据源
        current_source = data_fetcher.sector_data_manager.preferred_source
        print(f"📊 使用数据源: {current_source}")

        # 使用相同数据源获取成分股
        stocks_df = data_fetcher.sector_data_manager.get_sector_stocks(sector_name, source=current_source)
        print(f"🔍 获取结果: {stocks_df}")

        if stocks_df is None or stocks_df.is_empty():
            return jsonify({
                'success': False,
                'error': f'未找到板块 "{sector_name}" 的成分股数据',
                'timestamp': datetime.now().isoformat()
            }), 404

        # 使用统一的日期参数验证
        is_valid, target_date_obj, error_msg = validate_date_parameter(target_date)
        if not is_valid:
            print(f"❌ 日期验证失败: {error_msg}")
            return jsonify({
                'success': False,
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }), 400
        
        if target_date_obj:
            print(f"🔍 转换日期: {target_date} -> {target_date_obj}")
        elif target_date:
            print(f"⚠️ 忽略无效日期参数: {target_date}，使用最新数据")

        # 批量获取成分股的实时市场数据 - 性能优化
        enriched_stocks = []
        
        if market_states is not None and not market_states.is_empty():
            # 提取所有股票代码
            stock_codes = []
            stock_info_dict = {}
            
            # 后端防御性去重：构建代码集合，避免重复渲染
            seen_codes = set()
            for stock in stocks_df.to_dicts():
                stock_code = stock.get('代码') or stock.get('股票代码')
                if stock_code and stock_code not in seen_codes:
                    seen_codes.add(stock_code)
                    stock_codes.append(stock_code)
                    stock_info_dict[stock_code] = stock
            
            if stock_codes:
                if target_date_obj:
                    # 指定日期：一次性筛选该日再构造map
                    market_data = market_states.filter(
                        (pl.col('代码').is_in(stock_codes)) & 
                        (pl.col('日期') == pl.lit(target_date_obj))
                    )
                    market_map = {row['代码']: row for row in market_data.to_dicts()} if not market_data.is_empty() else {}
                else:
                    # 最新数据：使用全局缓存的最新行情map，避免每次分组/聚合
                    now_ts2 = datetime.now().timestamp()
                    if (_latest_market_cache['map'] is None) or (now_ts2 - _latest_market_cache['ts'] > _LATEST_MARKET_CACHE_TTL_SECONDS):
                        try:
                            latest_df = (
                                market_states
                                .sort(['代码', '日期'])
                                .group_by('代码', maintain_order=True)
                                .agg([pl.all().last()])
                                .explode(pl.all().exclude('代码'))
                            )
                            _latest_market_cache['map'] = {row['代码']: row for row in latest_df.to_dicts()}
                            _latest_market_cache['ts'] = now_ts2
                        except Exception:
                            _latest_market_cache['map'] = None
                            _latest_market_cache['ts'] = now_ts2
                    market_map = _latest_market_cache['map'] or {}

                # Python层直接按代码聚合，避免大表连接
                result = []
                for code in stock_codes:
                    base = stock_info_dict.get(code, {})
                    m = market_map.get(code, {})
                    # 趋势指标（若market_states预计算存在）
                    up_days = m.get('连阳天数') if '连阳天数' in m else None
                    down_days = m.get('连阴天数') if '连阴天数' in m else None

                    result.append({
                        '代码': code,
                        '名称': base.get('名称') or base.get('股票名称'),
                        '涨跌幅': m.get('涨跌幅', 0),
                        '5日涨跌幅': m.get('5日涨跌幅', None),
                        '10日涨跌幅': m.get('10日涨跌幅', None),
                        '最新价': m.get('收盘', 0),
                        '开盘': m.get('开盘', 0),
                        '最高': m.get('最高', 0),
                        '最低': m.get('最低', 0),
                        '成交量': m.get('成交量', 0),
                        '成交额': m.get('成交额', 0),
                        '振幅': m.get('振幅', 0),
                        '换手率': m.get('换手率', 0),
                        '连阳天数': up_days,
                        '连阴天数': down_days
                    })
                enriched_stocks = result
        else:
            # 如果没有市场数据，只使用基本信息
            seen_codes2 = set()
            for stock in stocks_df.to_dicts():
                stock_code = stock.get('代码') or stock.get('股票代码')
                if stock_code and stock_code not in seen_codes2:
                    seen_codes2.add(stock_code)
                    enriched_stock = {
                        '代码': stock_code,
                        '名称': stock.get('名称') or stock.get('股票名称'),
                        '涨跌幅': 0,
                        '5日涨跌幅': None,
                        '10日涨跌幅': None,
                        '最新价': 0,
                        '开盘': 0,
                        '最高': 0,
                        '最低': 0,
                        '成交量': 0,
                        '成交额': 0,
                        '振幅': 0,
                        '换手率': 0
                    }
                    enriched_stocks.append(enriched_stock)

        # 如果提供了时间区间，基于market_states计算区间涨跌幅并合并
        try:
            if intervals_param and market_states is not None and not market_states.is_empty():
                import json as _json
                try:
                    intervals = _json.loads(intervals_param)
                except Exception:
                    intervals = []

                if isinstance(intervals, list) and len(intervals) > 0:
                    # 预先按股票代码聚合每个区间的首末收盘
                    from datetime import datetime as _dt
                    # 构建代码集合
                    code_set = {item['代码'] for item in enriched_stocks if item.get('代码')}

                    for it in intervals:
                        start_str = it.get('start_date')
                        end_str = it.get('end_date')
                        key_name = it.get('key') or it.get('name')
                        if not (start_str and end_str and key_name):
                            continue

                        try:
                            start_dt = _dt.strptime(start_str, '%Y-%m-%d').date()
                            end_dt = _dt.strptime(end_str, '%Y-%m-%d').date()
                        except Exception:
                            continue

                        # 筛选区间数据
                        seg_df = market_states.filter(
                            (pl.col('代码').is_in(list(code_set))) &
                            (pl.col('日期') >= pl.lit(start_dt)) &
                            (pl.col('日期') <= pl.lit(end_dt))
                        ).sort(['代码', '日期'])

                        if seg_df.is_empty():
                            # 没有数据则该列置空
                            for i in range(len(enriched_stocks)):
                                enriched_stocks[i][key_name] = None
                            continue

                        # 取每个代码的首末收盘
                        try:
                            first_last = (
                                seg_df.group_by('代码', maintain_order=True)
                                .agg([
                                    pl.col('收盘').first().alias('_first_close'),
                                    pl.col('收盘').last().alias('_last_close')
                                ])
                            )
                            fl_map = {row['代码']: (row['_first_close'], row['_last_close']) for row in first_last.to_dicts()}

                            # 合并到列表
                            for idx in range(len(enriched_stocks)):
                                code = enriched_stocks[idx].get('代码')
                                first_last_pair = fl_map.get(code)
                                if first_last_pair and first_last_pair[0]:
                                    first_val, last_val = first_last_pair
                                    try:
                                        if first_val and first_val != 0:
                                            pct = (last_val - first_val) / first_val * 100.0
                                            enriched_stocks[idx][key_name] = round(float(pct), 2)
                                        else:
                                            enriched_stocks[idx][key_name] = None
                                    except Exception:
                                        enriched_stocks[idx][key_name] = None
                                else:
                                    enriched_stocks[idx][key_name] = None
                        except Exception:
                            # 任意失败则该列置空
                            for i in range(len(enriched_stocks)):
                                enriched_stocks[i][key_name] = None
        except Exception:
            pass

        # 写入缓存
        try:
            _sector_stocks_cache[cache_key] = {'data': enriched_stocks, 'ts': now_ts}
        except Exception:
            pass

        return jsonify({
            'success': True,
            'data': enriched_stocks,
            'count': len(enriched_stocks),
            'sector_name': sector_name,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        print(f"❌ 获取板块成分股失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500



# 股票组合管理API
@app.route('/api/stock-groups', methods=['GET'])
def get_stock_groups():
    """获取所有股票组合"""
    try:
        import json
        import os

        groups_file = 'data_cache/stock_groups.json'
        if os.path.exists(groups_file):
            with open(groups_file, 'r', encoding='utf-8') as f:
                groups = json.load(f)
        else:
            groups = []

        return jsonify({
            'success': True,
            'data': groups,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/stock-groups', methods=['POST'])
def save_stock_group():
    """保存股票组合"""
    try:
        import json
        import os

        data = request.get_json() or {}
        group_name = data.get('name', '').strip()
        stock_codes = data.get('stock_codes', [])
        description = data.get('description', '').strip()

        if not group_name:
            return jsonify({
                'success': False,
                'error': '组合名称不能为空',
                'timestamp': datetime.now().isoformat()
            }), 400

        if not stock_codes or len(stock_codes) == 0:
            return jsonify({
                'success': False,
                'error': '股票列表不能为空',
                'timestamp': datetime.now().isoformat()
            }), 400

        if len(stock_codes) > 30:
            return jsonify({
                'success': False,
                'error': '股票数量不能超过30只',
                'timestamp': datetime.now().isoformat()
            }), 400

        # 确保目录存在
        os.makedirs('data_cache', exist_ok=True)
        groups_file = 'data_cache/stock_groups.json'

        # 读取现有组合
        if os.path.exists(groups_file):
            with open(groups_file, 'r', encoding='utf-8') as f:
                groups = json.load(f)
        else:
            groups = []

        # 检查是否已存在同名组合
        existing_group = next((g for g in groups if g['name'] == group_name), None)
        if existing_group:
            return jsonify({
                'success': False,
                'error': f'组合名称 "{group_name}" 已存在',
                'timestamp': datetime.now().isoformat()
            }), 400

        # 创建新组合
        new_group = {
            'id': len(groups) + 1,
            'name': group_name,
            'description': description,
            'stock_codes': stock_codes,
            'stock_count': len(stock_codes),
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }

        groups.append(new_group)

        # 保存到文件
        with open(groups_file, 'w', encoding='utf-8') as f:
            json.dump(groups, f, ensure_ascii=False, indent=2)

        return jsonify({
            'success': True,
            'data': new_group,
            'message': f'股票组合 "{group_name}" 保存成功',
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/stock-groups/<int:group_id>', methods=['DELETE'])
def delete_stock_group(group_id):
    """删除股票组合"""
    try:
        import json
        import os

        groups_file = 'data_cache/stock_groups.json'
        if not os.path.exists(groups_file):
            return jsonify({
                'success': False,
                'error': '未找到股票组合文件',
                'timestamp': datetime.now().isoformat()
            }), 404

        with open(groups_file, 'r', encoding='utf-8') as f:
            groups = json.load(f)

        # 查找要删除的组合
        group_to_delete = next((g for g in groups if g['id'] == group_id), None)
        if not group_to_delete:
            return jsonify({
                'success': False,
                'error': f'未找到ID为 {group_id} 的股票组合',
                'timestamp': datetime.now().isoformat()
            }), 404

        # 删除组合
        groups = [g for g in groups if g['id'] != group_id]

        # 保存到文件
        with open(groups_file, 'w', encoding='utf-8') as f:
            json.dump(groups, f, ensure_ascii=False, indent=2)

        return jsonify({
            'success': True,
            'message': f'股票组合 "{group_to_delete["name"]}" 删除成功',
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


# 时间区间组合管理API
@app.route('/api/interval-groups', methods=['GET'])
def get_interval_groups():
    """获取所有时间区间组合"""
    try:
        import json
        import os

        groups_file = 'data_cache/interval_groups.json'
        if os.path.exists(groups_file):
            with open(groups_file, 'r', encoding='utf-8') as f:
                groups = json.load(f)
        else:
            groups = []

        return jsonify({
            'success': True,
            'data': groups,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/interval-groups', methods=['POST'])
def save_interval_group():
    """保存时间区间组合"""
    try:
        import json
        import os

        data = request.get_json() or {}
        group_name = data.get('name', '').strip()
        description = (data.get('description') or '').strip()
        selected_quick_intervals = data.get('selected_quick_intervals', [])
        custom_intervals = data.get('custom_intervals', [])
        global_date = data.get('global_date')  # 可选
        comparison = data.get('comparison')  # 可选：{"time_range_type":"preset|custom", "days":30, "custom_start_date":"YYYY-MM-DD", "custom_end_date":"YYYY-MM-DD"}

        if not group_name:
            return jsonify({
                'success': False,
                'error': '组合名称不能为空',
                'timestamp': datetime.now().isoformat()
            }), 400

        # 基本结构校验
        if not isinstance(selected_quick_intervals, list) or not isinstance(custom_intervals, list):
            return jsonify({
                'success': False,
                'error': '参数格式错误',
                'timestamp': datetime.now().isoformat()
            }), 400

        # 确保目录存在
        os.makedirs('data_cache', exist_ok=True)
        groups_file = 'data_cache/interval_groups.json'

        # 读取现有组合
        if os.path.exists(groups_file):
            with open(groups_file, 'r', encoding='utf-8') as f:
                groups = json.load(f)
        else:
            groups = []

        # 检查是否已存在同名组合
        existing_group = next((g for g in groups if g['name'] == group_name), None)
        if existing_group:
            return jsonify({
                'success': False,
                'error': f'组合名称 "{group_name}" 已存在',
                'timestamp': datetime.now().isoformat()
            }), 400

        new_group = {
            'id': len(groups) + 1,
            'name': group_name,
            'description': description,
            'selected_quick_intervals': selected_quick_intervals,
            'custom_intervals': custom_intervals,
            'global_date': global_date,
            'comparison': comparison,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }

        groups.append(new_group)

        with open(groups_file, 'w', encoding='utf-8') as f:
            json.dump(groups, f, ensure_ascii=False, indent=2)

        return jsonify({
            'success': True,
            'data': new_group,
            'message': f'时间区间组合 "{group_name}" 保存成功',
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/interval-groups/<int:group_id>', methods=['DELETE'])
def delete_interval_group(group_id: int):
    """删除时间区间组合"""
    try:
        import json
        import os

        groups_file = 'data_cache/interval_groups.json'
        if not os.path.exists(groups_file):
            return jsonify({
                'success': False,
                'error': '未找到时间区间组合文件',
                'timestamp': datetime.now().isoformat()
            }), 404

        with open(groups_file, 'r', encoding='utf-8') as f:
            groups = json.load(f)

        group_to_delete = next((g for g in groups if g['id'] == group_id), None)
        if not group_to_delete:
            return jsonify({
                'success': False,
                'error': f'未找到ID为 {group_id} 的时间区间组合',
                'timestamp': datetime.now().isoformat()
            }), 404

        groups = [g for g in groups if g['id'] != group_id]

        with open(groups_file, 'w', encoding='utf-8') as f:
            json.dump(groups, f, ensure_ascii=False, indent=2)

        return jsonify({
            'success': True,
            'message': f'时间区间组合 "{group_to_delete["name"]}" 删除成功',
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

if __name__ == '__main__':
    print("🚀 启动股票分析系统Flask后端...")
    
    # 初始化系统
    if init_system():
        print("✅ 系统初始化完成")
        print("🚀 启动股票分析系统Flask后端...")
        print("📊 Vue.js + Flask架构")
        print("🌐 后端API地址: http://localhost:5000")
        print("🌐 前端访问地址: http://localhost:8080")
        
        # 启动Flask应用 - 启用调试模式以便查看日志
        app.run(
            host='0.0.0.0',
            port=5000,
            debug=True,
            use_reloader=False
        )
    else:
        print("❌ 系统初始化失败，无法启动服务")
        sys.exit(1)
