#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
可视化管理器
统一管理所有可视化功能，提供简化的接口

作者: AI助手
日期: 2025-01-24
"""

import importlib.util
import polars as pl
from pathlib import Path
from types import ModuleType
import threading
from typing import List, Dict, Any, Optional
import warnings

from .visualizers import (
    ChartConfig,
    ChartUtils,
    ChartFormatters,
    UniversalKlineChart,
    plot_analysis_kline,
    IndexVisualizer,
    StockVisualizer,
    SectorVisualizer,
    MarketVisualizer,
    ModelVisualizer
)

# 屏蔽pandas警告
warnings.filterwarnings('ignore')

_VISUALIZATION_GATEWAY_PATH = (
    Path(__file__).resolve().parent.parent
    / "skills"
    / "visualization"
    / "scripts"
    / "visualization_gateway.py"
)
_VISUALIZATION_GATEWAY_MODULE: Optional[ModuleType] = None
_VISUALIZATION_GATEWAY_LOCK = threading.Lock()


def _load_visualization_gateway() -> ModuleType:
    global _VISUALIZATION_GATEWAY_MODULE
    with _VISUALIZATION_GATEWAY_LOCK:
        if _VISUALIZATION_GATEWAY_MODULE is not None:
            return _VISUALIZATION_GATEWAY_MODULE
        if not _VISUALIZATION_GATEWAY_PATH.exists():
            raise FileNotFoundError(f"Visualization gateway not found: {_VISUALIZATION_GATEWAY_PATH}")
        spec = importlib.util.spec_from_file_location("visualization_gateway", _VISUALIZATION_GATEWAY_PATH)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Failed to load visualization gateway spec: {_VISUALIZATION_GATEWAY_PATH}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _VISUALIZATION_GATEWAY_MODULE = module
        return module

class VisualizerManager:
    """可视化管理器，提供统一的可视化接口"""
    # ========== 板块可视化 ==========
    @staticmethod
    def plot_sector_kline(data_fetcher, date_str: str, days_range: int = 30, index_name: str = None) -> str:
        """绘制板块K线图 - 平均五日成交量前十的板块"""
        return SectorVisualizer.plot_sector_kline(data_fetcher, date_str, days_range, index_name)

    @staticmethod
    def plot_single_sector_kline(data_fetcher, sector_name: str, overlay_index: str = None, days_range: int = 30) -> str:
        """绘制单个板块K线图"""
        return SectorVisualizer.plot_single_sector_kline(data_fetcher, sector_name, overlay_index, days_range)
    
    @staticmethod
    def get_top_volume_sectors(data: pl.DataFrame, top_n: int = 10, days: int = 5) -> List[str]:
        """获取平均成交量前N的板块名称"""
        return SectorVisualizer.get_top_volume_sectors(data, top_n, days)
    
    # ========== 股票可视化 ==========
    @staticmethod
    def plot_stock_kline(stock_data: pl.DataFrame, 
                        stock_name: str = "", 
                        stock_code: str = "",
                        show_ma: bool = True,
                        show_volume: bool = True) -> str:
        """绘制股票K线图"""
        return StockVisualizer.plot_stock_kline(stock_data, stock_name, stock_code, show_ma, show_volume)
    
    @staticmethod
    def plot_stock_turnover(stock_data: pl.DataFrame, 
                           stock_name: str = "", 
                           stock_code: str = "") -> str:
        """绘制股票成交额图表"""
        return StockVisualizer.plot_turnover_chart(stock_data, stock_name, stock_code)
    
    @staticmethod
    def calculate_stock_ma_lines(stock_data: pl.DataFrame, periods: List[int] = [5, 10, 20]) -> pl.DataFrame:
        """计算股票移动平均线"""
        return StockVisualizer.calculate_ma_lines(stock_data, periods)
    
    @staticmethod
    def plot_stock_comparison(stock_data_dict: Dict[str, pl.DataFrame],
                             normalize: bool = True,
                             height: str = "600px") -> str:
        """绘制多股票对比图"""
        return StockVisualizer.plot_stock_comparison(stock_data_dict, normalize, height)

    @staticmethod
    def plot_new_high_stock_kline(stock_data: pl.DataFrame, stock_code: str,
                                  new_high_date: str = None, period_days: int = 5) -> str:
        """绘制新高股票K线图，带新高标记"""
        return StockVisualizer.plot_new_high_stock_kline(stock_data, stock_code, new_high_date, period_days)

    @staticmethod
    def _calculate_ma(data, window_size):
        """计算移动平均线"""
        return StockVisualizer._calculate_ma(data, window_size)
    
    # ========== 指数可视化 ==========
    @staticmethod
    def plot_index_kline(index_data: pl.DataFrame, title: str = None, height: str = "600px") -> str:
        """绘制指数K线图"""
        return IndexVisualizer.plot_index_kline(index_data, title, height)
    
    @staticmethod
    def plot_multi_index_kline(index_data_dict: Dict[str, pl.DataFrame], height: str = "800px") -> str:
        """绘制多个指数的K线图对比"""
        try:
            print(f"🔧 VisualizerManager: 开始处理多指数K线图对比请求")
            print(f"📊 收到指数数据: {list(index_data_dict.keys()) if index_data_dict else '无数据'}")

            if not index_data_dict:
                return "<div style='text-align:center; padding:50px; color:#666;'>📊 没有选择任何指数数据</div>"

            # 验证数据有效性
            valid_count = 0
            for name, data in index_data_dict.items():
                if data is not None and not data.is_empty():
                    valid_count += 1
                    print(f"✅ {name}: {data.height} 条记录")
                else:
                    print(f"❌ {name}: 数据无效")

            if valid_count == 0:
                return "<div style='text-align:center; padding:50px; color:#f56565;'>❌ 所有指数数据都无效</div>"

            print(f"📈 开始生成 {valid_count} 个指数的对比图表")
            result = IndexVisualizer.plot_multi_index_kline(index_data_dict, height)
            print(f"✅ 多指数K线图对比生成完成")
            return result

        except Exception as e:
            print(f"❌ VisualizerManager多指数K线图对比失败: {e}")
            import traceback
            traceback.print_exc()
            return f"<div style='text-align:center; padding:50px; color:#f56565;'>❌ 多指数K线图对比失败: {str(e)}</div>"
    
    @staticmethod
    def calculate_index_ma_lines(index_data: pl.DataFrame, periods: List[int] = [5, 10, 20]) -> pl.DataFrame:
        """计算指数移动平均线"""
        return IndexVisualizer.calculate_index_ma_lines(index_data, periods)
    
    @staticmethod
    def plot_index_comparison(index_data_dict: Dict[str, pl.DataFrame],
                             normalize: bool = True,
                             height: str = "600px") -> str:
        """绘制多指数对比图（归一化）"""
        return IndexVisualizer.plot_index_comparison(index_data_dict, normalize, height)

    @staticmethod
    def get_multi_index_kline_options(index_data_dict: Dict[str, pl.DataFrame]) -> dict:
        """生成多指数K线图的ECharts配置"""
        return IndexVisualizer.get_multi_index_kline_options(index_data_dict)
    
    # ========== 市场可视化 ==========
    @staticmethod
    def plot_market_metadata(market_states: pl.DataFrame, market_metadata: pl.DataFrame) -> Dict[str, str]:
        """绘制市场元数据图表"""
        return MarketVisualizer.plot_market_metadata(market_states, market_metadata)
    
    @staticmethod
    def plot_market_change_distribution(distribution_data: List[Dict[str, Any]]) -> str:
        """绘制市场涨跌幅分布图"""
        # 转换数据格式
        if distribution_data:
            change_distribution = {
                'ranges': distribution_data  # 直接传递原始数据，因为它已经是正确的格式
            }
            return MarketVisualizer.plot_change_distribution(change_distribution)
        return "<div>没有涨跌幅分布数据</div>"


    # ========== 通用工具 ==========
    @staticmethod
    def plot_analysis_kline(data: pl.DataFrame, entity_type: str = "auto", title: str = None, height: str = "600px") -> str:
        """统一分析K线绘图入口（股票/指数/板块）。"""
        return plot_analysis_kline(data, entity_type=entity_type, title=title, height=height)

    # ========== 通用工具 ==========
    @staticmethod
    def extract_chart_content(html_content: str) -> str:
        """提取图表内容"""
        return ChartUtils.extract_chart_content(html_content)
    
    @staticmethod
    def validate_data(data: pl.DataFrame, required_columns: List[str]) -> tuple:
        """验证数据完整性"""
        return ChartUtils.validate_data_columns(data, required_columns)
    
    @staticmethod
    def format_volume_unit(value: float) -> tuple:
        """格式化成交量单位"""
        return ChartUtils.format_volume_unit(value)


    # ========== 模型可视化 ==========
    @staticmethod
    def plot_model_one_stocks(model_one_stocks: List[dict]):
        """绘制模型一选股结果"""
        return ModelVisualizer.plot_model_one_stocks(model_one_stocks)

    # ========== Skill 扩展图表 ==========
    @staticmethod
    def list_skill_charts(force_reload: bool = False) -> List[Dict[str, Any]]:
        """列出 skills/visualization/scripts 中可用的扩展图表函数"""
        module = _load_visualization_gateway()
        return module.list_charts(force_reload=force_reload)

    @staticmethod
    def render_skill_chart(chart_id: str, **kwargs) -> Dict[str, Any]:
        """按 chart_id 调用 skills/visualization/scripts 下注册的绘图函数"""
        module = _load_visualization_gateway()
        return module.render_chart(chart_id=chart_id, **kwargs)
