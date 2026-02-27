#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
市场元数据管理器

负责管理市场情绪指标，如红盘率、涨停数、跌停数、地天板个数等
"""

import polars as pl
from datetime import datetime, timedelta, date
from pathlib import Path
import os
import time
from typing import Optional, Dict, List, Tuple, Union
import akshare as ak
import pandas as pd
import numpy as np
import threading
import tempfile
import shutil

from utils.trading_calendar import trading_calendar

try:
    import fcntl
except ImportError:
    fcntl = None  # Windows doesn't have fcntl

# 全局锁，防止并发写入
_file_locks = {}
_lock_mutex = threading.Lock()

def _parse_to_date(value):
    """将可能为 str/datetime/date 的值安全转换为 datetime.date。

    返回值：datetime.date 或 None
    """
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%Y%m%d'):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        return None
    return None

def _ensure_date_column(df: pl.DataFrame, column_name: str = '日期') -> pl.DataFrame:
    """确保 DataFrame 指定列为 pl.Date 类型。

    若为 Utf8，尝试按常见三种格式解析；若为 Datetime 则转为 Date；若已为 Date 直接返回。
    """
    if df is None or df.is_empty() or column_name not in df.columns:
        return df
    dtype = df.schema.get(column_name)
    try:
        if dtype == pl.Date:
            return df
        if dtype == pl.Datetime:
            return df.with_columns([
                pl.col(column_name).cast(pl.Date).alias(column_name)
            ])
        if dtype == pl.Utf8:
            # 依次尝试多种日期格式并合并
            parsed1 = pl.col(column_name).str.strptime(pl.Date, fmt='%Y-%m-%d', strict=False)
            parsed2 = pl.col(column_name).str.strptime(pl.Date, fmt='%Y/%m/%d', strict=False)
            parsed3 = pl.col(column_name).str.strptime(pl.Date, fmt='%Y%m%d', strict=False)
            return df.with_columns([
                pl.coalesce([parsed1, parsed2, parsed3]).alias(column_name)
            ])
        # 其他类型尽量直接 cast
        return df.with_columns([
            pl.col(column_name).cast(pl.Date).alias(column_name)
        ])
    except Exception:
        # 兜底：用 Python 解析
        values = [
            _parse_to_date(v) for v in df[column_name].to_list()
        ]
        return df.with_columns([
            pl.Series(name=column_name, values=values).cast(pl.Date)
        ])

def calculate_stock_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """
    计算股票技术指标：涨跌幅和移动均线
    
    参数:
        df: 包含股票数据的DataFrame，必须包含以下列：
            - date: 日期
            - 收盘: 收盘价 (f64)
            - 名称: 股票名称 (str)
    
    返回:
        添加了技术指标的DataFrame
    """
    
    # 数据验证
    required_cols = ['日期', '收盘', '名称']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"缺少必要的列: {missing_cols}")
    
    # 确保数据按股票和日期排序
    df_sorted = df.sort(['名称', '日期'])
    
    # 计算涨跌幅（基于收盘价）
    df_with_changes = df_sorted.with_columns([
        # 5日涨跌幅
        ((pl.col('收盘') / pl.col('收盘').shift(5).over('名称') - 1) * 100)
        .round(2)
        .alias('5日涨跌幅'),
        
        # 10日涨跌幅
        ((pl.col('收盘') / pl.col('收盘').shift(10).over('名称') - 1) * 100)
        .round(2)
        .alias('10日涨跌幅'),
        
        # 20日涨跌幅
        ((pl.col('收盘') / pl.col('收盘').shift(20).over('名称') - 1) * 100)
        .round(2)
        .alias('20日涨跌幅')
    ])
    
    # 计算移动均线
    df_with_ma = df_with_changes.with_columns([
        # 5日均线
        pl.col('收盘')
        .rolling_mean(window_size=5, min_periods=1)
        .over('名称')
        .round(2)
        .alias('MA5'),
        
        # 10日均线
        pl.col('收盘')
        .rolling_mean(window_size=10, min_periods=1)
        .over('名称')
        .round(2)
        .alias('MA10'),
        
        # 20日均线
        pl.col('收盘')
        .rolling_mean(window_size=20, min_periods=1)
        .over('名称')
        .round(2)
        .alias('MA20'),
        
    ])
    
    return df_with_ma

def add_price_relative_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """
    添加价格相对指标（相对于均线的位置）
    
    参数:
        df: 已计算均线的DataFrame
    
    返回:
        添加了相对指标的DataFrame
    """
    return df.with_columns([
        # 收盘价相对于各均线的位置（百分比）
        ((pl.col('收盘') / pl.col('MA5') - 1) * 100).round(2).alias('相对MA5'),
        ((pl.col('收盘') / pl.col('MA10') - 1) * 100).round(2).alias('相对MA10'),
        ((pl.col('收盘') / pl.col('MA20') - 1) * 100).round(2).alias('相对MA20'),
        
        
        # 均线排列状态
        pl.when((pl.col('MA5') > pl.col('MA10')) & 
                (pl.col('MA10') > pl.col('MA20')))
        .then(pl.lit("多头排列"))
        .when((pl.col('MA5') < pl.col('MA10')) & 
              (pl.col('MA10') < pl.col('MA20')))
        .then(pl.lit("空头排列"))
        .otherwise(pl.lit("均线混乱"))
        .alias('均线排列')
    ])

def add_candlestick_trend_streaks(df: pl.DataFrame) -> pl.DataFrame:
    """
    计算K线趋势指标：
    - 阳线定义：收盘 > 开盘
    - 阴线定义：收盘 < 开盘
    - 连阳天数：截至当日连续阳线的天数
    - 连阴天数：截至当日连续阴线的天数
    要求：按股票（以'名称'分组）与日期排序后计算。

    返回：新增列 '连阳天数', '连阴天数', '阳线', '阴线'
    """
    # 数据验证与排序
    required_cols = ['名称', '日期', '开盘', '收盘']
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"缺少必要列用于趋势计算: {missing_cols}")

    sorted_df = df.sort(['名称', '日期'])

    # 标记阳线/阴线
    sorted_df = sorted_df.with_columns([
        (pl.col('收盘') > pl.col('开盘')).alias('阳线'),
        (pl.col('收盘') < pl.col('开盘')).alias('阴线')
    ])

    # 由于Polars当前不直接支持按条件的递增计数，这里使用map_groups手动计算
    def _calc_streaks(group: pl.DataFrame) -> pl.DataFrame:
        pos = group['阳线'].to_list()
        neg = group['阴线'].to_list()
        n = len(pos)
        up_streak = [0] * n
        down_streak = [0] * n
        for i in range(n):
            if pos[i]:
                up_streak[i] = (up_streak[i-1] + 1) if i > 0 else 1
            else:
                up_streak[i] = 0
            if neg[i]:
                down_streak[i] = (down_streak[i-1] + 1) if i > 0 else 1
            else:
                down_streak[i] = 0
        return group.with_columns([
            pl.Series(name='连阳天数', values=up_streak).cast(pl.Int32),
            pl.Series(name='连阴天数', values=down_streak).cast(pl.Int32)
        ])

    result = (
        sorted_df
        .group_by('名称', maintain_order=True)
        .map_groups(_calc_streaks)
    )

    return result

def compute_limits(df):
    """
    计算涨跌停价格和状态，根据股票代码确定不同的涨跌幅限制
    
    参数:
    df: polars DataFrame，包含股票数据
    custom_limits: dict，自定义涨跌幅限制规则，如果为None则使用默认规则
    
    返回:
    修改后的DataFrame，包含涨停价、跌停价、涨停、跌停、炸板等列
    """
    
    # 默认涨跌幅限制规则
    custom_limits = {
        '68': 0.20,  # 科创板20%
        '30': 0.20,  # 创业板20% 
        '8': 0.30,    # 北交所30%
        '4': 0.30,    # 北交所30%
        '9': 0.30,    # 北交所30%
        'ST': 0.05,   # ST股票5%（通过名称判断）
        'default': 0.10  # 主板默认10%
    }

    # 按名称和日期排序
    df = df.sort(['名称', '日期'])
    
    # 检查是否有'昨收'列，如果没有则计算
    if '昨收' not in df.columns:
        df = df.with_columns([
            pl.col('收盘').shift(1).over('名称').alias('昨收')
        ])
    
    # 确保代码列是字符串类型
    df = df.with_columns([
        pl.col('代码').cast(pl.Utf8).alias('代码')
    ])
    
    # 根据代码和名称确定涨跌幅限制
    def get_limit_pct_expr():
        # 从默认值开始
        limit_expr = pl.lit(custom_limits.get('default', 0.10))
        
        # 先检查ST股票（通过名称判断）
        if 'ST' in custom_limits:
            limit_expr = pl.when(
                pl.col('名称').str.contains('ST')
            ).then(
                pl.lit(custom_limits['ST'])
            ).otherwise(limit_expr)
        
        # 按代码前缀匹配
        for prefix, limit_pct in custom_limits.items():
            if prefix not in ['ST', 'default']:  # 跳过特殊键
                limit_expr = pl.when(
                    pl.col('代码').str.starts_with(prefix)
                ).then(
                    pl.lit(limit_pct)
                ).otherwise(limit_expr)
        
        return limit_expr
    
    # 添加涨跌幅限制列
    df = df.with_columns([
        get_limit_pct_expr().alias('涨跌幅限制')
    ])
    
    # 计算涨停价和跌停价
    df = df.with_columns([
        (pl.col('昨收') * (1 + pl.col('涨跌幅限制'))).round(2).alias('涨停价'),
        (pl.col('昨收') * (1 - pl.col('涨跌幅限制'))).round(2).alias('跌停价')
    ])
    
    # 计算涨停、跌停和炸板状态
    df = df.with_columns([
        # 涨停：收盘涨跌幅达到涨跌幅限制且最高涨跌幅也达到涨跌幅限制
        ((pl.col('涨跌幅') * 0.01 >= pl.col('涨跌幅限制')) |
        (pl.col('涨停价') == pl.col('收盘'))).alias('涨停'),
        
        # 跌停：收盘涨跌幅达到负的涨跌幅限制且最低涨跌幅也达到负的涨跌幅限制
        ((pl.col('涨跌幅') * 0.01 <= -pl.col('涨跌幅限制')) |
        (pl.col('跌停价') == pl.col('收盘'))).alias('跌停'),
        
        # 炸板：最高价触及涨停价但收盘价未达到涨停价
        ((pl.col('最高').round(2) == pl.col('涨停价')) & 
        (pl.col('收盘').round(2) < pl.col('涨停价'))).alias('炸板')
    ])
    
    # 统计不同涨跌幅限制的股票数量
    limit_stats = df.group_by('涨跌幅限制').agg([
        pl.count().alias('数量'),
        pl.col('涨停').sum().alias('涨停数量'),
        pl.col('跌停').sum().alias('跌停数量'),
        pl.col('炸板').sum().alias('炸板数量')
    ]).sort('涨跌幅限制')
    
    print("各涨跌幅限制统计:")
    print(limit_stats.to_pandas())
    
    # 打印总体调试信息
    total_records = df.height
    total_zhangting = df.filter(pl.col('涨停') == True).height
    total_dieting = df.filter(pl.col('跌停') == True).height
    total_zhaban = df.filter(pl.col('炸板') == True).height
    
    print(f"\n总计: 记录数={total_records}, 涨停={total_zhangting}, 跌停={total_dieting}, 炸板={total_zhaban}")
    
    return df

def calculate_continuous_limit_up_optimized(df):
    """
    严格计算连板数和连板天数字段
    连板数：连续涨停天数（N天N板）
    连板天数：字符串标签，格式 N天N板
    """
    stock_key = '代码' if '代码' in df.columns else '名称'

    # 去重，避免同一股票同一天重复记录导致连板异常放大
    df = (
        df
        .unique(subset=[stock_key, '日期'], keep='last')
        .sort([stock_key, '日期'])
    )

    # 创建涨停序列分组标识
    df = (
        df
        .with_columns([
            (
                (pl.col('涨停') != pl.col('涨停').shift(1, fill_value=False).over(stock_key))
                .cast(pl.Int32)
                .alias('is_limit_changed')
            )
        ])
        .lazy()
        .with_columns([
            pl.col('is_limit_changed').cumsum().over(stock_key).alias('limit_group')
        ])
        .collect()
    )

    # 严格连续涨停天数
    df = df.with_columns([
        pl.when(pl.col('涨停'))
        .then(
            pl.col('涨停')
            .cast(pl.Int32)
            .cumsum()
            .over([stock_key, 'limit_group'])
            .cast(pl.Int32)
        )
        .otherwise(pl.lit(0).cast(pl.Int32))
        .alias('连板数')
    ])

    # 连板标签统一为 N天N板
    df = df.with_columns([
        pl.when(pl.col('连板数') > 0)
        .then(
            pl.concat_str([
                pl.col('连板数').cast(pl.Utf8),
                pl.lit('天'),
                pl.col('连板数').cast(pl.Utf8),
                pl.lit('板')
            ])
        )
        .otherwise(pl.lit('0天0板'))
        .alias('连板天数')
    ])

    return df.drop(['limit_group', 'is_limit_changed'])

def create_limit_status_parquet(df, output_path):
    """创建涨停跌停状态parquet文件"""
    # 计算涨跌停、连板、技术指标与相对指标，确保包含5/10/20日涨跌幅与MA列
    status_df = compute_limits(df)

    # 确保涨停、跌停和炸板列是布尔类型
    status_df = status_df.with_columns([
        pl.col('涨停').cast(pl.Boolean).alias('涨停'),
        pl.col('跌停').cast(pl.Boolean).alias('跌停'),
        pl.col('炸板').cast(pl.Boolean).alias('炸板')
    ])

    # 初始化并计算连板天数/连板数
    status_df = status_df.with_columns([
        pl.lit(0).cast(pl.Int32).alias('连板天数'),
        pl.lit(0).cast(pl.Int32).alias('连板数')
    ])
    status_df = calculate_continuous_limit_up_optimized(status_df)

    # 计算股票涨跌幅相关技术指标（5/10/20日涨跌幅与MA5/MA10/MA20）
    try:
        status_df = calculate_stock_indicators(status_df)
        # 相对均线类指标（可选）
        status_df = add_price_relative_indicators(status_df)
    except Exception as _e:
        print(f"计算技术指标时出现问题，将继续保存基础状态数据: {_e}")

    # 添加K线趋势指标：阳线/阴线与连阳天数/连阴天数
    try:
        status_df = add_candlestick_trend_streaks(status_df)
    except Exception as _e:
        print(f"计算趋势指标时出现问题，将继续保存: {_e}")
    
    # 不过滤任何记录，保存所有记录（包括涨停和跌停）
    print(f"总记录数: {status_df.height}, 涨停记录数: {status_df.filter(pl.col('涨停') == True).height}, 跌停记录数: {status_df.filter(pl.col('跌停') == True).height}")
    
    # 安全写入parquet文件
    if not safe_write_parquet(status_df, output_path):
        raise Exception(f"写入市场状态数据文件失败: {output_path}")
    
    return status_df

def safe_write_parquet(df: pl.DataFrame, file_path: str, max_retries: int = 3) -> bool:
    """
    安全写入parquet文件，支持重试和文件锁
    
    Args:
        df: 要写入的DataFrame
        file_path: 文件路径
        max_retries: 最大重试次数
    
    Returns:
        bool: 是否写入成功
    """
    if df is None or df.is_empty():
        print(f"⚠️ DataFrame为空，跳过写入: {file_path}")
        return False
    
    file_path = str(file_path)
    
    for attempt in range(max_retries):
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # 使用临时文件写入，然后原子性移动
            temp_file = None
            with tempfile.NamedTemporaryFile(
                mode='wb', 
                delete=False, 
                dir=os.path.dirname(file_path),
                suffix='.tmp'
            ) as temp_file:
                temp_path = temp_file.name
            
            # 写入临时文件
            df.write_parquet(temp_path)
            
            # 原子性移动到目标位置
            shutil.move(temp_path, file_path)
            
            print(f"✅ 成功写入文件: {file_path} ({df.height} 行)")
            return True
            
        except Exception as e:
            print(f"❌ 写入文件失败 (尝试 {attempt + 1}/{max_retries}): {e}")
            
            # 清理临时文件
            if temp_file and os.path.exists(temp_file.name):
                try:
                    os.unlink(temp_file.name)
                except:
                    pass
            
            if attempt < max_retries - 1:
                time.sleep(1)  # 等待1秒后重试
            else:
                return False
    
    return False


class MarketMetadataManager:
    """市场元数据管理类，处理市场情绪指标，如红盘率、涨停数、跌停数、地天板个数等"""

    def __init__(self, metadata_path: str = None,
                 stock_metadata_path: str = None,
                 market_states_path: str = None):
        if metadata_path is None:
            # 使用项目根目录的data_cache/other目录（实际数据存储位置）
            self.metadata_path = Path("data_cache/other/market_metadata.parquet")
        else:
            self.metadata_path = Path(metadata_path)

        if stock_metadata_path is None:
            self.stock_metadata_path = Path("data_cache/stock_daily/stock_daily_metadata.parquet")
        else:
            self.stock_metadata_path = Path(stock_metadata_path)

        if market_states_path is None:
            self.market_states_path = Path("data_cache/other/market_states.parquet")
        else:
            self.market_states_path = Path(market_states_path)

        # 确保目录存在
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        self.stock_metadata_path.parent.mkdir(parents=True, exist_ok=True)
        self.market_states_path.parent.mkdir(parents=True, exist_ok=True)

        # 初始化缓存属性
        self._market_states_cache = None
        self._metadata_cache = None
        self._cache_timestamp = None

        print(f"📊 市场元数据管理器初始化完成")

    def clear_cache(self):
        """清理内存缓存"""
        self._market_states_cache = None
        self._metadata_cache = None
        self._cache_timestamp = None
        print("MarketMetadataManager 内存缓存已清理")
    
    def precompute_market_states(self):
        """预计算市场状态数据，包括涨停、跌停、炸板、连板高度等"""
        try:
            print("开始预计算市场状态数据...")
            
            # 加载股票日K元数据
            stock_data = self.load_stock_metadata()
            if stock_data is None or stock_data.is_empty():
                print("股票日K元数据为空，无法预计算市场状态")
                return False
            
            # 创建涨停跌停状态文件
            create_limit_status_parquet(stock_data, self.market_states_path)
            
            print("市场状态数据预计算完成")
            return True
        except Exception as e:
            print(f"预计算市场状态数据失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    @staticmethod
    def _has_null_values(df: Optional[pl.DataFrame]) -> bool:
        if df is None or df.is_empty():
            return False
        checks = []
        # 检查Null
        checks.extend([
            pl.col(col).is_null().any().alias(f"{col}__has_null")
            for col in df.columns
        ])
        # 检查NaN（仅浮点列）
        checks.extend([
            pl.col(col).is_nan().any().alias(f"{col}__has_nan")
            for col, dtype in df.schema.items()
            if dtype in (pl.Float32, pl.Float64)
        ])
        result = df.select(checks).to_dicts()
        if not result:
            return False
        return any(bool(flag) for flag in result[0].values())

    @staticmethod
    def _get_recent_dates(df: pl.DataFrame, days: int = 30) -> List[date]:
        if df is None or df.is_empty() or days <= 0:
            return []
        date_series = df['日期'] if '日期' in df.columns else None
        if date_series is None:
            return []
        date_list = []
        for value in date_series.to_list():
            parsed = _parse_to_date(value)
            if parsed:
                date_list.append(parsed)
        if not date_list:
            return []
        unique_dates = sorted(set(date_list))
        return unique_dates[-days:]

    def calculate_daily_market_stats_optimized(self, stock_data, date_val):
        """优化的市场统计指标计算方法"""
        # 统一日期列与入参类型
        stock_data = _ensure_date_column(stock_data, '日期')
        date_obj = _parse_to_date(date_val)
        # 筛选当天的数据（使用 Date 类型比较）
        day_data = stock_data.filter(pl.col('日期') == pl.lit(date_obj))
        
        if day_data.is_empty():
            return self._empty_stats(date_val)
        
        # 一次性计算所有基本指标
        counts = day_data.select([
            pl.count().alias('总股票数'),
            pl.sum(pl.col('涨跌幅') > 0).alias('上涨股票数'),
            pl.sum(pl.col('涨跌幅') < 0).alias('下跌股票数'),
            pl.sum(pl.col('涨跌幅') == 0).alias('平盘股票数'),
            pl.sum(pl.col('涨停') == True).alias('涨停数'),
            pl.sum(pl.col('跌停') == True).alias('跌停数'),
            pl.sum(pl.col('炸板') == True).alias('炸板数'),
            pl.sum(pl.col('成交额')).alias('成交总额')
        ]).to_dicts()[0]
        
        # 打印调试信息
        print(f"优化方法计算结果 - 总股票数: {counts['总股票数']}, 涨停数: {counts['涨停数']}, 跌停数: {counts['跌停数']}, 炸板数: {counts['炸板数']}")
        
        # 计算红盘率
        counts['红盘率'] = (counts['上涨股票数'] / counts['总股票数'] * 100) if counts['总股票数'] > 0 else 0
        
        # 计算地天板个数 - 最低为跌停价，同时涨停
        ground_ceiling_count = day_data.filter(
            (pl.col('最低') == pl.col('跌停价')) & 
            (pl.col('收盘') == pl.col('涨停价'))
        ).height
        counts['地天板数'] = ground_ceiling_count
        
        # 计算连板高度分布
        dist = self._extract_limit_distribution(day_data)
        if not dist:
            # 如果缺少预计算列，回退到当日连板结果
            continuous_limit_up_count = self._calculate_continuous_limit_up_optimized(
                stock_data, date_val
            )
            for v in continuous_limit_up_count.values():
                try:
                    lb = int(v)
                except Exception:
                    continue
                if lb > 0:
                    dist[lb] = dist.get(lb, 0) + 1

        for i in range(1, 6):
            counts[f'{i}连板数'] = dist.get(i, 0)
        counts['6连板数'] = sum(v for k, v in dist.items() if k >= 6)
        
        # 添加日期
        counts['日期'] = date_obj
        
        # 转换成交额单位为亿元
        counts['成交总额'] = counts['成交总额'] / 100000000
        
        return counts
    
    def _empty_stats(self, date_val):
        """返回空的统计结果"""
        return {
            '日期': date_val,
            '红盘率': 0.0,
            '涨停数': 0,
            '跌停数': 0,
            '炸板数': 0,
            '地天板数': 0,
            '总股票数': 0,
            '上涨股票数': 0,
            '成交总额': 0.0,
            '1连板数': 0,
            '2连板数': 0,
            '3连板数': 0,
            '4连板数': 0,
            '5连板数': 0,
            '6连板数': 0,
            '最高连板数': 0,
            '最高连板股票数': 0,
            '最高连板股票名称': '',
            '连板总数': 0
        }

    @staticmethod
    def _extract_limit_distribution(day_df: pl.DataFrame) -> Dict[int, int]:
        """将连板相关列转换为 {板数: 股票数} 分布。"""
        if day_df is None or day_df.is_empty():
            return {}

        if '连板数' in day_df.columns:
            lb_expr = pl.col('连板数').cast(pl.Int32, strict=False)
        elif '连板天数' in day_df.columns:
            lb_expr = (
                pl.col('连板天数')
                .cast(pl.Utf8)
                .str.extract(r'(\d+)板', 1)
                .cast(pl.Int32, strict=False)
            )
        else:
            return {}

        dist_df = (
            day_df
            .select(lb_expr.alias('lb'))
            .drop_nulls()
            .filter(pl.col('lb') > 0)
            .group_by('lb')
            .agg(pl.count().alias('n'))
        )

        if dist_df.is_empty():
            return {}

        return {int(row['lb']): int(row['n']) for row in dist_df.to_dicts()}
    
    def _calculate_continuous_limit_up_optimized(self, stock_data, date_val):
        """优化的连板高度计算方法"""
        # 获取当前日期前30天的数据
        if isinstance(date_val, date):
            end_date = date_val
            start_date = end_date - timedelta(days=30)
        else:
            end_date = datetime.strptime(date_val, '%Y-%m-%d').date()
            start_date = end_date - timedelta(days=30)
        
        # 筛选日期范围内的数据（使用日期对象而非字符串）
        period_data = stock_data.filter(
            (pl.col('日期') >= pl.lit(start_date)) & 
            (pl.col('日期') <= pl.lit(end_date))
        )
        
        # 计算连板高度
        period_data = calculate_continuous_limit_up_optimized(period_data)
        
        # 筛选当天的数据（使用日期对象）
        day_data = period_data.filter(pl.col('日期') == pl.lit(end_date))
        
        # 提取连板高度结果
        result = {}
        name_col = '名称' if '名称' in day_data.columns else ('代码' if '代码' in day_data.columns else None)
        if name_col is None:
            return result

        for row in day_data.filter(pl.col('连板数') > 0).select([name_col, '连板数']).to_dicts():
            result[row[name_col]] = int(row['连板数'])
        
        return result
    
    def load_metadata(self) -> Optional[pl.DataFrame]:
        """加载市场元数据文件"""
        if not os.path.exists(self.metadata_path):
            print(f"市场元数据文件不存在: {self.metadata_path}")
            return None
        df = pl.read_parquet(self.metadata_path)
        # 统一日期列为 Date 类型
        df = _ensure_date_column(df, '日期')
        return df

    def get_latest_daily_trade_date(self) -> Optional[date]:
        """获取市场元数据中的最新交易日期"""
        try:
            df = self.load_metadata()
            if df is not None and not df.is_empty():
                latest_date = df['日期'].max()
                if isinstance(latest_date, str):
                    return datetime.strptime(latest_date, '%Y-%m-%d').date()
                elif isinstance(latest_date, datetime):
                    return latest_date.date()
                elif isinstance(latest_date, date):
                    return latest_date
            return None
        except Exception as e:
            print(f"获取最新交易日期失败: {e}")
            return None

    def load_stock_metadata(self) -> Optional[pl.DataFrame]:
        """加载股票日K元数据文件"""
        if not os.path.exists(self.stock_metadata_path):
            print(f"股票日K元数据文件不存在: {self.stock_metadata_path}")
            return None
            
        try:
            df = pl.read_parquet(self.stock_metadata_path)
            df = _ensure_date_column(df, '日期')
            return df
        except Exception as e:
            print(f"读取股票日K元数据文件失败: {str(e)}")
            return None
    
    def load_market_states(self) -> Optional[pl.DataFrame]:
        """加载市场状态数据文件，带智能缓存和文件修复"""
        # 检查内存缓存是否有效（5分钟内）
        if (self._market_states_cache is not None and
            self._cache_timestamp is not None and
            (datetime.now() - self._cache_timestamp).seconds < 300):
            print("使用内存缓存的市场状态数据")
            return self._market_states_cache

        if not os.path.exists(self.market_states_path):
            print(f"市场状态数据文件不存在: {self.market_states_path}")
            # 尝试预计算生成
            try:
                print("尝试预计算生成市场状态数据...")
                if self.precompute_market_states():
                    print("✅ 预计算成功，重新加载市场状态数据")
                    return self.load_market_states()
                else:
                    print("❌ 预计算失败，无法加载市场状态数据")
                    return None
            except Exception as gen_e:
                print(f"预计算市场状态数据失败: {str(gen_e)}")
                return None

        try:
            print("从文件加载市场状态数据")
            data = pl.read_parquet(self.market_states_path)

            # 确保日期列为 Date 类型
            data = _ensure_date_column(data, '日期')

            # 确保股票代码为6位数字（0填充）
            if '代码' in data.columns:
                data = data.with_columns([
                    pl.col('代码').cast(pl.Utf8).str.zfill(6).alias('代码')
                ])

            # 更新内存缓存
            self._market_states_cache = data
            self._cache_timestamp = datetime.now()
            return data
            
        except Exception as e:
            print(f"读取市场状态数据文件失败: {str(e)}")
            
            # 尝试修复损坏的文件
            if "Invalid thrift" in str(e) or "File out of specification" in str(e):
                print("检测到parquet文件损坏，尝试修复...")
                try:
                    # 备份损坏的文件
                    backup_path = f"{self.market_states_path}.corrupted_{int(time.time())}"
                    shutil.move(self.market_states_path, backup_path)
                    print(f"已备份损坏文件到: {backup_path}")
                    
                    # 尝试重新生成市场状态数据
                    print("尝试重新生成市场状态数据...")
                    if self.precompute_market_states():
                        print("✅ 市场状态数据重新生成成功")
                        # 重新加载
                        return self.load_market_states()
                    else:
                        print("❌ 市场状态数据重新生成失败")
                        return None
                        
                except Exception as repair_error:
                    print(f"修复文件失败: {str(repair_error)}")
                    return None
            
            return None
    
    def get_market_data_by_date(self, date_val: Union[str, date]) -> Optional[pl.DataFrame]:
        """获取指定日期的市场数据
        
        Args:
            date_val: 日期，可以是字符串格式('YYYY-MM-DD')或date对象
            
        Returns:
            指定日期的市场数据DataFrame，如果不存在则返回None
        """
        # 加载元数据
        metadata = self.load_metadata()
        if metadata is None or metadata.is_empty():
            return None
            
        # 将字符串日期转换为日期对象
        if isinstance(date_val, str):
            date_obj = datetime.strptime(date_val, '%Y-%m-%d').date()
        else:
            date_obj = date_val
            
        # 筛选指定日期的数据（使用日期对象）
        date_col = '日期' if '日期' in metadata.columns else 'date'
        day_data = metadata.filter(pl.col(date_col) == pl.lit(date_obj))
        
        if day_data.is_empty():
            return None
            
        return day_data
    
    def update_metadata(self, progress_callback=None, refresh_market_states: bool = True) -> bool:
        """更新市场元数据
        
        Args:
            progress_callback: 进度回调函数，接受当前进度、总进度和消息参数
            
        Returns:
            更新是否成功
        """
        try:
            print("开始更新市场元数据...")
            if progress_callback:
                progress_callback(0, 100, "开始更新市场元数据")
            
            # 是否先刷新市场状态数据
            if refresh_market_states:
                # 首先检查市场状态数据是否存在，如果不存在则预计算
                if not os.path.exists(self.market_states_path):
                    if progress_callback:
                        progress_callback(10, 100, "预计算市场状态数据...")
                    if not self.precompute_market_states():
                        if progress_callback:
                            progress_callback(100, 100, "预计算市场状态数据失败，无法更新市场元数据")
                        return False
                else:
                    # 直接全量刷新市场状态数据
                    if progress_callback:
                        progress_callback(10, 100, "全量更新市场状态数据...")
                    if not self.precompute_market_states():
                        if progress_callback:
                            progress_callback(100, 100, "全量更新市场状态数据失败，无法更新市场元数据")
                        return False
            else:
                if not os.path.exists(self.market_states_path):
                    if progress_callback:
                        progress_callback(100, 100, "市场状态数据不存在，无法仅更新市场元数据")
                    return False
            
            # 加载市场状态数据
            if progress_callback:
                progress_callback(20, 100, "加载市场状态数据...")
            market_states = self.load_market_states()
            if market_states is None or market_states.is_empty():
                if progress_callback:
                    progress_callback(100, 100, "市场状态数据为空，无法更新市场元数据")
                return False
            # 统一日期类型
            market_states = _ensure_date_column(market_states, '日期')
            
            # 获取现有市场元数据
            if progress_callback:
                progress_callback(30, 100, "加载现有市场元数据...")
            existing_metadata = self.load_metadata()
            
            # 确定需要更新的日期范围（新增日期 + 历史缺失日期）
            if existing_metadata is not None and not existing_metadata.is_empty():
                # 获取最新日期
                existing_metadata = _ensure_date_column(existing_metadata, '日期')
                latest_date = existing_metadata['日期'].max()
                latest_date = _parse_to_date(latest_date)
                
                # 获取新增日期（大于最新日期）
                try:
                    incremental_dates = (
                        market_states
                        .filter(pl.col('日期') > pl.lit(latest_date))
                        ['日期']
                        .unique()
                        .sort()
                        .to_list()
                    )
                except Exception as e:
                    print(f"筛选日期时出错: {str(e)}")
                    # 尝试使用不同的方法获取日期列表
                    incremental_dates = []
                    for row in market_states.to_dicts():
                        date_val = _parse_to_date(row.get('日期'))
                        if date_val and latest_date and date_val > latest_date:
                            incremental_dates.append(date_val)
                    incremental_dates = sorted(set(incremental_dates))

                # 获取历史缺失日期（market_states有、market_metadata没有）
                try:
                    market_dates_raw = (
                        market_states
                        .select('日期')
                        .unique()
                        .to_series()
                        .to_list()
                    )
                    metadata_dates_raw = (
                        existing_metadata
                        .select('日期')
                        .unique()
                        .to_series()
                        .to_list()
                    )

                    market_dates = {
                        d for d in (_parse_to_date(v) for v in market_dates_raw) if d is not None
                    }
                    metadata_dates = {
                        d for d in (_parse_to_date(v) for v in metadata_dates_raw) if d is not None
                    }

                    missing_dates = sorted(d for d in market_dates if d not in metadata_dates)
                except Exception as e:
                    print(f"检测市场元数据缺失日期时出错: {str(e)}")
                    missing_dates = []

                if missing_dates:
                    print(
                        f"📌 检测到市场元数据缺失交易日 {len(missing_dates)} 个，"
                        f"范围: {missing_dates[0]} 到 {missing_dates[-1]}"
                    )

                dates_to_update = sorted(set(incremental_dates + missing_dates))
            else:
                # 如果没有现有元数据，获取所有日期
                try:
                    dates_to_update = market_states['日期'].unique().sort().to_list()
                except Exception as e:
                    print(f"获取所有日期时出错: {str(e)}")
                    # 尝试使用不同的方法获取日期列表
                    dates_to_update = []
                    for row in market_states.to_dicts():
                        date_val = _parse_to_date(row.get('日期'))
                        if date_val:
                            dates_to_update.append(date_val)
                    dates_to_update = sorted(set(dates_to_update))
                
            # 如果没有需要更新的日期，也继续执行最近30天覆盖刷新与空值检查
            if len(dates_to_update) == 0:
                print("市场元数据没有新增交易日，执行最近30天覆盖刷新与空值检查…")
                if progress_callback:
                    progress_callback(40, 100, "无新增交易日，进行最近30天覆盖刷新…")
                # 将metadata设为现有元数据，后续走统一刷新与保存流程
                metadata = existing_metadata if existing_metadata is not None else pl.DataFrame()
                # 对最近30个交易日执行覆盖式刷新，确保无空值
                recent_dates = self._get_recent_dates(market_states if metadata is None or metadata.is_empty() else metadata, 30)
                if recent_dates:
                    recent_refresh_df = self._recompute_market_metadata(market_states, recent_dates)
                    metadata = self._replace_recent_rows(existing_metadata, recent_refresh_df, recent_dates)
                # 空值检测，必要时触发全量重建
                if self._has_null_values(metadata):
                    print("⚠️ 刷新后的市场元数据存在空值，将尝试全量重建")
                    if not self.precompute_market_states():
                        raise Exception("全量重建市场状态数据失败，无法修复market_metadata空值")
                    market_states = self.load_market_states()
                    if market_states is None or market_states.is_empty():
                        raise Exception("全量重建后市场状态数据为空")
                    metadata = self._recompute_market_metadata(market_states, self._get_recent_dates(market_states, 30) or [])
                    if metadata is None or metadata.is_empty():
                        raise Exception("全量重建后市场元数据仍为空")
                # 保存
                if progress_callback:
                    progress_callback(98, 100, "保存市场元数据…")
                if not safe_write_parquet(metadata, self.metadata_path):
                    raise Exception("保存市场元数据失败")
                print("市场元数据刷新完成（无新增日期场景）")
                if progress_callback:
                    progress_callback(100, 100, "市场元数据更新完成")
                return True
                
            print(f"需要更新 {len(dates_to_update)} 个交易日的市场元数据")
            if progress_callback:
                progress_callback(40, 100, f"需要更新 {len(dates_to_update)} 个交易日的市场元数据")
            
            # 计算每个日期的市场指标
            market_stats = []
            total_dates = len(dates_to_update)
            for i, date_val in enumerate(dates_to_update):
                try:
                    print(f"处理日期 {date_val} ({i+1}/{total_dates})")
                    if progress_callback:
                        progress_callback(
                            40 + int(50 * (i+1) / total_dates),
                            100,
                            f"处理日期 {date_val} ({i+1}/{total_dates})"
                        )
                    
                    # 获取当天的市场状态数据
                    day_states = market_states.filter(pl.col('日期') == pl.lit(_parse_to_date(date_val)))
                    
                    # 计算当天的市场指标
                    day_stats = self.calculate_daily_market_stats_from_states(day_states, _parse_to_date(date_val))
                    market_stats.append(day_stats)
                except Exception as e:
                    print(f"处理日期 {date_val} 时出错: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    # 使用空的统计数据
                    market_stats.append(self._empty_stats(date_val))
            
            # 将所有日期的市场指标合并为一个DataFrame
            if progress_callback:
                progress_callback(90, 100, "合并市场指标数据...")
            market_stats_df = pl.DataFrame(market_stats)
            
            # 合并新旧元数据
            if existing_metadata is not None and not existing_metadata.is_empty():
                if progress_callback:
                    progress_callback(95, 100, "合并新旧元数据...")
                
                # 获取两个DataFrame的列集合
                existing_cols = set(existing_metadata.columns)
                new_cols = set(market_stats_df.columns)
                
                # 找出共同的列
                common_cols = list(existing_cols.intersection(new_cols))
                
                # 确保至少有基本的必要列
                essential_cols = ['日期', '涨停数', '跌停数', '炸板数', '红盘率', '成交总额', '总股票数']
                missing_essential = [col for col in essential_cols if col not in common_cols]
                
                if missing_essential:
                    print(f"警告：合并市场元数据时缺少必要的列: {missing_essential}")
                    print(f"现有数据列: {existing_metadata.columns}")
                    print(f"新数据列: {market_stats_df.columns}")
                
                # 如果共同列不为空，使用共同列合并
                if common_cols:
                    print(f"使用共同列合并市场元数据: {common_cols}")
                    # 只选择共同的列进行合并
                    existing_subset = existing_metadata.select(common_cols)
                    new_subset = market_stats_df.select(common_cols)
                    metadata = pl.concat([existing_subset, new_subset])
                else:
                    print("无法找到共同列，使用新市场元数据替代")
                    metadata = market_stats_df
            else:
                metadata = market_stats_df
                
            # 对最近30个交易日执行覆盖式刷新，确保无空值
            recent_dates = self._get_recent_dates(metadata, 30)
            if recent_dates:
                recent_refresh_df = self._recompute_market_metadata(market_states, recent_dates)
                metadata = self._replace_recent_rows(metadata, recent_refresh_df, recent_dates)

            if self._has_null_values(metadata):
                print("⚠️ 刷新后的市场元数据仍存在空值，将尝试全量重建")
                if not self.precompute_market_states():
                    raise Exception("全量重建市场状态数据失败，无法修复market_metadata空值")
                market_states = self.load_market_states()
                if market_states is None or market_states.is_empty():
                    raise Exception("全量重建后市场状态数据为空")
                metadata = self._recompute_market_metadata(market_states, self._get_recent_dates(market_states, 30) or [])
                if metadata is None or metadata.is_empty():
                    raise Exception("全量重建后市场元数据仍为空")

            # 安全保存元数据
            if progress_callback:
                progress_callback(98, 100, "保存市场元数据...")
            if not safe_write_parquet(metadata, self.metadata_path):
                raise Exception("保存市场元数据失败")
            
            print("市场元数据更新完成")
            if progress_callback:
                progress_callback(100, 100, "市场元数据更新完成")
            return True
        except Exception as e:
            print(f"更新市场元数据失败: {str(e)}")
            import traceback
            traceback.print_exc()
            if progress_callback:
                progress_callback(100, 100, f"更新市场元数据失败: {str(e)}")
            return False
    
    def calculate_daily_market_stats_from_states(self, day_states, date_val):
        """从市场状态数据计算每日市场指标"""
        try:
            if day_states.is_empty():
                return self._empty_stats(date_val)
            
            # 加载股票日K元数据
            stock_metadata = self.load_stock_metadata()
            
            # 筛选当天的数据（使用日期对象）
            day_all_stocks = stock_metadata.filter(pl.col('日期') == pl.lit(date_val))
            
            # 计算总股票数、涨停数、跌停数、炸板数
            stats = {
                '总股票数': day_all_stocks.height if not day_all_stocks.is_empty() else 0,
                '涨停数': 0,
                '跌停数': 0,
                '炸板数': 0
            }
            
            # 从day_states中计算涨停数、跌停数和炸板数
            if not day_states.is_empty():
                if '涨停' in day_states.columns:
                    stats['涨停数'] = day_states.filter(pl.col('涨停') == True).height
                    print(f"从day_states中计算涨停数: {stats['涨停数']}")
                
                if '跌停' in day_states.columns:
                    stats['跌停数'] = day_states.filter(pl.col('跌停') == True).height
                    print(f"从day_states中计算跌停数: {stats['跌停数']}")
                
                if '炸板' in day_states.columns:
                    stats['炸板数'] = day_states.filter(pl.col('炸板') == True).height
                    print(f"从day_states中计算炸板数: {stats['炸板数']}")
            
            # 如果day_states中没有相关列或计算结果为0，尝试从day_all_stocks中计算
            if stats['涨停数'] == 0 and not day_all_stocks.is_empty() and '涨停' in day_all_stocks.columns:
                stats['涨停数'] = day_all_stocks.filter(pl.col('涨停') == True).height
                print(f"从day_all_stocks中计算涨停数: {stats['涨停数']}")
            
            if stats['跌停数'] == 0 and not day_all_stocks.is_empty() and '跌停' in day_all_stocks.columns:
                stats['跌停数'] = day_all_stocks.filter(pl.col('跌停') == True).height
                print(f"从day_all_stocks中计算跌停数: {stats['跌停数']}")
            
            if stats['炸板数'] == 0 and not day_all_stocks.is_empty() and '炸板' in day_all_stocks.columns:
                stats['炸板数'] = day_all_stocks.filter(pl.col('炸板') == True).height
                print(f"从day_all_stocks中计算炸板数: {stats['炸板数']}")
            
            # 计算红盘率（涨幅>0的股票比例）
            if not day_all_stocks.is_empty() and '涨跌幅' in day_all_stocks.columns:
                up_count = day_all_stocks.filter(pl.col('涨跌幅') > 0).height
                stats['上涨股票数'] = up_count
                stats['红盘率'] = (up_count / stats['总股票数'] * 100) if stats['总股票数'] > 0 else 0
            else:
                # 如果没有涨跌幅列，设置默认值
                stats['上涨股票数'] = 0
                stats['红盘率'] = 0
            
            # 计算市场量能（成交额）
            # 获取上证指数和深证成指的成交额
            try:
                # 获取指定日期的上证指数和深证成指数据
                # 尝试从指数元数据中获取数据
                index_metadata_path = "data_cache/indices/index_daily_metadata.parquet"
                if os.path.exists(index_metadata_path):
                    try:
                        # 读取指数元数据
                        index_metadata = pl.read_parquet(index_metadata_path)
                        
                        # 确保日期列格式正确
                        date_col = '日期'
                        
                        if date_col is not None:
                            # 筛选上证指数和深证成指的数据
                            sh_code = '000001'
                            sz_code = '399001'
                            
                            # 确定代码列
                            code_col = '代码'
                            
                            if code_col is not None:
                                # 筛选指定日期的数据（使用日期对象）
                                if sh_code is not None and sz_code is not None:
                                    sh_day_data = index_metadata.filter(
                                        (pl.col(date_col) == pl.lit(date_val)) & 
                                        (pl.col(code_col) == sh_code)
                                    )
                                    
                                    sz_day_data = index_metadata.filter(
                                        (pl.col(date_col) == pl.lit(date_val)) & 
                                        (pl.col(code_col) == sz_code)
                                    )
                                    
                                    # 确定成交额列
                                    amount_col = '成交额'

                                    
                                    if amount_col is not None:
                                        # 计算总成交额（上证+深证）
                                        sh_amount = float(sh_day_data[amount_col].sum()) if not sh_day_data.is_empty() else 0
                                        sz_amount = float(sz_day_data[amount_col].sum()) if not sz_day_data.is_empty() else 0
                                        
                                        # 转换为亿元（上证+深证 [+ 北交所]）
                                        # 注：akshare 指数日线 '成交额' 为对应市场当日成交额，单位为元
                                        # 因此这里不再额外缩放，直接按亿元汇总
                                        bj_amount = 0.0
                                        try:
                                            bj_code = '899050'
                                            bj_day_data = index_metadata.filter(
                                                (pl.col(date_col) == pl.lit(date_val)) & 
                                                (pl.col(code_col) == bj_code)
                                            )
                                            if not bj_day_data.is_empty() and '成交额' in bj_day_data.columns:
                                                bj_amount = float(bj_day_data['成交额'].sum())
                                        except Exception:
                                            bj_amount = 0.0

                                        raw_amount = (sh_amount + sz_amount + bj_amount) / 100000000  # 亿元

                                        amount_source = '指数元数据'

                                        # 单位自检：若异常偏小（通常沪深两市日成交额合计应远大于100亿），
                                        # 回退到AK日线权威值（单位：元）以避免数据源单位差异导致的低估。
                                        try:
                                            sh_yi = sh_amount / 100000000
                                            sz_yi = sz_amount / 100000000
                                            # 若总额或任一市场金额明显偏小，则触发回退
                                            if raw_amount < 100 or sh_yi < 100 or sz_yi < 100:
                                                import akshare as ak
                                                ds = date_val.strftime('%Y%m%d') if hasattr(date_val, 'strftime') else str(date_val).replace('-', '')
                                                _sh = ak.index_zh_a_hist(symbol='000001', period='daily', start_date=ds, end_date=ds)
                                                _sz = ak.index_zh_a_hist(symbol='399001', period='daily', start_date=ds, end_date=ds)
                                                _bj = None
                                                try:
                                                    _bj = ak.index_zh_a_hist(symbol='899050', period='daily', start_date=ds, end_date=ds)
                                                except Exception:
                                                    _bj = None
                                                sh_amount = float(_sh['成交额'].iloc[-1]) if (_sh is not None and not _sh.empty and '成交额' in _sh.columns) else 0.0
                                                sz_amount = float(_sz['成交额'].iloc[-1]) if (_sz is not None and not _sz.empty and '成交额' in _sz.columns) else 0.0
                                                bj_amount = float(_bj['成交额'].iloc[-1]) if (_bj is not None and not _bj.empty and '成交额' in _bj.columns) else 0.0
                                                raw_amount = (sh_amount + sz_amount + bj_amount) / 100000000
                                                amount_source = '指数元数据(AK校准)'
                                                print(f"⚠️ 指数元数据成交额疑似单位异常，已改用AK日线: 上证{sh_amount/100000000:.2f}亿 + 深证{sz_amount/100000000:.2f}亿 + 北证{bj_amount/100000000:.2f}亿 = {raw_amount:.2f}亿")
                                        except Exception:
                                            pass

                                        stats['成交总额'] = raw_amount
                                        print(f"从指数元数据中获取成交额: 上证{sh_amount/100000000:.2f}亿 + 深证{sz_amount/100000000:.2f}亿 + 北证{bj_amount/100000000:.2f}亿 = {stats['成交总额']:.2f}亿")
                                        
                                        # 计算连板高度分布
                                        if '连板数' in day_states.columns:
                                            dist = self._extract_limit_distribution(day_states)
                                            for i in range(1, 6):
                                                stats[f'{i}连板数'] = dist.get(i, 0)
                                            stats['6连板数'] = sum(v for k, v in dist.items() if k >= 6)
                                        else:
                                            for i in range(1, 6):
                                                stats[f'{i}连板数'] = 0
                                            stats['6连板数'] = 0
                                        
                                        # 添加日期
                                        stats['日期'] = date_val
                                        
                                        # 仅当从指数元数据计算的成交额有效(>0)时直接返回；
                                        # 否则继续走后续回退逻辑，用stock日K或market_states汇总填充，避免误写0。
                                        try:
                                            if stats['成交总额'] is not None and float(stats['成交总额']) > 0:
                                                return stats
                                        except Exception:
                                            pass
                    except Exception as e:
                        print(f"从指数元数据获取市场量能时出错: {str(e)}")
                
                # 如果无法从指数元数据获取，尝试从市场状态数据中获取成交额
                if '成交额' in day_states.columns:
                    stats['成交总额'] = day_states['成交额'].sum() / 100000000
                    print(f"从市场状态数据中获取成交额: {stats['成交总额']} 亿元")
                elif '成交额' in day_all_stocks.columns:
                    stats['成交总额'] = day_all_stocks['成交额'].sum() / 100000000
                    print(f"从日K数据中获取成交额: {stats['成交总额']} 亿元")
                else:
                    stats['成交总额'] = 0
                    print("无法获取成交额，设置为0")
                
                # 计算连板高度分布
                if '连板数' in day_states.columns:
                    dist = self._extract_limit_distribution(day_states)
                    for i in range(1, 6):
                        stats[f'{i}连板数'] = dist.get(i, 0)
                    stats['6连板数'] = sum(v for k, v in dist.items() if k >= 6)
                else:
                    for i in range(1, 6):
                        stats[f'{i}连板数'] = 0
                    stats['6连板数'] = 0
            except Exception as e:
                print(f"计算市场量能时出错: {str(e)}")
                stats['成交总额'] = 0
                for i in range(1, 7):
                    stats[f'{i}连板数'] = 0
            
            # 添加日期
            stats['日期'] = date_val
            
            return stats
        except Exception as e:
            print(f"计算市场指标时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return self._empty_stats(date_val)

    def is_latest_daily_trading_day(self) -> bool:
        """检查市场状态数据是否为最新交易日"""
        try:
            # 加载市场状态数据
            market_states = self.load_market_states()
            if market_states is None or market_states.is_empty():
                print("市场状态数据为空，需要更新")
                return False

            # 获取数据中的最新日期
            latest_date = market_states['日期'].max()

            # 确保latest_date是datetime.date类型
            if isinstance(latest_date, str):
                latest_date = datetime.strptime(latest_date, '%Y-%m-%d').date()
            elif hasattr(latest_date, 'date'):
                latest_date = latest_date.date()

            # 获取当前日期
            today = datetime.now().date()

            # 检查今天是否是交易日
            is_weekend = today.weekday() >= 5  # 周末
            try:
                is_holiday = trading_calendar.is_holiday(today)
            except Exception:
                is_holiday = False

            is_trading_day = not (is_weekend or is_holiday)

            # 如果今天是交易日，检查数据是否包含今天
            if is_trading_day:
                return latest_date >= today
            else:
                # 如果今天不是交易日，找到最近的交易日
                check_date = today - timedelta(days=1)
                while check_date.weekday() >= 5 or (check_date.strftime('%Y-%m-%d') in getattr(self, '_holidays', [])):
                    check_date -= timedelta(days=1)
                return latest_date >= check_date

        except Exception as e:
            print(f"检查市场状态数据最新日期失败: {e}")
            return False

    def _recompute_market_metadata(self, market_states: pl.DataFrame, dates: List[date]) -> pl.DataFrame:
        if not dates:
            return pl.DataFrame()
        dates = sorted(set(_parse_to_date(d) for d in dates if d is not None))
        if not dates:
            return pl.DataFrame()

        rows = []
        for d in dates:
            day_states = market_states.filter(pl.col('日期') == pl.lit(d))
            stats = self.calculate_daily_market_stats_from_states(day_states, d)
            rows.append(stats)

        if not rows:
            return pl.DataFrame()

        refreshed = pl.DataFrame(rows)
        if '日期' in refreshed.columns:
            refreshed = _ensure_date_column(refreshed, '日期')
        return refreshed

    def _replace_recent_rows(self, existing: Optional[pl.DataFrame], recent: pl.DataFrame, recent_dates: List[date]) -> pl.DataFrame:
        if recent is None or recent.is_empty():
            return existing if existing is not None else pl.DataFrame()

        recent_dates = [d for d in (recent_dates or []) if d is not None]
        if not recent_dates:
            return existing if existing is not None else recent

        recent = _ensure_date_column(recent, '日期')

        if existing is None or existing.is_empty():
            return recent.sort('日期') if '日期' in recent.columns else recent

        existing = _ensure_date_column(existing, '日期')

        mask = pl.col('日期').is_in(recent_dates)
        existing_others = existing.filter(~mask)

        union_cols = list(set(existing_others.columns) | set(recent.columns))
        for col in union_cols:
            if col not in existing_others.columns:
                existing_others = existing_others.with_columns(pl.lit(None).alias(col))
            if col not in recent.columns:
                recent = recent.with_columns(pl.lit(None).alias(col))

        # 不再持久化 `成交额来源`，如存在则在合并前去除该列，避免落盘结构变动
        if '成交额来源' in existing_others.columns:
            existing_others = existing_others.drop('成交额来源')
        if '成交额来源' in recent.columns:
            recent = recent.drop('成交额来源')
        if '成交额来源' in union_cols:
            union_cols.remove('成交额来源')

        existing_others = existing_others.select(union_cols)
        recent = recent.select(union_cols)

        merged = pl.concat([existing_others, recent], how='vertical')
        if '日期' in merged.columns and '名称' in merged.columns:
            merged = merged.sort(['日期', '名称'])
        elif '日期' in merged.columns:
            merged = merged.sort('日期')

        return merged

