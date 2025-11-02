#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用数据处理工具 - 只包含真正有价值的通用功能
避免过度封装简单操作，只提供复杂的通用逻辑
"""

import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Optional, Tuple
import warnings

# 屏蔽pandas警告
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)


class DataProcessor:
    """通用数据处理工具 - 只包含复杂的通用逻辑，避免过度封装"""
    
    @staticmethod
    def should_update_data(latest_date, days_back: int = 0) -> Tuple[bool, str]:
        """
        智能判断是否需要更新数据 - 复杂的通用逻辑
        
        Args:
            latest_date: 最新数据日期
            days_back: 允许落后的天数
            
        Returns:
            (是否需要更新, 开始日期字符串)
        """
        try:
            # 确保latest_date是date对象
            if isinstance(latest_date, str):
                latest_date = datetime.strptime(latest_date, '%Y-%m-%d').date()
            elif isinstance(latest_date, datetime):
                latest_date = latest_date.date()
            
            today = datetime.now().date()
            
            # 如果没有数据或数据过旧，需要更新
            if latest_date is None:
                return True, (today - timedelta(days=days_back)).strftime('%Y%m%d')
            
            # 计算数据延迟天数
            days_behind = (today - latest_date).days
            
            # 如果数据落后超过指定天数，需要更新
            if days_behind > days_back:
                # 从最新数据日期的下一天开始更新
                start_date = (latest_date + timedelta(days=1)).strftime('%Y%m%d')
                return True, start_date
            else:
                return False, today.strftime('%Y%m%d')
                
        except Exception as e:
            print(f"⚠️ 判断更新状态失败: {e}")
            return True, datetime.now().strftime('%Y%m%d')
    
    @staticmethod
    def merge_and_deduplicate_data(existing_data: Optional[pl.DataFrame], 
                                  new_data: pl.DataFrame,
                                  unique_columns: List[str]) -> pl.DataFrame:
        """
        合并数据并去重 - 复杂的通用逻辑
        
        Args:
            existing_data: 现有数据
            new_data: 新数据
            unique_columns: 用于去重的列
            
        Returns:
            合并去重后的数据
        """
        try:
            if existing_data is not None and not existing_data.is_empty():
                # 合并新旧数据
                combined_data = pl.concat([existing_data, new_data])
                
                # 去重：保留最新的记录
                final_data = combined_data.unique(unique_columns, keep='last')
            else:
                final_data = new_data
            
            return final_data
            
        except Exception as e:
            print(f"❌ 合并数据失败: {e}")
            return new_data
    
    @staticmethod
    def calculate_period_changes(data: pl.DataFrame, 
                               price_column: str = '收盘价',
                               date_column: str = '日期',
                               group_columns: List[str] = None,
                               periods: List[int] = [5, 10]) -> pl.DataFrame:
        """
        计算周期涨跌幅 - 复杂的通用逻辑
        
        Args:
            data: 数据
            price_column: 价格列名
            date_column: 日期列名
            group_columns: 分组列名
            periods: 计算周期列表
            
        Returns:
            添加了涨跌幅列的数据
        """
        try:
            if data.is_empty():
                return data
            
            result_data = []
            
            # 如果有分组列，按组计算
            if group_columns:
                for group_values, group_data in data.group_by(group_columns):
                    # 按日期排序
                    sorted_data = group_data.sort(date_column)
                    
                    # 计算各周期涨跌幅
                    change_columns = []
                    for period in periods:
                        if sorted_data.height >= period:
                            latest_price = sorted_data[price_column][-1]
                            past_price = sorted_data[price_column][-period]
                            change = ((latest_price - past_price) / past_price * 100) if past_price != 0 else 0
                        else:
                            change = None
                        
                        change_columns.append(pl.lit(change).alias(f'{period}日涨跌幅'))
                    
                    # 添加涨跌幅列
                    group_with_changes = sorted_data.with_columns(change_columns)
                    result_data.append(group_with_changes)
            else:
                # 不分组，直接计算
                sorted_data = data.sort(date_column)
                change_columns = []
                
                for period in periods:
                    if sorted_data.height >= period:
                        latest_price = sorted_data[price_column][-1]
                        past_price = sorted_data[price_column][-period]
                        change = ((latest_price - past_price) / past_price * 100) if past_price != 0 else 0
                    else:
                        change = None
                    
                    change_columns.append(pl.lit(change).alias(f'{period}日涨跌幅'))
                
                result_data.append(sorted_data.with_columns(change_columns))
            
            if result_data:
                return pl.concat(result_data)
            else:
                # 如果计算失败，添加空列
                empty_columns = [pl.lit(None).alias(f'{period}日涨跌幅') for period in periods]
                return data.with_columns(empty_columns)
                
        except Exception as e:
            print(f"⚠️ 计算周期涨跌幅失败: {e}")
            # 返回带空列的原数据
            empty_columns = [pl.lit(None).alias(f'{period}日涨跌幅') for period in periods]
            return data.with_columns(empty_columns)
    
    @staticmethod
    def calculate_volume_ratio(data: pl.DataFrame,
                              volume_column: str = '成交额',
                              date_column: str = '日期',
                              group_columns: List[str] = None,
                              period: int = 5) -> pl.DataFrame:
        """
        计算量比 - 复杂的通用逻辑
        
        Args:
            data: 数据
            volume_column: 成交量列名
            date_column: 日期列名
            group_columns: 分组列名
            period: 计算周期
            
        Returns:
            添加了量比列的数据
        """
        try:
            if data.is_empty():
                return data
            
            result_data = []
            
            # 如果有分组列，按组计算
            if group_columns:
                for group_values, group_data in data.group_by(group_columns):
                    # 按日期排序
                    sorted_data = group_data.sort(date_column)
                    
                    # 计算量比
                    if sorted_data.height >= period:
                        recent_volumes = sorted_data[volume_column][-period:].to_list()
                        avg_volume = sum(recent_volumes) / len(recent_volumes)
                        
                        # 当日成交量与平均的比值
                        latest_volume = sorted_data[volume_column][-1]
                        volume_ratio = (latest_volume / avg_volume) if avg_volume != 0 else 1
                    else:
                        volume_ratio = 1
                    
                    # 添加量比列
                    group_with_ratio = sorted_data.with_columns([
                        pl.lit(volume_ratio).alias('量比')
                    ])
                    
                    result_data.append(group_with_ratio)
            else:
                # 不分组，直接计算
                sorted_data = data.sort(date_column)
                
                if sorted_data.height >= period:
                    recent_volumes = sorted_data[volume_column][-period:].to_list()
                    avg_volume = sum(recent_volumes) / len(recent_volumes)
                    
                    latest_volume = sorted_data[volume_column][-1]
                    volume_ratio = (latest_volume / avg_volume) if avg_volume != 0 else 1
                else:
                    volume_ratio = 1
                
                result_data.append(sorted_data.with_columns([
                    pl.lit(volume_ratio).alias('量比')
                ]))
            
            if result_data:
                return pl.concat(result_data)
            else:
                return data.with_columns([pl.lit(1.0).alias('量比')])
                
        except Exception as e:
            print(f"⚠️ 计算量比失败: {e}")
            return data.with_columns([pl.lit(1.0).alias('量比')])
    
    @staticmethod
    def standardize_stock_code(code: str, length: int = 6) -> str:
        """标准化股票代码格式 - 简单但常用的工具函数"""
        try:
            return str(code).zfill(length)
        except Exception:
            return str(code)
    
    @staticmethod
    def standardize_date_format(df: pl.DataFrame, date_column: str = '日期') -> pl.DataFrame:
        """标准化日期格式 - 简单但常用的工具函数"""
        try:
            if date_column in df.columns:
                # 确保日期列是字符串类型
                if df[date_column].dtype != pl.Utf8:
                    df = df.with_columns([
                        pl.col(date_column).cast(pl.Utf8).alias(date_column)
                    ])
            return df
        except Exception as e:
            print(f"⚠️ 标准化日期格式失败: {e}")
            return df
