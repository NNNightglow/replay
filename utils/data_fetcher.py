#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据获取器 - 清理版本，只保留纯数据获取功能
迁移后的架构：只负责从各种数据源获取原始数据
"""

import akshare as ak
import polars as pl
from datetime import datetime, timedelta, date, time as dt_time
from .data_cache import DataCache
from typing import Dict, Optional, Union, List, Tuple
from pathlib import Path
import os
import pandas as pd
import time
import threading

# 导入metadata管理器
from .metadata.stock_data_manager import StockMetadataManager
from .metadata.index_data_manager import IndexMetadataManager
from .metadata.market_data_manager import MarketMetadataManager
from .metadata.sector_data_manager import SectorDataManager


class LocalFileDataFetcher:
    """从本地文件系统获取数据"""
    def __init__(self, data_dir: str = "E:/jupyter/大A/all_stock_candle/stock/daily"):
        super().__init__()
        self.data_dir = data_dir
        print(f"初始化本地文件数据获取器，数据目录: {data_dir}")
        
        # 确保stock_metadata_manager和index_metadata_manager已正确初始化
        self.stock_metadata_manager = StockMetadataManager()
        self.index_metadata_manager = IndexMetadataManager()
        
        # 检查目录是否存在
        if not os.path.exists(data_dir):
            raise FileNotFoundError(f"数据目录不存在: {data_dir}")
            
        # 获取可用的股票代码列表
        self.available_stocks = self._get_available_stocks()
        print(f"找到 {len(self.available_stocks)} 只股票的本地数据")
    
    def _is_bond_code(self, stock_code: str) -> bool:
        """判断是否为债券代码"""
        # 定义需要忽略的债券代码列表
        ignore_codes = [
            'sh600001', 'sh600002', 'sh600003', 'sh600065', 'sh600087', 'sh600092',
            'sh600102', 'sh600181', 'sh600205', 'sh600253', 'sh600263', 'sh600286',
            'sh600296', 'sh600357', 'sh600472', 'sh600553', 'sh600591', 'sh600607',
            'sh600625', 'sh600627', 'sh600631', 'sh600632', 'sh600646', 'sh600659',
            'sh600669', 'sh600670', 'sh600672', 'sh600700', 'sh600709', 'sh600752',
            'sh600762', 'sh600772', 'sh600786', 'sh600788', 'sh600799', 'sh600813',
            'sh600832', 'sh600840'
        ]
        
        # 如果在忽略列表中，则是债券
        if stock_code in ignore_codes:
            return True
            
        # 通用的债券代码识别规则
        bond_prefixes = ['sh110', 'sh113', 'sh120', 'sh122', 'sh123', 'sh124', 'sh127', 'sh128', 'sh132', 
                         'sz110', 'sz111', 'sz112', 'sz113', 'sz118', 'sz120', 'sz123', 'sz127', 'sz128']
        
        # 检查是否匹配债券前缀
        for prefix in bond_prefixes:
            if stock_code.startswith(prefix):
                return True
                
        return False

    def _get_available_stocks(self) -> List[str]:
        """获取可用的股票代码列表"""
        stock_files = glob.glob(os.path.join(self.data_dir, "*.csv"))
        stock_codes = [os.path.basename(f).replace(".csv", "") for f in stock_files]
        
        # 过滤掉债券代码
        filtered_codes = []
        bond_codes = []
        
        for code in stock_codes:
            if self._is_bond_code(code):
                bond_codes.append(code)
            else:
                filtered_codes.append(code)
        
        # 记录过滤情况
        if bond_codes:
            print(f"已过滤 {len(bond_codes)} 个债券代码，例如: {bond_codes[:5]}")
        
        return filtered_codes
    
    def _read_stock_data(self, stock_code: str) -> pl.DataFrame:
        """读取单只股票的数据"""
        # 如果是债券代码，直接返回空DataFrame，不打印错误信息
        if self._is_bond_code(stock_code):
            return pl.DataFrame()
            
        file_path = os.path.join(self.data_dir, f"{stock_code}.csv")
        if not os.path.exists(file_path):
            print(f"股票 {stock_code} 的数据文件不存在: {file_path}")
            return pl.DataFrame()
            
        try:
            # 读取CSV文件
            df = pl.read_csv(file_path)
            
            # 如果文件为空或只有标题行，返回空DataFrame
            if df.is_empty() or df.height <= 1:
                return pl.DataFrame()
            
            # 确保日期列格式正确
            if '日期' in df.columns:
                df = df.with_columns([
                    pl.col('日期').str.strptime(pl.Date, '%Y-%m-%d').alias('date')
                ])
                # 删除原始日期列
                df = df.drop('日期')
            
            # 重命名列以匹配期望的格式
            column_mapping = {
                '代码': 'code',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'change_pct',
                '涨跌额': 'change_amount',
                '换手率': 'turnover'
            }
            
            # 重命名列
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df = df.rename({old_col: new_col})
            
            return df
            
        except Exception as e:
            # 如果是"empty CSV"错误，静默处理
            if "empty CSV" in str(e):
                return pl.DataFrame()
            print(f"读取股票 {stock_code} 数据失败: {str(e)}")
            return pl.DataFrame()
    
    def get_index_data(self, start_date: str, end_date: str = None) -> dict:
        """获取主要指数数据"""
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        indices = {
            '上证指数': 'sh000001',
            '深证成指': 'sz399001',
            '创业板指': 'sz399006'
        }
        
        result = {}
        for name, code in indices.items():
            # 检查缓存
            if not self.cache.needs_update(f"index_{code}", end_date):
                cached_data = self.cache.load_data(f"index_{code}", end_date)
                if cached_data is not None:
                    result[name] = cached_data
                    continue
            
            # 尝试从本地文件获取数据
            if code in self.available_stocks:
                df = self._read_stock_data(code)
                if not df.is_empty():
                    # 转换日期格式
                    start_date_obj = datetime.strptime(start_date, '%Y%m%d').date()
                    end_date_obj = datetime.strptime(end_date, '%Y%m%d').date()
                    
                    # 筛选日期范围
                    df = df.filter(
                        (pl.col('date') >= pl.lit(start_date_obj)) & 
                        (pl.col('date') <= pl.lit(end_date_obj))
                    )
                    
                    # 保存到缓存
                    self.cache.save_data(f"index_{code}", end_date, df)
                    result[name] = df
                    continue
            
            # 如果本地文件不存在，尝试使用 akshare 获取数据
            try:
                ak_code = code.replace('sh', 'sh').replace('sz', 'sz')
                df = ak.index_zh_a_hist(symbol=ak_code)
                df = pl.from_pandas(df)
                
                # 保存到缓存
                self.cache.save_data(f"index_{code}", end_date, df)
                result[name] = df
            except Exception as e:
                print(f"获取指数 {code} 数据失败: {str(e)}")
                result[name] = pl.DataFrame()
        
        return result
    
    def get_market_sentiment(self, date: str = None) -> dict:
        """获取市场情绪指标"""
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
            
        # 检查缓存
        if not self.cache.needs_update("sentiment", date):
            cached_data = self.cache.load_dict_data("sentiment", date, 
                ['limit_up', 'limit_down', 'market_overview', 'strong_stocks', 
                 'previous_limit_up', 'break_limit_up', 'big_deal'])
            if cached_data is not None:
                return cached_data
        
        try:
            # 获取新数据
            limit_up = ak.stock_zt_pool_em(date=date)
            limit_down = ak.stock_zt_pool_dtgc_em(date=date)
            market_data = ak.stock_zh_a_spot_em()
            strong_stocks = ak.stock_zt_pool_strong_em(date=date)
            previous_limit_up = ak.stock_zt_pool_previous_em(date=date)
            break_limit_up = ak.stock_zt_pool_zbgc_em(date=date)
            big_deal = ak.stock_fund_flow_big_deal()
            
            # 转换为pandas DataFrame，并检查是否为空
            limit_up_df = pd.DataFrame(limit_up) if not isinstance(limit_up, pd.DataFrame) else limit_up
            limit_down_df = pd.DataFrame(limit_down) if not isinstance(limit_down, pd.DataFrame) else limit_down
            market_data_df = pd.DataFrame(market_data) if not isinstance(market_data, pd.DataFrame) else market_data
            strong_stocks_df = pd.DataFrame(strong_stocks) if not isinstance(strong_stocks, pd.DataFrame) else strong_stocks
            previous_limit_up_df = pd.DataFrame(previous_limit_up) if not isinstance(previous_limit_up, pd.DataFrame) else previous_limit_up
            break_limit_up_df = pd.DataFrame(break_limit_up) if not isinstance(break_limit_up, pd.DataFrame) else break_limit_up
            big_deal_df = pd.DataFrame(big_deal) if not isinstance(big_deal, pd.DataFrame) else big_deal
            
            # 转换为polars DataFrame，并确保非空
            result = {}
            
            # 转换limit_up
            if not limit_up_df.empty:
                result['limit_up'] = pl.from_pandas(limit_up_df)
            else:
                result['limit_up'] = pl.DataFrame()
                
            # 转换limit_down
            if not limit_down_df.empty:
                result['limit_down'] = pl.from_pandas(limit_down_df)
            else:
                result['limit_down'] = pl.DataFrame()
                
            # 转换market_overview
            if not market_data_df.empty:
                # 确保 market_overview 中有 'amount' 或 '成交额' 列
                market_overview = pl.from_pandas(market_data_df)
                
                # 检查是否有成交额列
                has_amount_col = False
                for col in ['成交额', 'amount', 'trade_amount', '成交金额']:
                    if col in market_overview.columns:
                        has_amount_col = True
                        break
                
                # 如果没有成交额列，添加一个默认值为0的列
                if not has_amount_col:
                    market_overview = market_overview.with_columns([
                        pl.lit(0).alias('成交额')
                    ])
                    print("警告: market_overview 中没有成交额列，已添加默认值为0的列")
                
                # 检查并处理日期列
                date_cols = []
                for col in market_overview.columns:
                    if col.lower() in ['date', '日期', 'trade_date', '交易日期']:
                        date_cols.append(col)
                
                # 如果有日期列，确保它们是字符串类型
                for date_col in date_cols:
                    col_type = market_overview[date_col].dtype
                    if col_type in [pl.Date, pl.Datetime]:
                        print(f"将market_overview中的{date_col}列从日期类型转换为字符串类型")
                        market_overview = market_overview.with_columns([
                            pl.col(date_col).dt.strftime('%Y-%m-%d').alias(date_col)
                        ])
                
                # 检查并处理代码列
                code_cols = []
                for col in market_overview.columns:
                    if col.lower() in ['code', '代码', 'stock_code', '股票代码']:
                        code_cols.append(col)
                
                # 如果有代码列，确保它们是字符串类型
                for code_col in code_cols:
                    col_type = market_overview[code_col].dtype
                    if col_type != pl.Utf8:
                        print(f"将market_overview中的{code_col}列转换为字符串类型")
                        market_overview = market_overview.with_columns([
                            pl.col(code_col).cast(pl.Utf8).alias(code_col)
                        ])
                
                result['market_overview'] = market_overview
            else:
                # 创建一个带有默认列的空 DataFrame
                result['market_overview'] = pl.DataFrame({
                    '代码': [],
                    '名称': [],
                    '涨跌幅': [],
                    '成交额': []
                })
                
            # 转换strong_stocks
            if not strong_stocks_df.empty:
                result['strong_stocks'] = pl.from_pandas(strong_stocks_df)
            else:
                result['strong_stocks'] = pl.DataFrame()
                
            # 转换previous_limit_up
            if not previous_limit_up_df.empty:
                result['previous_limit_up'] = pl.from_pandas(previous_limit_up_df)
            else:
                result['previous_limit_up'] = pl.DataFrame()
                
            # 转换break_limit_up
            if not break_limit_up_df.empty:
                result['break_limit_up'] = pl.from_pandas(break_limit_up_df)
            else:
                result['break_limit_up'] = pl.DataFrame()
                
            # 转换big_deal
            if not big_deal_df.empty:
                result['big_deal'] = pl.from_pandas(big_deal_df)
            else:
                result['big_deal'] = pl.DataFrame()
            
            # 保存到缓存
            self.cache.save_dict_data("sentiment", date, result)
            return result
            
        except Exception as e:
            print(f"获取市场情绪数据失败: {str(e)}")
            import traceback
            traceback.print_exc()
            # 返回空数据框
            return {
                'limit_up': pl.DataFrame(),
                'limit_down': pl.DataFrame(),
                'market_overview': pl.DataFrame({
                    '代码': [],
                    '名称': [],
                    '涨跌幅': [],
                    '成交额': []
                }),
                'strong_stocks': pl.DataFrame(),
                'previous_limit_up': pl.DataFrame(),
                'break_limit_up': pl.DataFrame(),
                'big_deal': pl.DataFrame()
            }
    
    def fetch_stock_details(self, date: str = None) -> Dict[str, pl.DataFrame]:
        """获取个股详细数据"""
        try:
            # 获取个股历史数据
            stock_details = {}
            
            # 获取昨日涨停股票列表
            sentiment_data = self.get_market_sentiment(date)
            previous_limit_up = sentiment_data.get('previous_limit_up', pl.DataFrame())
            
            # 检查是否为空DataFrame
            if previous_limit_up is None or not isinstance(previous_limit_up, pl.DataFrame) or previous_limit_up.is_empty():
                print("未获取到昨日涨停股票列表")
                return {}
            
            # 确定代码列名
            code_col = None
            for col in ['代码', '股票代码', 'code', 'symbol']:
                if col in previous_limit_up.columns:
                    code_col = col
                    break
            
            if code_col is None:
                print("无法确定股票代码列")
                return {}
                
            stock_codes = previous_limit_up[code_col].to_list()
            print(f"开始获取 {len(stock_codes)} 只股票的历史数据")
            
            for code in stock_codes:
                try:
                    print(f"正在获取股票 {code} 的历史数据...")
                    
                    # 检查缓存
                    cache_key = f"stock_detail_{code}"
                    if not self.cache.needs_update(cache_key, date):
                        cached_data = self.cache.load_data(cache_key, date)
                        if cached_data is not None and isinstance(cached_data, pl.DataFrame) and not cached_data.is_empty():
                            print(f"从缓存加载股票 {code} 的历史数据")
                            stock_details[code] = cached_data
                            continue
                    
                    # 获取最近30个交易日的数据
                    start_date = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
                    end_date = datetime.now().strftime("%Y%m%d")
                    
                    print(f"获取股票 {code} 从 {start_date} 到 {end_date} 的历史数据")
                    df = ak.stock_zh_a_hist(symbol=code, period="daily", 
                                          start_date=start_date,
                                          end_date=end_date,
                                          adjust="qfq")
                    
                    # 检查是否为空
                    df_pd = pd.DataFrame(df) if not isinstance(df, pd.DataFrame) else df
                    if df_pd.empty:
                        print(f"股票{code}返回的数据为空")
                        continue
                        
                    # 转换为polars DataFrame并确保列名正确
                    df = pl.from_pandas(df_pd)
                    
                    # 检查必要的列是否存在
                    required_columns = ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
                    missing_columns = [col for col in required_columns if col not in df.columns]
                    if missing_columns:
                        print(f"股票{code}数据缺少必要的列: {missing_columns}")
                        print(f"实际列名: {df.columns}")
                        continue
                    
                    # 重命名列
                    column_mapping = {
                        '日期': 'date',
                        '开盘': 'open',
                        '收盘': 'close',
                        '最高': 'high',
                        '最低': 'low',
                        '成交量': 'volume',
                        '成交额': 'amount',
                        '振幅': 'amplitude',
                        '涨跌幅': 'change_pct',
                        '涨跌额': 'change_amount',
                        '换手率': 'turnover'
                    }
                    
                    df = df.rename(column_mapping)
                    
                    # 确保日期列格式正确
                    if df['date'].dtype == pl.Utf8:
                        try:
                            print(f"股票 {code} 将日期列从字符串转换为日期类型")
                            df = df.with_columns([
                                pl.col('date').str.strptime(pl.Date, '%Y-%m-%d').alias('date')
                            ])
                        except Exception as e:
                            print(f"股票{code}日期格式转换失败: {str(e)}")
                            print(f"日期示例: {df['date'].head(5)}")
                            continue
                    
                    # 按日期排序
                    df = df.sort('date')
                    
                    # 保存到缓存
                    self.cache.save_data(cache_key, date, df)
                    stock_details[code] = df
                    print(f"成功获取并处理股票 {code} 的历史数据")
                    
                except Exception as e:
                    print(f"获取股票{code}数据失败: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            if not stock_details:
                print("未能成功获取任何股票的历史数据")
            else:
                print(f"成功获取了{len(stock_details)}只股票的历史数据")
                
            return stock_details
            
        except Exception as e:
            print(f"获取个股详细数据失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return {}
    
    def get_sector_data(self) -> dict:
        """获取行业板块数据"""
        # 这个方法需要行业板块的映射关系，简化处理，直接使用 akshare
        return DataFetcher().get_sector_data()

class DataFetcher:
    """数据获取器 - 整合所有数据获取和管理功能"""
    def __init__(self):
        """初始化数据获取器"""
        self.cache = DataCache()
        self.stock_metadata_manager = StockMetadataManager()
        self.index_metadata_manager = IndexMetadataManager()
        self.market_metadata_manager = MarketMetadataManager()
        self.sector_data_manager = SectorDataManager()

    def check_and_update_metadata(self, progress_callback=None):
        """检查并更新所有元数据"""
        print("🔄 开始检查并更新所有元数据...")

        if progress_callback:
            progress_callback(0, 100, "开始检查元数据更新需求...")

        # 1. 检查并更新股票元数据
        stock_metadata_updated = True
        if not self.stock_metadata_manager.is_latest_trading_day():
            print("📊 检测到股票元数据需要更新...")
            if progress_callback:
                progress_callback(10, 100, "正在更新股票元数据...")
            stock_metadata_updated = self.stock_metadata_manager.update_metadata(progress_callback=progress_callback)
            print("✅ 股票元数据更新完成" if stock_metadata_updated else "❌ 股票元数据更新失败")
        else:
            print("✅ 股票元数据已是最新，无需更新")
            if progress_callback:
                progress_callback(20, 100, "股票元数据检查完成")

        # 2. 检查并更新指数元数据
        index_metadata_updated = True
        if not self.index_metadata_manager.is_latest_trading_day():
            print("📊 检测到指数元数据需要更新...")
            if progress_callback:
                progress_callback(30, 100, "正在更新指数元数据...")
            index_metadata_updated = self.index_metadata_manager.update_metadata(
                progress_callback=lambda current, total, message:
                    progress_callback(30 + int(current / 3), 100, message)
                    if progress_callback else None
            )
            print("✅ 指数元数据更新完成" if index_metadata_updated else "❌ 指数元数据更新失败")
        else:
            print("✅ 指数元数据已是最新，无需更新")
            if progress_callback:
                progress_callback(50, 100, "指数元数据检查完成")

        # 3. 检查并更新市场状态数据
        market_states_updated = True
        if not self.market_metadata_manager.is_latest_trading_day():
            print("📊 检测到市场状态数据需要更新...")
            if progress_callback:
                progress_callback(60, 100, "正在更新市场状态数据...")
            try:
                market_states_updated = self.market_metadata_manager.update_market_states_incremental()
                print("✅ 市场状态数据更新完成" if market_states_updated else "❌ 市场状态数据更新失败")
            except Exception as e:
                print(f"❌ 更新市场状态数据时出错: {str(e)}")
                import traceback
                traceback.print_exc()
                market_states_updated = False
        else:
            print("✅ 市场状态数据已是最新，无需更新")
            if progress_callback:
                progress_callback(70, 100, "市场状态数据检查完成")

        # 4. 检查并更新板块数据（包含行业和概念板块）
        sector_updated = True
        # 检查板块数据是否需要更新
        try:
            # 使用SectorDataManager的is_latest_trading_day方法
            if not self.sector_data_manager.is_latest_trading_day():
                print("📊 检测到板块数据需要更新...")
                if progress_callback:
                    progress_callback(80, 100, "正在更新板块数据...")
                try:
                    sector_updated = self.sector_data_manager.update_sector_data()
                    print("✅ 板块数据更新完成" if sector_updated else "❌ 板块数据更新失败")
                except Exception as e:
                    print(f"❌ 更新板块数据时出错: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    sector_updated = False
            else:
                print("✅ 板块数据已是最新，无需更新")
                if progress_callback:
                    progress_callback(85, 100, "板块数据检查完成")
        except Exception as e:
            print(f"❌ 检查板块数据时出错: {str(e)}")
            sector_updated = False

        # 5. 检查并更新市场元数据
        market_metadata_updated = True
        if not self.market_metadata_manager.is_latest_trading_day():
            print("📊 检测到市场元数据需要更新...")
            if progress_callback:
                progress_callback(90, 100, "正在更新市场元数据...")
            market_metadata_updated = self.market_metadata_manager.update_metadata(
                progress_callback=lambda current, total, message:
                    progress_callback(90 + int(current / 10), 100, message)
                    if progress_callback else None
            )
            print("✅ 市场元数据更新完成" if market_metadata_updated else "❌ 市场元数据更新失败")
        else:
            print("✅ 市场元数据已是最新，无需更新")

        # 完成所有更新
        if progress_callback:
            progress_callback(100, 100, "所有数据更新检查完成")

        print("🎉 所有元数据更新检查完成")

        # 返回所有更新是否成功
        all_success = (stock_metadata_updated and index_metadata_updated and
                      market_states_updated and sector_updated and market_metadata_updated)

        return all_success

    def get_market_sentiment(self, date: str = None) -> dict:
        """获取市场情绪指标"""
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
            
        # 检查缓存
        if not self.cache.needs_update("sentiment", date):
            cached_data = self.cache.load_dict_data("sentiment", date, 
                ['limit_up', 'limit_down', 'market_overview', 'strong_stocks', 
                 'previous_limit_up', 'break_limit_up', 'big_deal'])
            if cached_data is not None:
                return cached_data
        
        try:
            # 获取新数据
            limit_up = ak.stock_zt_pool_em(date=date)
            limit_down = ak.stock_zt_pool_dtgc_em(date=date)
            market_data = ak.stock_zh_a_spot_em()
            strong_stocks = ak.stock_zt_pool_strong_em(date=date)
            previous_limit_up = ak.stock_zt_pool_previous_em(date=date)
            break_limit_up = ak.stock_zt_pool_zbgc_em(date=date)
            big_deal = ak.stock_fund_flow_big_deal()
            
            # 转换为pandas DataFrame，并检查是否为空
            limit_up_df = pd.DataFrame(limit_up) if not isinstance(limit_up, pd.DataFrame) else limit_up
            limit_down_df = pd.DataFrame(limit_down) if not isinstance(limit_down, pd.DataFrame) else limit_down
            market_data_df = pd.DataFrame(market_data) if not isinstance(market_data, pd.DataFrame) else market_data
            strong_stocks_df = pd.DataFrame(strong_stocks) if not isinstance(strong_stocks, pd.DataFrame) else strong_stocks
            previous_limit_up_df = pd.DataFrame(previous_limit_up) if not isinstance(previous_limit_up, pd.DataFrame) else previous_limit_up
            break_limit_up_df = pd.DataFrame(break_limit_up) if not isinstance(break_limit_up, pd.DataFrame) else break_limit_up
            big_deal_df = pd.DataFrame(big_deal) if not isinstance(big_deal, pd.DataFrame) else big_deal
            
            # 转换为polars DataFrame，并确保非空
            result = {}
            
            # 转换limit_up
            if not limit_up_df.empty:
                result['limit_up'] = pl.from_pandas(limit_up_df)
            else:
                result['limit_up'] = pl.DataFrame()
                
            # 转换limit_down
            if not limit_down_df.empty:
                result['limit_down'] = pl.from_pandas(limit_down_df)
            else:
                result['limit_down'] = pl.DataFrame()
                
            # 转换market_overview
            if not market_data_df.empty:
                # 确保 market_overview 中有 'amount' 或 '成交额' 列
                market_overview = pl.from_pandas(market_data_df)
                
                # 检查是否有成交额列
                has_amount_col = False
                for col in ['成交额', 'amount', 'trade_amount', '成交金额']:
                    if col in market_overview.columns:
                        has_amount_col = True
                        break
                
                # 如果没有成交额列，添加一个默认值为0的列
                if not has_amount_col:
                    market_overview = market_overview.with_columns([
                        pl.lit(0).alias('成交额')
                    ])
                    print("警告: market_overview 中没有成交额列，已添加默认值为0的列")
                
                result['market_overview'] = market_overview
            else:
                # 创建一个带有默认列的空 DataFrame
                result['market_overview'] = pl.DataFrame({
                    '代码': [],
                    '名称': [],
                    '涨跌幅': [],
                    '成交额': []
                })
                
            # 转换strong_stocks
            if not strong_stocks_df.empty:
                result['strong_stocks'] = pl.from_pandas(strong_stocks_df)
            else:
                result['strong_stocks'] = pl.DataFrame()
                
            # 转换previous_limit_up
            if not previous_limit_up_df.empty:
                result['previous_limit_up'] = pl.from_pandas(previous_limit_up_df)
            else:
                result['previous_limit_up'] = pl.DataFrame()
                
            # 转换break_limit_up
            if not break_limit_up_df.empty:
                result['break_limit_up'] = pl.from_pandas(break_limit_up_df)
            else:
                result['break_limit_up'] = pl.DataFrame()
                
            # 转换big_deal
            if not big_deal_df.empty:
                result['big_deal'] = pl.from_pandas(big_deal_df)
            else:
                result['big_deal'] = pl.DataFrame()
            
            # 保存到缓存
            self.cache.save_dict_data("sentiment", date, result)
            return result
            
        except Exception as e:
            print(f"获取市场情绪数据失败: {str(e)}")
            import traceback
            traceback.print_exc()
            # 返回空数据框
            return {
                'limit_up': pl.DataFrame(),
                'limit_down': pl.DataFrame(),
                'market_overview': pl.DataFrame({
                    '代码': [],
                    '名称': [],
                    '涨跌幅': [],
                    '成交额': []
                }),
                'strong_stocks': pl.DataFrame(),
                'previous_limit_up': pl.DataFrame(),
                'break_limit_up': pl.DataFrame(),
                'big_deal': pl.DataFrame()
            }
    
    # ========== 板块数据管理方法 ==========


    def get_combined_sectors_summary(self, date_str: str = None, include_sectors: bool = True, include_concepts: bool = True, days_back: int = 30) -> Dict:
        """获取合并板块数据摘要（行业+概念）

        Args:
            date_str: 指定日期（格式：YYYY-MM-DD），如果为None则使用最新数据
            include_sectors: 是否包含行业板块
            include_concepts: 是否包含概念板块
            days_back: 当date_str为None时，加载最近多少天的数据
        """
        try:
            from datetime import datetime

            # 使用统一数据加载方法
            if date_str:
                unified_data = self.sector_data_manager.load_sector_data(
                    days_back=60,  # 加载更多天数以确保包含目标日期
                    include_sectors=include_sectors,
                    include_concepts=include_concepts,
                    target_date=date_str  # 修正：传递target_date
                )
                target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            else:
                # 最新数据模式：使用指定的days_back
                unified_data = self.sector_data_manager.load_sector_data(
                    days_back=days_back,
                    include_sectors=include_sectors,
                    include_concepts=include_concepts
                )
                # 使用最新日期
                if not unified_data.is_empty():
                    target_date = unified_data['日期'].max()
                else:
                    target_date = datetime.now().date()

            if unified_data.is_empty():
                return {
                    'top_sectors': [],
                    'fund_flow': [],
                    'performance': [],
                    'trend_analysis': [],
                    'summary': {
                        'total_sectors': 0,
                        'up_sectors': 0,
                        'down_sectors': 0,
                        'avg_change': 0.0
                    }
                }

            # 筛选指定日期的数据
            target_data = unified_data.filter(pl.col('日期') == target_date)

            if target_data.is_empty():
                # 如果指定日期没有数据，尝试找最接近的日期
                available_dates = unified_data['日期'].unique().sort()
                closest_date = None

                for date in available_dates:
                    if date <= target_date:
                        closest_date = date
                    else:
                        break

                if closest_date:
                    print(f"⚠️ 指定日期 {date_str} 无数据，使用最接近的日期 {closest_date}")
                    target_data = unified_data.filter(pl.col('日期') == closest_date)
                else:
                    return {
                        'top_sectors': [],
                        'fund_flow': [],
                        'performance': [],
                        'trend_analysis': [],
                        'summary': {
                            'total_sectors': 0,
                            'up_sectors': 0,
                            'down_sectors': 0,
                            'avg_change': 0.0
                        }
                    }

            # 板块数据已经包含5日和10日涨跌幅以及成交额量比，直接使用
            target_data_with_periods = target_data

            # 成交额量比也已经在数据中，无需重新计算
            target_data_with_volume_ratio = target_data_with_periods

            # 处理数据格式化（移除股票数量计算以提升性能）
            combined_top_sectors = []
            for row in target_data_with_volume_ratio.to_dicts():
                # 先保留原始数据，处理日期序列化
                formatted_row = {}
                for key, value in row.items():
                    if hasattr(value, 'strftime'):  # 处理日期对象
                        formatted_row[key] = value.strftime('%Y-%m-%d')
                    else:
                        formatted_row[key] = value

                # 保留原始数据，让前端处理格式化
                formatted_row['成交额量比'] = row.get('成交额量比')  # 添加成交额量比字段
                formatted_row['成交额'] = row.get('成交额') or 0
                formatted_row['成交量'] = row.get('成交量') or 0
                formatted_row['涨跌幅'] = row.get('涨跌幅') or 0
                formatted_row['5日涨跌幅'] = row.get('5日涨跌幅') or 0
                formatted_row['10日涨跌幅'] = row.get('10日涨跌幅') or 0
                formatted_row['换手率'] = row.get('换手率') or 0
                formatted_row['振幅'] = row.get('振幅') or 0
                formatted_row['最新价'] = row.get('收盘') or 0

                # 移除股票数量计算以提升性能
                # formatted_row['股票数量'] = 0  # 不再计算股票数量

                combined_top_sectors.append(formatted_row)

            # 按涨跌幅排序（处理None值）
            def safe_float(value):
                if value is None:
                    return 0.0
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return 0.0

            combined_top_sectors.sort(key=lambda x: safe_float(x.get('涨跌幅', 0)), reverse=True)

            # 计算统计信息（使用原始数值而不是格式化后的字符串）
            total_sectors = len(combined_top_sectors)
            up_sectors = len([s for s in combined_top_sectors if safe_float(s.get('涨跌幅_原始', 0)) > 0])
            down_sectors = len([s for s in combined_top_sectors if safe_float(s.get('涨跌幅_原始', 0)) < 0])
            avg_change = sum(safe_float(s.get('涨跌幅_原始', 0)) for s in combined_top_sectors) / total_sectors if total_sectors > 0 else 0

            return {
                'top_sectors': combined_top_sectors,
                'fund_flow': [],  # 暂时为空，可以后续扩展
                'performance': [],  # 暂时为空，可以后续扩展
                'trend_analysis': [],  # 暂时为空，可以后续扩展
                'summary': {
                    'total_sectors': total_sectors,
                    'up_sectors': up_sectors,
                    'down_sectors': down_sectors,
                    'avg_change': round(avg_change, 2)
                }
            }

        except Exception as e:
            print(f"❌ 获取指定日期板块摘要失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                'top_sectors': [],
                'fund_flow': [],
                'performance': [],
                'trend_analysis': [],
                'summary': {
                    'total_sectors': 0,
                    'up_sectors': 0,
                    'down_sectors': 0,
                    'avg_change': 0.0
                }
            }

    def get_sectors_custom_period(self, start_date: str, end_date: str, include_sectors: bool = True, include_concepts: bool = True) -> List[Dict]:
        """获取板块自定义区间涨跌幅"""
        try:
            # 加载统一板块数据
            unified_data = self.sector_data_manager.load_sector_data(
                include_sectors=include_sectors,
                include_concepts=include_concepts
            )

            if unified_data.is_empty():
                return []

            # 转换日期格式
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

            # 筛选日期范围内的数据
            period_data = unified_data.filter(
                (pl.col('日期') >= start_dt) & (pl.col('日期') <= end_dt)
            )

            if period_data.is_empty():
                return []

            # 按板块名称分组计算区间涨跌幅
            result = []

            for sector_name in period_data['板块名称'].unique():
                sector_data = period_data.filter(pl.col('板块名称') == sector_name).sort('日期')

                if sector_data.height < 2:
                    continue

                # 获取起始和结束价格
                start_price = sector_data.select(pl.col('收盘')).to_series()[0]
                end_price = sector_data.select(pl.col('收盘')).to_series()[-1]

                if start_price and end_price and start_price != 0:
                    # 计算区间涨跌幅
                    period_change = ((end_price - start_price) / start_price * 100)

                    # 获取板块类型
                    sector_type = sector_data.select(pl.col('板块类型')).to_series()[0]

                    # 获取起始和结束日期
                    start_date = sector_data.select(pl.col('日期')).to_series()[0]
                    end_date = sector_data.select(pl.col('日期')).to_series()[-1]

                    result.append({
                        '板块名称': sector_name,
                        '板块类型': sector_type,
                        '区间涨跌幅': round(period_change, 2),
                        '起始价格': round(start_price, 2),
                        '结束价格': round(end_price, 2),
                        '起始日期': start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else str(start_date),
                        '结束日期': end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else str(end_date)
                    })

            # 按区间涨跌幅降序排序
            result.sort(key=lambda x: x['区间涨跌幅'], reverse=True)

            return result

        except Exception as e:
            print(f"❌ 获取自定义区间板块数据失败: {e}")
            import traceback
            traceback.print_exc()
            return []

    def get_sector_kline_data(self, sector_name: str, days_back: int = 30, target_date: str = None) -> pl.DataFrame:
        """
        获取单个板块的K线数据，用于前端原生ECharts渲染
        
        Args:
            sector_name: 板块名称
            days_back: 获取最近多少天的数据
            target_date: 目标日期（可选），如果不指定则使用最新日期
            
        Returns:
            pl.DataFrame: 板块K线数据
        """
        try:
            # 调用SectorDataManager获取K线数据
            return self.sector_data_manager.get_sector_kline_data(sector_name, days_back, target_date)
        except Exception as e:
            print(f"❌ DataFetcher获取板块K线数据失败: {e}")
            import traceback
            traceback.print_exc()
            return pl.DataFrame()

def create_data_fetcher(use_local_file: bool = False, local_file_path: str = "E:/jupyter/大A/all_stock_candle/stock/daily") -> Union[DataFetcher, LocalFileDataFetcher]:
    """创建数据获取器"""
    if use_local_file:
        try:
            print(f"尝试使用本地文件数据获取器，路径: {local_file_path}")
            return LocalFileDataFetcher(local_file_path)
        except Exception as e:
            print(f"初始化本地文件数据获取器失败: {str(e)}")
            print("将使用默认数据获取器")
    
    # 返回默认数据获取器
    print("使用AKShare数据获取器")
    return DataFetcher()
