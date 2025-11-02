#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票元数据管理器

主板使用baostock，北交所使用ak.stock_zh_a_hist接口
"""

import polars as pl
from datetime import datetime, timedelta, date
from pathlib import Path
import os
import time
from typing import Optional, Dict, List, Tuple
import akshare as ak
import pandas as pd
import numpy as np
import baostock as bs
import threading
import tempfile
import shutil
# from akshare.utils.requests_fun import requests_obj

# requests_obj.headers.update({
#     "User-Agent": "Mozilla/5.0 ...",
#     "Referer": "https://quote.eastmoney.com/"
# })
def safe_network_request(func, *args, max_retries=3, timeout_seconds=30, **kwargs):
    """安全的网络请求包装器，带重试机制"""
    import time
    import random

    for attempt in range(max_retries):
        try:
            result = func(*args, **kwargs)
            return result

        except Exception as e:
            print(f"⚠️ 网络请求失败 (尝试 {attempt + 1}/{max_retries}): {e}")

            if attempt < max_retries - 1:
                # 指数退避重试
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"⏳ 等待 {wait_time:.1f} 秒后重试...")
                time.sleep(wait_time)
            else:
                print(f"❌ 网络请求最终失败，已重试 {max_retries} 次")

    return None


try:
    import fcntl
except ImportError:
    fcntl = None  # Windows doesn't have fcntl

# 全局锁，防止并发写入
_file_locks = {}
_lock_mutex = threading.Lock()


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





class StockMetadataManager:
    """股票元数据管理类，主板使用baostock,北交所使用ak.stock_zh_a_hist接口"""

    def __init__(self, metadata_path: str = None):
        if metadata_path is None:
            # 使用项目根目录的data_cache
            self.metadata_path = Path("data_cache/stock_daily")
        else:
            self.metadata_path = Path(metadata_path)

        # 确保目录存在
        self.metadata_path.mkdir(parents=True, exist_ok=True)

        # 股票元数据文件路径
        self.stock_metadata_file = self.metadata_path / "stock_daily_metadata.parquet"

        print(f"📊 股票元数据管理器初始化完成")

    def load_metadata(self) -> Optional[pl.DataFrame]:
        """加载股票元数据"""
        try:
            if self.stock_metadata_file.exists():
                df = pl.read_parquet(self.stock_metadata_file)
                print(f"✅ 成功加载股票元数据: {df.height} 条记录")
                return df
            else:
                print(f"⚠️ 股票元数据文件不存在: {self.stock_metadata_file}")
                return None
        except Exception as e:
            print(f"❌ 加载股票元数据失败: {e}")
            return None

    def is_latest_trading_day(self) -> bool:
        """检查股票元数据是否是最新交易日的数据

        逻辑：
        1. 获取现有数据的最新日期
        2. 获取当前应该更新到的最新交易日期
        3. 判断当天是否为交易日，是否已过18:00
        4. 考虑周末和节假日的影响
        """
        try:
            # 1. 获取现有数据的最新日期
            metadata = self.load_metadata()
            if metadata is None or metadata.is_empty():
                print("股票元数据为空，需要更新")
                return False

            if '日期' not in metadata.columns:
                print("警告: 股票元数据中缺少日期列")
                return False

            # 解析现有数据的最新日期
            latest_date_raw = metadata['日期'].max()
            if isinstance(latest_date_raw, str):
                try:
                    latest_local_date = datetime.strptime(latest_date_raw, '%Y-%m-%d').date()
                except ValueError:
                    try:
                        latest_local_date = datetime.strptime(latest_date_raw, '%Y/%m/%d').date()
                    except ValueError:
                        latest_local_date = datetime.strptime(latest_date_raw, '%Y%m%d').date()
            elif isinstance(latest_date_raw, datetime):
                latest_local_date = latest_date_raw.date()
            elif isinstance(latest_date_raw, date):
                latest_local_date = latest_date_raw
            else:
                print(f"⚠️ 未知的日期类型: {type(latest_date_raw)}, 值: {latest_date_raw}")
                return False

            # 2. 获取当前时间信息
            now = datetime.now()
            current_date = now.date()
            current_time = now.time()

            # 3. 定义数据更新时间（18:00后认为当日数据已更新）
            from datetime import time as dt_time
            update_time = dt_time(18, 0)  # 18:00

            # 4. 使用holidays库判断节假日
            def is_holiday(check_date):
                """使用holidays库判断是否为中国节假日"""
                try:
                    import holidays
                    # 创建中国节假日对象
                    china_holidays = holidays.China(years=check_date.year)
                    return check_date in china_holidays
                except Exception as e:
                    print(f"⚠️ 节假日判断失败: {e}")
                    return False

            # 5. 判断是否为交易日（非周末且非节假日）
            def is_trading_day(check_date):
                """判断是否为交易日"""
                # 周末不是交易日
                if check_date.weekday() >= 5:  # 5=周六, 6=周日
                    return False
                # 节假日不是交易日
                if is_holiday(check_date):
                    return False
                return True

            # 6. 获取前一个交易日
            def get_previous_trading_day(from_date):
                """获取指定日期前的最近一个交易日"""
                check_date = from_date - timedelta(days=1)
                for _ in range(15):  # 最多往前找15天（考虑长假期）
                    if is_trading_day(check_date):
                        return check_date
                    check_date -= timedelta(days=1)
                # 如果15天内都没找到，返回15天前的日期
                return from_date - timedelta(days=15)

            # 7. 获取最新应该更新到的交易日期
            def get_latest_expected_trading_date():
                """获取最新应该更新到的交易日期"""
                if is_trading_day(current_date):
                    # 今天是交易日
                    if current_time >= update_time:
                        # 已过18:00，应该有今天的数据
                        return current_date
                    else:
                        # 未过18:00，应该有前一个交易日的数据
                        return get_previous_trading_day(current_date)
                else:
                    # 今天不是交易日，应该有最近一个交易日的数据
                    return get_previous_trading_day(current_date)

            # 8. 比较现有数据的最新日期与最新交易日，判断是否需要更新
            expected_latest_date = get_latest_expected_trading_date()

            print(f"📊 现有数据最新日期: {latest_local_date}")
            print(f"📊 最新交易日期: {expected_latest_date}")


            # 如果现有数据日期 >= 最新交易日期，则认为是最新的
            is_latest = latest_local_date >= expected_latest_date

            if is_latest:
                print("✅ 股票元数据已是最新，无需更新")
            else:
                print("📊 股票元数据需要更新")

            return is_latest

        except Exception as e:
            print(f"❌ 检查是否为最新交易日失败: {e}")
            return False

    def update_metadata(self, start_date: str = None, end_date: str = None, 
                       progress_callback=None) -> bool:
        """更新股票元数据"""
        if progress_callback:
            progress_callback(0, 100, "开始更新股票元数据")
        
        # 获取现有元数据
        existing_metadata = self.load_metadata()
        
        # 确定日期范围
        if start_date is None or end_date is None:
            latest_date = None
            if existing_metadata is not None and not existing_metadata.is_empty():
                if '日期' in existing_metadata.columns:
                    latest_date = existing_metadata['日期'].max()
                    if isinstance(latest_date, str):
                        try:
                            latest_date = datetime.strptime(latest_date, '%Y-%m-%d').date()
                        except Exception:
                            latest_date = None

            if latest_date is None:
                latest_date = (datetime.now() - timedelta(days=30)).date()

            if start_date is None:
                start_date = (latest_date + timedelta(days=1)).strftime('%Y-%m-%d')
            if end_date is None:
                # 计算应当更新到的“最新交易日”
                from datetime import time as dt_time
                try:
                    from utils.holiday_utils import china_holiday_util
                except Exception:
                    china_holiday_util = None

                now_dt = datetime.now()
                current_date = now_dt.date()
                update_time = dt_time(18, 0)

                def get_prev_trading_day(d):
                    if china_holiday_util is not None:
                        return china_holiday_util.get_previous_trading_day(d)
                    # 兜底：仅根据周末回退
                    check = d - timedelta(days=1)
                    for _ in range(15):
                        if check.weekday() < 5:
                            return check
                        check -= timedelta(days=1)
                    return d - timedelta(days=1)

                def is_trading(d):
                    if china_holiday_util is not None:
                        return china_holiday_util.is_trading_day(d)
                    return d.weekday() < 5

                if is_trading(current_date):
                    if now_dt.time() >= update_time:
                        expected = current_date
                    else:
                        expected = get_prev_trading_day(current_date)
                else:
                    expected = get_prev_trading_day(current_date)

                end_date = expected.strftime('%Y-%m-%d')
        else:
            # 如果传入的end_date是非交易日，则回退到上一个交易日
            try:
                from utils.holiday_utils import china_holiday_util
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
                if not china_holiday_util.is_trading_day(end_date_obj):
                    end_date = china_holiday_util.get_previous_trading_day(end_date_obj).strftime('%Y-%m-%d')
            except Exception:
                pass

        # 防止开始日期晚于结束日期
        try:
            if datetime.strptime(start_date, '%Y-%m-%d') > datetime.strptime(end_date, '%Y-%m-%d'):
                start_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        except Exception:
            pass

        print(start_date, end_date)

        #### 登陆系统 ####
        lg = bs.login()
        print('login respond error_code:'+lg.error_code)
        print('login respond error_msg:'+lg.error_msg)

        #### 获取所有A股股票代码和名称（在非交易日自动回退） ####
        max_back_steps = 5
        attempt_end_date = end_date
        stock_rs = None
        for _ in range(max_back_steps):
            rs = bs.query_all_stock(day=attempt_end_date)
            # baostock若返回错误，直接回退到上一个交易日
            if hasattr(rs, 'error_code') and rs.error_code != '0':
                print(f"获取股票列表失败: {getattr(rs, 'error_msg', '')}，回退重试: {attempt_end_date}")
                try:
                    from utils.holiday_utils import china_holiday_util
                    d = datetime.strptime(attempt_end_date, '%Y-%m-%d').date()
                    attempt_end_date = china_holiday_util.get_previous_trading_day(d).strftime('%Y-%m-%d')
                except Exception:
                    d = datetime.strptime(attempt_end_date, '%Y-%m-%d').date() - timedelta(days=1)
                    attempt_end_date = d.strftime('%Y-%m-%d')
                continue

            stock_rs = rs.get_data()
            # 确保是DataFrame
            if not isinstance(stock_rs, pd.DataFrame):
                stock_rs = pd.DataFrame(stock_rs)

            # 非空且包含code列则认为成功
            if stock_rs is not None and len(stock_rs) > 0 and ('code' in stock_rs.columns):
                break

            print(f"获取到0只股票或缺少code列，回退到上一个交易日重试: {attempt_end_date}")
            try:
                from utils.holiday_utils import china_holiday_util
                d = datetime.strptime(attempt_end_date, '%Y-%m-%d').date()
                attempt_end_date = china_holiday_util.get_previous_trading_day(d).strftime('%Y-%m-%d')
            except Exception:
                d = datetime.strptime(attempt_end_date, '%Y-%m-%d').date() - timedelta(days=1)
                attempt_end_date = d.strftime('%Y-%m-%d')

        # 更新最终使用的end_date为有效交易日
        end_date = attempt_end_date
        if stock_rs is None:
            stock_rs = pd.DataFrame()
        print(f"获取到{len(stock_rs)}只股票")

        #### 筛选A股股票（沪深两市） ####
        if isinstance(stock_rs, pd.DataFrame) and 'code' in stock_rs.columns:
            a_stocks = stock_rs[stock_rs['code'].astype(str).str.startswith(('sh.6', 'sz.0', 'sz.30'))]
            print(f"筛选后A股数量：{len(a_stocks)}")
        else:
            print("⚠️ 股票列表缺失code列，无法筛选A股，后续仅尝试北交所数据")
            a_stocks = pd.DataFrame(columns=['code', 'code_name'])

        #### 获取所有A股历史K线数据 ####
        all_data = []
        failed_stocks = []
        i=0
        for index, stock in a_stocks.iterrows():
            stock_code = stock['code']
            stock_name = stock['code_name']
            
            try:
                #print(f"正在获取 {stock_code} {stock_name} 的数据...")
                
                rs = bs.query_history_k_data_plus(stock_code,
                    "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg",
                    start_date=start_date, end_date=end_date,
                    frequency="d", adjustflag="2")
                
                if rs.error_code != '0':
                    print(f"获取 {stock_code} 数据失败: {rs.error_msg}")
                    failed_stocks.append((stock_code, stock_name))
                    continue
                    
                # 获取数据
                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())
                
                if data_list:
                    temp_df = pd.DataFrame(data_list, columns=rs.fields)
                    # 添加股票名称列
                    temp_df['名称'] = stock_name
                    all_data.append(temp_df)
                    i+=1
                    if i%100==0:
                        print(f"获取到{i}/{len(stock_rs)}只股票")
                        # 添加延时，避免请求过快
                        time.sleep(0.1)
                
            except Exception as e:
                print(f"处理 {stock_code} {stock_name} 时出错: {str(e)}")
                failed_stocks.append((stock_code, stock_name))
                continue
        
        # 获取所有股票列表（获取北交所股票数据）
        print('正在获取股票列表信息...')
        stock_info = safe_network_request(ak.stock_info_a_code_name, max_retries=3, timeout_seconds=60)

        if stock_info is not None and not stock_info.empty:
            print(f'成功获取股票列表，共 {len(stock_info)} 只股票')
        else:
            print("⚠️ 获取股票列表失败，跳过北交所数据更新")
            stock_info = pd.DataFrame()

        if not stock_info.empty:
            # 过滤code以4, 8, 9开头的行
            filtered_stocks = stock_info[stock_info['code'].str.startswith(('4', '8', '9'))]
            all_stock_data = []
            print(f'更新北交所数据，共 {len(filtered_stocks)} 只股票')

            for code in filtered_stocks['code']:
                try:
                    df = ak.stock_zh_a_hist(symbol=code, period='daily', start_date=datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y%m%d')
                                            , end_date=datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y%m%d'), adjust="qfq")
                    if not df.empty:
                        all_stock_data.append(df)
                except Exception as e:
                    print(f"获取股票 {code} 数据失败: {e}")
                    continue

            if all_stock_data:
                combined_df = pd.concat(all_stock_data, ignore_index=True)
                combined_df = combined_df.rename(columns={'股票代码': '代码',})
                # 用filtered_stocks里code和name做merge，补全名称字段
                merged_df = combined_df.merge(
                    filtered_stocks[['code', 'name']],
                    how='left',
                    left_on='代码',
                    right_on='code'
                )
                # 把原名称（假设是空的或缺失）替换为filtered_stocks里的name
                merged_df['名称'] = merged_df['name']

                # 删除多余的列 'code' 和 'name'
                merged_df.drop(['code', 'name'], axis=1, inplace=True)

                # 转换为polars
                beijiao_pl = pl.from_pandas(merged_df)

                # 保存为parquet
                #beijiao_pl.write_parquet("北交所股票历史行情.parquet")
                print(f'成功处理北交所数据，共 {len(combined_df)} 条记录')
            else:
                print("没有获取到北交所数据")
        else:
            print('跳过北交所数据处理（股票列表获取失败）')

        #### 合并所有新数据并转换为Polars格式 ####
        new_data_pl = None
        if all_data:
            # 合并所有pandas数据
            new_data_pd = pd.concat(all_data, ignore_index=True)
            # 在转换为Polars之前，先处理pandas DataFrame中的空值
            if 'volume' in new_data_pd.columns:
                new_data_pd = new_data_pd[(new_data_pd['volume'] != '') & (new_data_pd['volume'].notna())]

            # 转换为Polars DataFrame
            new_data_pl = pl.from_pandas(new_data_pd)

            # 重命名列名并调整列顺序，匹配现有parquet文件格式
            select_exprs = []
            if 'date' in new_data_pl.columns: select_exprs.append(pl.col("date").alias("日期"))
            if 'open' in new_data_pl.columns: select_exprs.append(pl.col("open").cast(pl.Float64).alias("开盘"))
            if 'close' in new_data_pl.columns: select_exprs.append(pl.col("close").cast(pl.Float64).alias("收盘"))
            if 'high' in new_data_pl.columns: select_exprs.append(pl.col("high").cast(pl.Float64).alias("最高"))
            if 'low' in new_data_pl.columns: select_exprs.append(pl.col("low").cast(pl.Float64).alias("最低"))
            if 'volume' in new_data_pl.columns: select_exprs.append(pl.col("volume").cast(pl.Int64).alias("成交量"))
            if 'amount' in new_data_pl.columns: select_exprs.append(pl.col("amount").cast(pl.Float64).alias("成交额"))
            if 'turn' in new_data_pl.columns: select_exprs.append(pl.col("turn").cast(pl.Float64).alias("换手率"))
            if 'pctChg' in new_data_pl.columns: select_exprs.append(pl.col("pctChg").cast(pl.Float64).alias("涨跌幅"))
            if '名称' in new_data_pl.columns: select_exprs.append(pl.col("名称"))
            if 'code' in new_data_pl.columns: select_exprs.append(pl.col("code").str.slice(3).alias("代码"))

            if select_exprs:
                new_data_pl = new_data_pl.select(select_exprs)

            # 计算振幅和涨跌额
            if all([c in new_data_pl.columns for c in ["最高", "最低", "收盘", "涨跌幅"]]):
                new_data_pl = new_data_pl.with_columns([
                    ((pl.col("最高") - pl.col("最低")) / pl.col("最低") * 100).round(2).alias("振幅"),
                    (pl.col("收盘") * pl.col("涨跌幅") / 100).round(2).alias("涨跌额")
                ])

            # 转换日期格式为Date类型
            if '日期' in new_data_pl.columns:
                new_data_pl = new_data_pl.with_columns([
                    pl.col("日期").str.strptime(pl.Date, format='%Y-%m-%d')
                ])
        else:
            print("未获取到主板新数据")

        # 统一对北交所数据进行列对齐
        # 若未成功获取北交所数据，则beijiao_pl可能未定义
        try:
            beijiao_pl
        except NameError:
            beijiao_pl = None

        # 组合可用的数据源
        frames = []
        base_columns = None
        if existing_metadata is not None and not existing_metadata.is_empty():
            frames.append(existing_metadata)
            base_columns = existing_metadata.columns
        if new_data_pl is not None and not new_data_pl.is_empty():
            if base_columns is None:
                base_columns = new_data_pl.columns
            # 对齐列
            new_cols = [c for c in base_columns if c in new_data_pl.columns]
            if new_cols:
                frames.append(new_data_pl.select(new_cols))
        if beijiao_pl is not None and not beijiao_pl.is_empty():
            if base_columns is None:
                base_columns = beijiao_pl.columns
            bj_cols = [c for c in base_columns if c in beijiao_pl.columns]
            if bj_cols:
                frames.append(beijiao_pl.select(bj_cols))

        if frames:
            combined = pl.concat(frames, how="vertical")
            if ('日期' in combined.columns) and ('代码' in combined.columns):
                combined = combined.unique(subset=["日期", "代码"], keep="last")
            print(f"合并后总记录数：{len(combined)}")
            combined.write_parquet('data_cache/stock_daily/stock_daily_metadata.parquet')
        else:
            print("没有可合并的数据，保持现有数据不变")

        #### 登出系统 ####
        bs.logout()
        
        # 若执行到此处，说明流程未抛出异常，视为更新成功
        return True