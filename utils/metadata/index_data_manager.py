
import os
from datetime import datetime, date, timedelta, time as dt_time
from pathlib import Path
from typing import Optional, Union, Dict, List, Tuple, Callable
import importlib
import polars as pl
import akshare as ak
import pandas as pd
import baostock as bs

requests_obj = None
try:
    requests_fun_module = importlib.import_module('akshare.utils.requests_fun')
    requests_obj = getattr(requests_fun_module, 'requests_obj', None)
except Exception:
    requests_obj = None

if requests_obj is not None:
    requests_obj.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Referer": "https://quote.eastmoney.com/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive"
    })

class IndexMetadataManager:
    """指数元数据管理类，基于ak.index_zh_a_hist接口"""
    def __init__(self, metadata_path: str = None):
        if metadata_path is None:
            # 使用项目根目录的data_cache
            self.metadata_path = Path("data_cache/indices/index_daily_metadata.parquet")
        else:
            self.metadata_path = Path(metadata_path)

        # 确保目录存在
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 分钟数据存储路径 - 统一存储在一个文件中
        self.minute_metadata_path = Path("data_cache/indices/index_minute_metadata.parquet")

        print(f"📊 指数元数据管理器初始化完成")
    
    def load_metadata(self) -> Optional[pl.DataFrame]:
        """加载指数元数据文件"""
        if not os.path.exists(self.metadata_path):
            return None
            
        try:
            return pl.read_parquet(self.metadata_path)
        except Exception as e:
            print(f"读取指数元数据文件失败: {str(e)}")
            return None
    
    def is_latest_trading_day(self) -> bool:
        """检查指数元数据是否是最新交易日的数据

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
                print("指数元数据为空，需要更新")
                return False

            if '日期' not in metadata.columns:
                print("警告: 指数元数据中缺少日期列")
                return False

            # 检查是否有均线列
            ma_cols = ['MA5', 'MA10', 'MA20']
            missing_ma_cols = [col for col in ma_cols if col not in metadata.columns]
            if missing_ma_cols:
                print(f"指数元数据缺少均线列: {missing_ma_cols}，需要更新")
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
                print("✅ 指数元数据已是最新，无需更新")
            else:
                print("📊 指数元数据需要更新")

            return is_latest

        except Exception as e:
            print(f"❌ 检查是否为最新交易日失败: {e}")
            return False

    def update_metadata(self,  start_date=None, end_date=None, progress_callback=None) -> bool:
        """更新指数元数据

        Args:
            start_date: 开始日期，默认为None时使用最近一次更新后的日期
            end_date: 结束日期，默认为None时使用当前日期
            progress_callback: 进度回调函数

        Returns:
            更新是否成功
        """
        try:
            if progress_callback:
                progress_callback(0, 100, "开始更新指数元数据")
            
            # 默认指数列表
            index_list = [
                {'code': '000001', 'name': '上证指数'},
                {'code': '399001', 'name': '深证成指'},
                {'code': '399006', 'name': '创业板指'},
                {'code': '000016', 'name': '上证50'},
                {'code': '000300', 'name': '沪深300'},
                {'code': '000905', 'name': '中证500'},
                {'code': '000852', 'name': '中证1000'},
                {'code': '000688', 'name': '科创50'},
                {'code': '932000', 'name': '中证2000'},
                {'code': '899050', 'name': '北证50'},
                {'code': '800007', 'name': '微盘股'}
            ]
            
            # 获取现有元数据
            existing_metadata = self.load_metadata()
            
            # 确定日期范围
            # 尝试从现有元数据中获取最大日期
            metadata_latest_date = None
            if existing_metadata is not None and not existing_metadata.is_empty():
                if '日期' in existing_metadata.columns:
                    metadata_latest_date = existing_metadata['日期'].max()
                    print(f"从现有指数元数据中获取到的最大日期: {metadata_latest_date}")
            
                
            if start_date is None and metadata_latest_date is not None:
                # 将开始日期设置为最大日期的下一天
                if isinstance(metadata_latest_date, str):
                    latest_date_obj = datetime.strptime(metadata_latest_date, '%Y-%m-%d')
                else:
                    latest_date_obj = metadata_latest_date
                start_date = (latest_date_obj + timedelta(days=1)).strftime('%Y%m%d')
                print(f"设置开始日期为: {start_date}")
                
            if end_date is None:
                end_date = datetime.now().strftime('%Y%m%d')

            # 先转为datetime对象
            end_date_dt = datetime.strptime(end_date, '%Y%m%d')
            start_date = (end_date_dt - timedelta(days=30)).strftime('%Y%m%d')
            print(start_date,end_date)
            all_index_data = []
            total_indices = len(index_list)
            
            for i, index_info in enumerate(index_list):
                try:
                    if progress_callback:
                        progress_callback(
                            10 + int(80 * i / total_indices),
                            100,
                            f"获取指数 {index_info['name']} 数据 ({i+1}/{total_indices})"
                        )

                    df_pl = self._fetch_index_with_fallback(index_info, start_date, end_date)
                    if df_pl is not None and not df_pl.is_empty():
                        all_index_data.append(df_pl)
                    else:
                        print(f"⚠️ 无法获取指数 {index_info['name']} 的有效数据")

                except Exception as e:
                    print(f"获取指数 {index_info['name']} 数据失败: {str(e)}")
                    continue
            
            if not all_index_data:
                return False


            # 合并新数据
            new_index_data = pl.concat(all_index_data,how="vertical")

            # 为新数据计算均线
            print("为新获取的指数数据计算均线...")
            new_index_data = self._calculate_index_ma(new_index_data)

            # 合并新旧数据
            if existing_metadata is not None and not existing_metadata.is_empty():
                # 确保现有数据也有均线列，如果没有则计算
                existing_cols = existing_metadata.columns
                ma_cols = ['MA5', 'MA10', 'MA20', '5日涨跌幅', '10日涨跌幅', '20日涨跌幅']
                missing_ma_cols = [col for col in ma_cols if col not in existing_cols]

                if missing_ma_cols:
                    print(f"现有数据缺少均线列: {missing_ma_cols}，重新计算...")
                    existing_metadata = self._calculate_index_ma(existing_metadata)

                # 处理列顺序不匹配问题
                existing_cols = existing_metadata.columns
                new_cols = new_index_data.columns

                # 找出共同的列
                common_cols = [col for col in existing_cols if col in new_cols]

                if common_cols:
                    # 按照原来的列顺序进行选择
                    existing_subset = existing_metadata.select(common_cols)
                    new_subset = new_index_data.select(common_cols)
                    updated_metadata = pl.concat([existing_subset, new_subset], how="vertical")
                else:
                    # 如果没有共同列，直接使用新数据
                    updated_metadata = new_index_data
            else:
                updated_metadata = new_index_data
            updated_metadata = updated_metadata.unique(subset=['日期', '代码'])

            # 保存数据
            updated_metadata.write_parquet(self.metadata_path)
            
            # 更新分钟数据（根据时间选择应更新的目标交易日，避免未开盘时使用未来日期）
            if end_date:
                try:
                    if progress_callback:
                        progress_callback(95, 100, "更新分钟数据...")
                    
                    # 计算分钟数据目标日期：
                    # - 非交易日：使用最近一个交易日
                    # - 交易日未开盘（<09:30）：使用前一交易日
                    # - 其它时间：使用当日
                    now = datetime.now()
                    current_date = now.date()
                    current_time = now.time()

                    def is_holiday(check_date):
                        try:
                            import holidays
                            china_holidays = holidays.China(years=check_date.year)
                            return check_date in china_holidays
                        except Exception:
                            return False

                    def is_trading_day(check_date):
                        if check_date.weekday() >= 5:
                            return False
                        if is_holiday(check_date):
                            return False
                        return True

                    def get_previous_trading_day(from_date):
                        d = from_date - timedelta(days=1)
                        for _ in range(15):
                            if is_trading_day(d):
                                return d
                            d -= timedelta(days=1)
                        return from_date - timedelta(days=1)

                    if not is_trading_day(current_date):
                        minute_target_date = get_previous_trading_day(current_date)
                    elif current_time < dt_time(9, 30):
                        minute_target_date = get_previous_trading_day(current_date)
                    else:
                        minute_target_date = current_date

                    end_date_formatted = minute_target_date.strftime('%Y-%m-%d')

                    print(f"📊 开始更新 {end_date_formatted} 的分钟数据...")
                    minute_data = self._fetch_and_cache_market_minute_data_akshare(end_date_formatted)
                    if minute_data is not None:
                        print(f"✅ {end_date_formatted} 分钟数据更新成功，已保存到 {self.minute_metadata_path}")
                    else:
                        print(f"⚠️ {end_date_formatted} 分钟数据更新失败")
                        # 若当日未能获取，则回退到前一交易日再尝试一次
                        prev_trade = get_previous_trading_day(minute_target_date)
                        prev_str = prev_trade.strftime('%Y-%m-%d')
                        print(f"🔁 回退尝试更新前一交易日 {prev_str} 的分钟数据...")
                        minute_data_prev = self._fetch_and_cache_market_minute_data_akshare(prev_str)
                        if minute_data_prev is not None:
                            print(f"✅ 前一交易日 {prev_str} 分钟数据更新成功")
                        else:
                            print(f"⚠️ 前一交易日 {prev_str} 分钟数据也不可用")
                        
                except Exception as e:
                    print(f"⚠️ 更新分钟数据时出错: {e}")
                
            if progress_callback:
                progress_callback(100, 100, "指数元数据更新完成")

            return True
            
        except Exception as e:
            print(f"更新指数元数据失败: {str(e)}")
            return False
    
    def _fetch_index_with_fallback(self, index_info: Dict[str, str], start_date: str, end_date: str) -> Optional[pl.DataFrame]:
        """获取指数数据，包含多数据源降级策略"""

        fetch_strategies: List[Tuple[str, Callable[[Dict[str, str], str, str], Optional[pd.DataFrame]]]] = [
            ("baostock", self._fetch_index_via_baostock),
            ("ak.index_zh_a_hist", self._fetch_index_via_akshare_hist)
        ]

        for source_name, fetcher in fetch_strategies:
            try:
                df = fetcher(index_info, start_date, end_date)
                if df is not None and not df.empty:
                    standardized = self._standardize_index_dataframe(df, index_info)
                    if standardized is not None and not standardized.is_empty():
                        print(f"✅ 使用 {source_name} 获取指数 {index_info['name']} 数据成功")
                        return standardized
            except Exception as fetch_error:
                print(f"⚠️ 使用 {source_name} 获取指数 {index_info['name']} 数据失败: {fetch_error}")

        return None

    def _fetch_index_via_baostock(self, index_info: Dict[str, str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """使用baostock获取指数日线数据"""
        try:
            # 登录baostock
            lg = bs.login()
            if lg.error_code != '0':
                print(f"⚠️ baostock登录失败: {lg.error_msg}")
                return None
            
            # 构建baostock指数代码格式：sh.000001 或 sz.399001
            code = index_info['code'].zfill(6)
            if code.startswith('000') or code.startswith('001'):
                bs_code = f"sh.{code}"
            elif code.startswith('399'):
                bs_code = f"sz.{code}"
            elif code.startswith('932') or code.startswith('899') or code.startswith('800'):
                # 中证系列指数使用sh前缀
                bs_code = f"sh.{code}"
            else:
                bs_code = f"sh.{code}"
            
            # 转换日期格式：YYYYMMDD -> YYYY-MM-DD
            start_date_formatted = self._format_date_for_baostock(start_date)
            end_date_formatted = self._format_date_for_baostock(end_date)
            
            # 查询指数K线数据
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,open,high,low,close,preclose,volume,amount,pctChg",
                start_date=start_date_formatted,
                end_date=end_date_formatted,
                frequency="d"
            )
            
            if rs.error_code != '0':
                bs.logout()
                return None
            
            # 获取数据
            data_list = []
            while (rs.error_code == '0') and rs.next():
                data_list.append(rs.get_row_data())
            
            # 登出
            bs.logout()
            
            if not data_list:
                return None
            
            # 转换为DataFrame
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 数据类型转换
            numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'pctChg']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df['date'] = pd.to_datetime(df['date'])
            
            return df
            
        except Exception as e:
            try:
                bs.logout()
            except:
                pass
            raise e
    
    @staticmethod
    def _format_date_for_baostock(date_str: str) -> str:
        """将日期格式转换为baostock要求的格式 YYYY-MM-DD"""
        if not date_str:
            return datetime.now().strftime('%Y-%m-%d')
        
        # 如果是YYYYMMDD格式，转换为YYYY-MM-DD
        if len(date_str) == 8 and date_str.isdigit():
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        
        # 如果已经是YYYY-MM-DD格式，直接返回
        if len(date_str) == 10 and '-' in date_str:
            return date_str
        
        return date_str

    def _fetch_index_via_akshare_hist(self, index_info: Dict[str, str], start_date: str, end_date: str):
        return ak.index_zh_a_hist(
            symbol=index_info['code'],
            period="daily",
            start_date=start_date,
            end_date=end_date,
        )

    def _fetch_index_via_tencent(self, index_info: Dict[str, str], start_date: str, end_date: str):
        symbol = self._build_exchange_symbol(index_info['code'])
        df = ak.stock_zh_index_daily_tx(symbol=symbol)
        if df is None or df.empty:
            return df

        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        start_dt = self._safe_parse_date(start_date, default=df['date'].min())
        end_dt = self._safe_parse_date(end_date, default=df['date'].max())
        mask = (df['date'] >= start_dt) & (df['date'] <= end_dt)
        return df.loc[mask]

    def _fetch_index_via_sina(self, index_info: Dict[str, str], start_date: str, end_date: str):
        symbol = self._build_exchange_symbol(index_info['code'])
        df = ak.stock_zh_index_daily(symbol=symbol)
        if df is None or df.empty:
            return df

        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        start_dt = self._safe_parse_date(start_date, default=df['date'].min())
        end_dt = self._safe_parse_date(end_date, default=df['date'].max())
        mask = (df['date'] >= start_dt) & (df['date'] <= end_dt)
        return df.loc[mask]

    def _fetch_index_minute_with_fallback(self, code: str, start_dt: str, end_dt: str, period: str = "5") -> pd.DataFrame:
        strategies: List[Tuple[str, Callable[[], Optional[pd.DataFrame]]]] = [
            (
                "ak.index_zh_a_hist_min_em",
                lambda: ak.index_zh_a_hist_min_em(symbol=code, period=period, start_date=start_dt, end_date=end_dt)
            ),
            (
                "ak.stock_zh_index_minute",
                lambda: self._fetch_index_minute_tencent(code, period, start_dt, end_dt)
            )
        ]

        for source_name, fetcher in strategies:
            try:
                df = fetcher()
                if df is not None and not df.empty:
                    print(f"✅ 使用 {source_name} 获取 {code} {period}分钟数据成功")
                    return df
            except Exception as fetch_error:
                print(f"⚠️ 使用 {source_name} 获取 {code} {period}分钟数据失败: {fetch_error}")

        return pd.DataFrame()

    def _fetch_index_minute_tencent(self, code: str, period: str, start_dt: str, end_dt: str) -> Optional[pd.DataFrame]:
        symbol = self._build_exchange_symbol(code)
        df = ak.stock_zh_index_minute(symbol=symbol, period=period)
        if df is None or df.empty:
            return df

        df = df.copy()

        if 'day' in df.columns:
            df['时间'] = pd.to_datetime(df['day'])
        elif '时间' in df.columns:
            df['时间'] = pd.to_datetime(df['时间'])
        else:
            return df

        start_dt_obj = self._safe_parse_datetime(start_dt, default=df['时间'].min())
        end_dt_obj = self._safe_parse_datetime(end_dt, default=df['时间'].max())
        df = df[(df['时间'] >= start_dt_obj) & (df['时间'] <= end_dt_obj)]

        if df.empty:
            return df

        if 'amount' in df.columns:
            df['成交额'] = pd.to_numeric(df['amount'], errors='coerce') * 10000  # amount单位为万元
        elif '成交额' in df.columns:
            df['成交额'] = pd.to_numeric(df['成交额'], errors='coerce')
        else:
            close_series = pd.to_numeric(df.get('close', df.get('收盘', 0)), errors='coerce')
            volume_series = pd.to_numeric(df.get('volume', df.get('成交量', 0)), errors='coerce')
            df['成交额'] = close_series * volume_series

        df = df[['时间', '成交额']].dropna()
        return df

    @staticmethod
    def _safe_parse_date(date_str: Optional[str], default: datetime) -> datetime:
        if not date_str:
            return default
        if len(date_str) == 8 and date_str.isdigit():
            return datetime.strptime(date_str, '%Y%m%d')
        if len(date_str) == 10 and '-' in date_str:
            return datetime.strptime(date_str, '%Y-%m-%d')
        return default

    @staticmethod
    def _safe_parse_datetime(date_time_str: Optional[str], default: datetime) -> datetime:
        if not date_time_str:
            return default
        try:
            return datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                return datetime.strptime(date_time_str, '%Y/%m/%d %H:%M:%S')
            except ValueError:
                return default

    @staticmethod
    def _build_exchange_symbol(code: str) -> str:
        code = code.zfill(6)
        if code.startswith('000') or code.startswith('001'):
            return f"sh{code}"
        elif code.startswith('399'):
            return f"sz{code}"
        elif code.startswith('932') or code.startswith('899'):
            # 北交所与中证部分指数在腾讯接口使用 sh 前缀
            return f"sh{code}"
        else:
            return f"sh{code}"

    def _standardize_index_dataframe(self, df: pd.DataFrame, index_info: Dict[str, str]) -> Optional[pl.DataFrame]:
        if df is None or df.empty:
            return None

        df_copy = df.copy()

        column_mapping = {
            '日期': '日期',
            'date': '日期',
            'day': '日期',
            '开盘价': '开盘',
            'open': '开盘',
            '收盘价': '收盘',
            'close': '收盘',
            '最高价': '最高',
            'high': '最高',
            '最低价': '最低',
            'low': '最低',
            '成交量': '成交量',
            'volume': '成交量',
            '成交额': '成交额',
            'amount': '成交额'
        }

        df_copy = df_copy.rename(columns={k: v for k, v in column_mapping.items() if k in df_copy.columns})

        # 日期列统一为日期类型
        if '日期' in df_copy.columns:
            df_copy['日期'] = pd.to_datetime(df_copy['日期']).dt.date
        else:
            return None

        # 按日期排序
        df_copy = df_copy.sort_values('日期')

        df_pl = pl.from_pandas(df_copy)

        # 添加指数代码、名称、交易所
        df_pl = df_pl.with_columns([
            pl.lit(index_info['code']).cast(pl.Utf8).str.zfill(6).alias('代码'),
            pl.lit(index_info['name']).alias('名称'),
            pl.lit(self._infer_exchange(index_info['code'])).alias('交易所')
        ])

        # 确保日期列是Date类型
        if '日期' in df_pl.columns and df_pl['日期'].dtype != pl.Date:
            df_pl = df_pl.with_columns([
                pl.col('日期').cast(pl.Date).alias('日期')
            ])

        return df_pl

    @staticmethod
    def _infer_exchange(code: str) -> str:
        if code.startswith('399'):
            return 'sz'
        return 'sh'

    def get_index_data(self, code: str, start_date: str = None, 
                      end_date: str = None) -> Optional[pl.DataFrame]:
        """获取指定指数的数据"""
        metadata = self.load_metadata()
        if metadata is None or metadata.is_empty():
            return None
            
        # 筛选指定代码的数据
        index_data = metadata.filter(pl.col('代码') == code)
        
        # 确保日期列是date类型
        if '日期' in index_data.columns and index_data['日期'].dtype == pl.Utf8:
            index_data = index_data.with_columns([
                pl.col('日期').str.strptime(pl.Date, '%Y-%m-%d').alias('日期')
            ])
        
        # 确保有交易所列
        if '交易所' not in index_data.columns and '代码' in index_data.columns:
            index_data = index_data.with_columns([
                pl.col('代码').str.slice(0, 2).alias('交易所')
            ])
        
        # 筛选日期范围时使用pl.lit()
        if start_date:
            start_date_obj = datetime.strptime(start_date, '%Y%m%d' if len(start_date) == 8 else '%Y-%m-%d').date()
            index_data = index_data.filter(pl.col('日期') >= pl.lit(start_date_obj))
        
        if end_date:
            end_date_obj = datetime.strptime(end_date, '%Y%m%d' if len(end_date) == 8 else '%Y-%m-%d').date()
            index_data = index_data.filter(pl.col('日期') <= pl.lit(end_date_obj))
        
        return index_data if not index_data.is_empty() else None

    def _calculate_index_ma(self, df: pl.DataFrame) -> pl.DataFrame:
        """为指数数据计算MA5、MA10、MA20均线"""
        if df is None or df.is_empty():
            return df

        # 确保数据按指数名称和日期排序
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

        print(f"指数均线计算完成，数据行数: {df_with_ma.height}")
        return df_with_ma

    # ==================== 分钟数据管理功能 ====================
    
    def _should_initialize_minute_data(self) -> bool:
        """检查是否需要初始化分钟数据"""
        try:
            if not self.minute_metadata_path.exists():
                return True
                
            # 检查数据是否足够新（最近7天内有数据）
            all_data = pl.read_parquet(self.minute_metadata_path)
            if all_data.is_empty():
                return True
                
            # 获取最新日期
            latest_date = all_data['日期'].max()
            if latest_date is None:
                return True
                
            # 检查最新日期是否在最近7天内
            from datetime import datetime, timedelta
            latest_date_obj = datetime.strptime(latest_date, '%Y-%m-%d').date()
            seven_days_ago = datetime.now().date() - timedelta(days=7)
            
            return latest_date_obj < seven_days_ago
            
        except Exception as e:
            print(f"⚠️ 检查分钟数据状态失败: {e}")
            return True
    
    def _initialize_two_months_minute_data(self) -> bool:
        """初始化近两个月的分钟数据"""
        try:
            from datetime import datetime, timedelta
            
            # 计算近两个月的日期范围
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=60)  # 近两个月
            
            print(f"📊 初始化分钟数据日期范围: {start_date} 到 {end_date}")
            
            # 获取交易日列表（排除周末）
            trading_days = []
            current_date = start_date
            while current_date <= end_date:
                # 排除周末
                if current_date.weekday() < 5:  # 周一到周五
                    trading_days.append(current_date.strftime('%Y-%m-%d'))
                current_date += timedelta(days=1)
            
            print(f"📅 共需获取 {len(trading_days)} 个交易日的分钟数据")
            
            success_count = 0
            for i, trading_day in enumerate(trading_days):
                try:
                    print(f"📈 获取 {trading_day} 分钟数据 ({i+1}/{len(trading_days)})...")
                    result = self._fetch_and_cache_market_minute_data_akshare(trading_day)
                    if result is not None:
                        success_count += 1
                except Exception as e:
                    print(f"❌ 获取 {trading_day} 数据失败: {e}")
                    continue
            
            print(f"✅ 分钟数据初始化完成，成功获取 {success_count}/{len(trading_days)} 个交易日")
            return success_count > 0
            
        except Exception as e:
            print(f"❌ 初始化分钟数据失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_market_minute_data(self, target_date: str, aggregate_minutes: int = 5) -> Optional[pl.DataFrame]:
        """获取指定日期的市场分钟级成交额数据，支持聚合
        使用baostock获取的日线数据，聚合为5分钟间隔
        
        Args:
            target_date: 目标日期，格式为 YYYY-MM-DD 或 YYYYMMDD
            aggregate_minutes: 聚合分钟数，默认为5分钟
            
        Returns:
            包含分钟级市场成交额数据的DataFrame，列包括：
            - 日期: 交易日期
            - 时间: 聚合后的时间戳
            - 深交所成交额: 深交所聚合成交额(亿元)
            - 沪交所成交额: 沪交所聚合成交额(亿元) 
            - 北交所成交额: 北交所聚合成交额(亿元)
            - 总成交额: 三个交易所成交额之和(亿元)
        """
        try:
            # 标准化日期格式
            if len(target_date) == 8:
                date_str = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:]}"
            else:
                date_str = target_date
            
            # 检查是否需要初始化近两个月的数据
            if not self.minute_metadata_path.exists() or self._should_initialize_minute_data():
                print(f"🔄 数据不足，开始获取近两个月的分钟数据...")
                self._initialize_two_months_minute_data()
                
            # 从统一的分钟数据文件中读取
            if self.minute_metadata_path.exists():
                print(f"📊 从统一文件加载 {date_str} 市场分钟数据")
                all_minute_data = pl.read_parquet(self.minute_metadata_path)
                
                # 筛选指定日期的数据
                minute_data = all_minute_data.filter(pl.col('日期') == date_str)
                
                if not minute_data.is_empty():
                    # 检查数据格式，如果是日线数据则获取5分钟数据
                    if '时间' not in minute_data.columns:
                        print(f"📊 检测到日线数据格式，获取5分钟数据...")
                        minute_data = self._fetch_and_cache_market_minute_data_akshare(date_str)
                        # 源数据已为5分钟粒度，除非请求的聚合粒度不是5，才进行聚合
                        if minute_data is not None and aggregate_minutes and aggregate_minutes not in (0, 5) and aggregate_minutes > 1:
                            minute_data = self._aggregate_minute_data(minute_data, aggregate_minutes)
                    else:
                        # 源数据已为5分钟，只有当聚合粒度与5不同且大于1时才进行聚合
                        if aggregate_minutes and aggregate_minutes not in (0, 5) and aggregate_minutes > 1:
                            minute_data = self._aggregate_minute_data(minute_data, aggregate_minutes)
                    return minute_data
                else:
                    print(f"📊 {date_str} 市场分钟数据不存在，开始获取...")
                    raw_data = self._fetch_and_cache_market_minute_data_akshare(date_str)
                    if raw_data is not None and aggregate_minutes and aggregate_minutes not in (0, 5) and aggregate_minutes > 1:
                        return self._aggregate_minute_data(raw_data, aggregate_minutes)
                    return raw_data
            else:
                print(f"📊 分钟数据文件不存在，开始获取 {date_str} 数据...")
                raw_data = self._fetch_and_cache_market_minute_data_akshare(date_str)
                if raw_data is not None and aggregate_minutes and aggregate_minutes not in (0, 5) and aggregate_minutes > 1:
                    return self._aggregate_minute_data(raw_data, aggregate_minutes)
                return raw_data
                
        except Exception as e:
            print(f"❌ 获取市场分钟数据失败: {e}")
            return None
    
    def _convert_daily_to_minute_data(self, daily_data: pl.DataFrame, date_str: str, aggregate_minutes: int = 5) -> pl.DataFrame:
        """将日线数据转换为分钟数据格式
        
        Args:
            daily_data: 日线数据
            date_str: 日期字符串
            aggregate_minutes: 聚合分钟数
            
        Returns:
            转换后的分钟数据
        """
        try:
            print(f"🔄 将日线数据转换为 {aggregate_minutes} 分钟间隔的分钟数据...")
            
            # 创建5分钟间隔的时间点
            morning_times = [
                f"{date_str} 09:30:00", f"{date_str} 09:35:00", f"{date_str} 09:40:00", f"{date_str} 09:45:00", f"{date_str} 09:50:00",
                f"{date_str} 09:55:00", f"{date_str} 10:00:00", f"{date_str} 10:05:00", f"{date_str} 10:10:00", f"{date_str} 10:15:00",
                f"{date_str} 10:20:00", f"{date_str} 10:25:00", f"{date_str} 10:30:00", f"{date_str} 10:35:00", f"{date_str} 10:40:00",
                f"{date_str} 10:45:00", f"{date_str} 10:50:00", f"{date_str} 10:55:00", f"{date_str} 11:00:00", f"{date_str} 11:05:00",
                f"{date_str} 11:10:00", f"{date_str} 11:15:00", f"{date_str} 11:20:00", f"{date_str} 11:25:00", f"{date_str} 11:30:00"
            ]
            afternoon_times = [
                f"{date_str} 13:00:00", f"{date_str} 13:05:00", f"{date_str} 13:10:00", f"{date_str} 13:15:00", f"{date_str} 13:20:00",
                f"{date_str} 13:25:00", f"{date_str} 13:30:00", f"{date_str} 13:35:00", f"{date_str} 13:40:00", f"{date_str} 13:45:00",
                f"{date_str} 13:50:00", f"{date_str} 13:55:00", f"{date_str} 14:00:00", f"{date_str} 14:05:00", f"{date_str} 14:10:00",
                f"{date_str} 14:15:00", f"{date_str} 14:20:00", f"{date_str} 14:25:00", f"{date_str} 14:30:00", f"{date_str} 14:35:00",
                f"{date_str} 14:40:00", f"{date_str} 14:45:00", f"{date_str} 14:50:00", f"{date_str} 14:55:00", f"{date_str} 15:00:00"
            ]
            all_times = morning_times + afternoon_times
            
            # 获取成交额列
            turnover_cols = [col for col in daily_data.columns if col.endswith('成交额')]
            
            # 创建分钟数据
            minute_data_list = []
            for row in daily_data.to_dicts():
                for time_str in all_times:
                    minute_row = {
                        '日期': row['日期'],
                        '时间': time_str
                    }
                    
                    # 为每个成交额列分配平均到每个时间点
                    for col in turnover_cols:
                        if col in row and row[col] is not None:
                            minute_row[col] = float(row[col]) / len(all_times)
                        else:
                            minute_row[col] = 0.0
                    
                    minute_data_list.append(minute_row)
            
            if minute_data_list:
                minute_df = pl.DataFrame(minute_data_list)
                
                # 计算累计成交额
                for col in turnover_cols:
                    if col in minute_df.columns:
                        cumulative_col = col.replace('成交额', '累计成交额')
                        minute_df = minute_df.with_columns([
                            pl.col(col).cumsum().alias(cumulative_col)
                        ])
                
                print(f"✅ 日线数据转换完成，生成 {minute_df.height} 条分钟数据")
                return minute_df
            else:
                print("❌ 日线数据转换失败")
                return daily_data
                
        except Exception as e:
            print(f"❌ 日线数据转换失败: {e}")
            return daily_data

    def _aggregate_minute_data(self, minute_data: pl.DataFrame, aggregate_minutes: int) -> pl.DataFrame:
        """将分钟数据按指定分钟数聚合
        
        Args:
            minute_data: 原始分钟数据
            aggregate_minutes: 聚合分钟数
            
        Returns:
            聚合后的数据
        """
        try:
            print(f"🔄 开始进行 {aggregate_minutes} 分钟聚合...")
            
            # 确保时间列是datetime类型
            if minute_data['时间'].dtype == pl.Utf8:
                minute_data = minute_data.with_columns([
                    pl.col('时间').str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S').alias('时间')
                ])
            
            # 创建聚合时间戳（向下取整到聚合间隔）
            minute_data = minute_data.with_columns([
                (pl.col('时间').dt.truncate(f"{aggregate_minutes}m")).alias('聚合时间')
            ])
            
            # 按聚合时间分组并求和
            # 排除"总成交额"，只包含各交易所的成交额
            exchange_cols = [col for col in minute_data.columns if col.endswith('成交额') and col != '总成交额']
            
            aggregated_data = minute_data.group_by(['日期', '聚合时间']).agg([
                pl.col('时间').first().alias('时间'),  # 取第一个时间作为代表
                *[pl.col(col).sum().alias(col) for col in exchange_cols]
            ])
            
            # 重新计算总成交额（使用各交易所成交额之和）
            aggregated_data = aggregated_data.with_columns([
                pl.sum_horizontal([pl.col(col) for col in exchange_cols]).alias('总成交额')
            ])
            
            # 按时间排序
            aggregated_data = aggregated_data.sort('聚合时间')
            
            # 计算累计成交额
            aggregated_data = aggregated_data.with_columns([
                pl.col('深交所成交额').cumsum().alias('深交所累计成交额'),
                pl.col('沪交所成交额').cumsum().alias('沪交所累计成交额'),
                pl.col('北交所成交额').cumsum().alias('北交所累计成交额'),
                pl.col('总成交额').cumsum().alias('总累计成交额')
            ])
            
            # 聚合时间列已经是正确的时间，不需要重命名
            
            print(f"✅ {aggregate_minutes} 分钟聚合完成，从 {minute_data.height} 条记录聚合为 {aggregated_data.height} 条记录")
            return aggregated_data
            
        except Exception as e:
            print(f"❌ 分钟数据聚合失败: {e}")
            return minute_data
    
    def _fetch_and_cache_market_minute_data_akshare(self, date_str: str) -> Optional[pl.DataFrame]:
        """使用akshare获取并缓存指定日期的市场5分钟数据"""
        try:
            print(f"🔄 开始获取 {date_str} 市场5分钟数据...")
            # 定义目标时间范围
            start_dt = f"{date_str} 09:30:00"
            end_dt = f"{date_str} 15:00:00"

            # 获取三大指数的5分钟数据
            print("📈 获取指数5分钟分时数据...")
            def fetch_min_df(symbol: str) -> pd.DataFrame:
                df = self._fetch_index_minute_with_fallback(symbol, start_dt, end_dt, period="5")
                if df is None or df.empty:
                    print(f"❌ 获取 {symbol} 5分钟数据失败: 数据为空")
                    return pd.DataFrame()
                return df

            sh_min = fetch_min_df("000001")  # 上证指数
            sz_min = fetch_min_df("399001")  # 深证成指
            bj_min = fetch_min_df("899050")  # 北证50（可选）

            # 如果分时数据不可用，则回退到旧逻辑
            if (sh_min is None or sh_min.empty) and (sz_min is None or sz_min.empty):
                print("⚠️ 分时数据不可用，回退到日线估算逻辑")
                # 回退：使用旧实现（返回None由上层决定如何处理）
                return None

            # 只保留目标日期的数据，并转换为亿元
            def prep_minute(df: pd.DataFrame) -> pl.DataFrame:
                if df is None or df.empty:
                    return pl.DataFrame({"时间": [], "成交额": []})
                df = df.copy()
                # 过滤当天
                df["时间"] = pd.to_datetime(df["时间"])  # pandas
                df = df[(df["时间"].dt.strftime('%Y-%m-%d') == date_str)]
                if df.empty:
                    return pl.DataFrame({"时间": [], "成交额": []})
                # 转换为polars并单位换算为亿元
                pl_df = pl.from_pandas(df[["时间", "成交额"]])
                pl_df = pl_df.with_columns([
                    pl.col("成交额").cast(pl.Float64) / 100000000
                ])
                return pl_df

            sh_pl = prep_minute(sh_min).rename({"成交额": "沪交所成交额"})
            sz_pl = prep_minute(sz_min).rename({"成交额": "深交所成交额"})
            bj_pl = prep_minute(bj_min).rename({"成交额": "北交所成交额"})

            # 合并到统一的时间轴（外连接）
            merged_data = None
            for df in [sh_pl, sz_pl, bj_pl]:
                if df is None or df.is_empty():
                    continue
                if merged_data is None:
                    merged_data = df
                else:
                    merged_data = merged_data.join(df, on="时间", how="outer")

            if merged_data is None or merged_data.is_empty():
                print("❌ 合并后的分钟数据为空")
                return None

            # 填充缺失值为0
            for col in ["沪交所成交额", "深交所成交额", "北交所成交额"]:
                if col not in merged_data.columns:
                    merged_data = merged_data.with_columns([pl.lit(0.0).alias(col)])
                else:
                    merged_data = merged_data.with_columns([pl.col(col).fill_null(0.0).alias(col)])

            # 计算总成交额（亿元）
            merged_data = merged_data.with_columns([
                (pl.col("沪交所成交额") + pl.col("深交所成交额") + pl.col("北交所成交额")).alias("总成交额")
            ])

            # 为了与日总额对齐，按日线总额进行缩放校准
            print("📈 获取当日日线指数成交额用于缩放...")
            sh_daily = ak.index_zh_a_hist(symbol='000001', period='daily', start_date=date_str.replace('-', ''), end_date=date_str.replace('-', ''))
            sz_daily = ak.index_zh_a_hist(symbol='399001', period='daily', start_date=date_str.replace('-', ''), end_date=date_str.replace('-', ''))
            bj_daily = None
            try:
                bj_daily = ak.index_zh_a_hist(symbol='899050', period='daily', start_date=date_str.replace('-', ''), end_date=date_str.replace('-', ''))
            except Exception:
                bj_daily = None

            target_total = 0.0
            if sh_daily is not None and not sh_daily.empty:
                target_total += float(sh_daily['成交额'].iloc[-1]) / 100000000
            if sz_daily is not None and not sz_daily.empty:
                target_total += float(sz_daily['成交额'].iloc[-1]) / 100000000
            if bj_daily is not None and not bj_daily.empty and '成交额' in bj_daily.columns:
                target_total += float(bj_daily['成交额'].iloc[-1]) / 100000000

            # 分钟合计
            minute_sum = float(merged_data.select(pl.col("总成交额").sum()).to_series()[0]) if "总成交额" in merged_data.columns else 0.0
            scale_factor = (target_total / minute_sum) if minute_sum and minute_sum > 0 else 1.0
            if abs(scale_factor - 1.0) > 0.05:
                print(f"⚖️ 按日线总额校准分钟数据: scale={scale_factor:.4f} (分钟合计:{minute_sum:.2f}亿, 日线目标:{target_total:.2f}亿)")
            merged_data = merged_data.with_columns([
                (pl.col("沪交所成交额") * scale_factor).alias("沪交所成交额"),
                (pl.col("深交所成交额") * scale_factor).alias("深交所成交额"),
                (pl.col("北交所成交额") * scale_factor).alias("北交所成交额"),
            ])
            merged_data = merged_data.with_columns([
                (pl.col("沪交所成交额") + pl.col("深交所成交额") + pl.col("北交所成交额")).alias("总成交额")
            ])

            # 添加日期列并排序
            merged_data = merged_data.with_columns([
                pl.lit(date_str).alias('日期')
            ])
            merged_data = merged_data.sort('时间')

            # 计算累计成交额 - 按日期分组累计
            merged_data = merged_data.with_columns([
                pl.col('深交所成交额').cumsum().over('日期').alias('深交所累计成交额'),
                pl.col('沪交所成交额').cumsum().over('日期').alias('沪交所累计成交额'),
                pl.col('北交所成交额').cumsum().over('日期').alias('北交所累计成交额'),
                pl.col('总成交额').cumsum().over('日期').alias('总累计成交额')
            ])
            
            # 读取现有数据并合并
            if self.minute_metadata_path.exists():
                existing_data = pl.read_parquet(self.minute_metadata_path)
                # 删除同一天的数据（避免重复）
                existing_data = existing_data.filter(pl.col('日期') != date_str)
                
                # 确保列结构一致
                existing_cols = set(existing_data.columns)
                new_cols = set(merged_data.columns)
                
                # 如果列结构不同，需要统一列结构
                if existing_cols != new_cols:
                    print(f"⚠️ 列结构不一致，现有列: {existing_cols}, 新列: {new_cols}")
                    
                    # 为缺失的列添加默认值
                    for col in new_cols - existing_cols:
                        existing_data = existing_data.with_columns([
                            pl.lit(0.0).alias(col)
                        ])
                    
                    for col in existing_cols - new_cols:
                        merged_data = merged_data.with_columns([
                            pl.lit(0.0).alias(col)
                        ])
                    
                    # 确保列顺序一致
                    merged_data = merged_data.select(existing_data.columns)
                
                # 合并新旧数据
                updated_data = pl.concat([existing_data, merged_data], how="vertical")
            else:
                updated_data = merged_data
            
            # 去重处理 - 按时间和日期去重
            updated_data = updated_data.unique(subset=['日期', '时间'], keep='first')
            print(f"🔄 去重处理完成，保留 {updated_data.height} 条记录")
            
            # 保存到统一文件
            updated_data.write_parquet(self.minute_metadata_path)
            
            print(f"✅ {date_str} 市场5分钟数据获取并缓存成功，共{merged_data.height}条记录")
            return merged_data
            
        except Exception as e:
            print(f"❌ 获取并缓存市场5分钟数据失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _fetch_and_cache_market_minute_data(self, date_str: str) -> Optional[pl.DataFrame]:
        """获取并缓存指定日期的市场分钟数据"""
        try:
            print(f"🔄 开始获取 {date_str} 市场分钟数据...")
            
            # 交易所指数代码映射
            exchange_indices = {
                '深交所': '399001',  # 深证成指
                '沪交所': '000001',  # 上证指数
                '北交所': '899050'   # 北证50
            }
            
            all_minute_data = {}
            
            # 获取各交易所指数分钟数据
            for exchange, code in exchange_indices.items():
                try:
                    print(f"  📈 获取{exchange}指数{code}分钟数据...")
                    
                    # 使用akshare获取分钟数据
                    minute_df = ak.index_zh_a_hist_min_em(
                        symbol=code,
                        period='1',  # 1分钟K线
                        start_date=date_str.replace('-', ''),
                        end_date=date_str.replace('-', '')
                    )
                    
                    if minute_df is not None and not minute_df.empty:
                        # 转换为polars
                        minute_pl = pl.from_pandas(minute_df)
                        
                        # 标准化列名并计算成交额
                        if '时间' in minute_pl.columns and '成交量' in minute_pl.columns:
                            # 成交额 = 成交量 * 平均价格，这里用收盘价近似
                            if '收盘' in minute_pl.columns:
                                minute_pl = minute_pl.with_columns([
                                    (pl.col('成交量') * pl.col('收盘') / 100000000).alias(f'{exchange}成交额')
                                ])
                            else:
                                # 如果没有价格数据，使用成交量作为成交额的代理
                                minute_pl = minute_pl.with_columns([
                                    (pl.col('成交量') / 100000000).alias(f'{exchange}成交额')
                                ])
                            
                            all_minute_data[exchange] = minute_pl.select(['时间', f'{exchange}成交额'])
                            print(f"  ✅ {exchange}数据获取成功，{minute_pl.height}条记录")
                        else:
                            print(f"  ⚠️ {exchange}数据格式异常，跳过")
                    else:
                        print(f"  ❌ {exchange}数据获取失败或为空")
                        
                except Exception as e:
                    print(f"  ❌ 获取{exchange}数据失败: {e}")
                    continue
            
            if not all_minute_data:
                print(f"❌ 未能获取到任何交易所的分钟数据")
                return None
            
            # 合并所有交易所的数据
            print(f"🔗 合并 {len(all_minute_data)} 个交易所的分钟数据...")
            
            # 以时间为基准进行外连接
            merged_data = None
            for exchange, data in all_minute_data.items():
                if merged_data is None:
                    merged_data = data
                else:
                    merged_data = merged_data.join(data, on='时间', how='outer')
            
            if merged_data is None:
                return None
            
            # 填充缺失值为0并计算总成交额
            # 排除"总成交额"，只包含各交易所的成交额
            exchange_cols = [col for col in merged_data.columns if col.endswith('成交额') and col != '总成交额']
            
            # 填充空值为0
            for col in exchange_cols:
                merged_data = merged_data.with_columns([
                    pl.col(col).fill_null(0).alias(col)
                ])
            
            # 计算总成交额（使用各交易所成交额之和）
            merged_data = merged_data.with_columns([
                pl.sum_horizontal([pl.col(col) for col in exchange_cols]).alias('总成交额')
            ])
            
            # 添加日期列
            merged_data = merged_data.with_columns([
                pl.lit(date_str).alias('日期')
            ])
            
            # 按时间排序
            merged_data = merged_data.sort('时间')
            
            # 读取现有数据并合并
            if self.minute_metadata_path.exists():
                existing_data = pl.read_parquet(self.minute_metadata_path)
                # 删除同一天的数据（避免重复）
                existing_data = existing_data.filter(pl.col('日期') != date_str)
                
                # 确保列结构一致
                existing_cols = set(existing_data.columns)
                new_cols = set(merged_data.columns)
                
                # 如果列结构不同，需要统一列结构
                if existing_cols != new_cols:
                    print(f"⚠️ 列结构不一致，现有列: {existing_cols}, 新列: {new_cols}")
                    
                    # 为缺失的列添加默认值
                    for col in new_cols - existing_cols:
                        existing_data = existing_data.with_columns([
                            pl.lit(0.0).alias(col)
                        ])
                    
                    for col in existing_cols - new_cols:
                        merged_data = merged_data.with_columns([
                            pl.lit(0.0).alias(col)
                        ])
                    
                    # 确保列顺序一致
                    merged_data = merged_data.select(existing_data.columns)
                
                # 合并新旧数据
                updated_data = pl.concat([existing_data, merged_data], how="vertical")
            else:
                updated_data = merged_data
            
            # 去重处理 - 按时间和日期去重
            updated_data = updated_data.unique(subset=['日期', '时间'], keep='first')
            print(f"🔄 去重处理完成，保留 {updated_data.height} 条记录")
            
            # 保存到统一文件
            updated_data.write_parquet(self.minute_metadata_path)
            
            print(f"✅ {date_str} 市场分钟数据获取并缓存成功，共{merged_data.height}条记录")
            return merged_data
            
        except Exception as e:
            print(f"❌ 获取并缓存市场分钟数据失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_market_volume_comparison(self, current_date: str, previous_date: str = None) -> Optional[Dict]:
        """获取市场量能对比数据
        
        Args:
            current_date: 当前日期 (YYYY-MM-DD)
            previous_date: 对比日期，默认为前一交易日 (YYYY-MM-DD)
            
        Returns:
            包含量能对比数据的字典：
            {
                'current_data': 当日分钟数据,
                'previous_data': 前日分钟数据,
                'comparison_data': 对比统计数据
            }
        """
        try:
            # 获取当日数据（使用5分钟聚合）
            current_data = self.get_market_minute_data(current_date, aggregate_minutes=5)
            if current_data is None:
                print(f"❌ 无法获取 {current_date} 的分钟数据")
                return None
            
            # 如果没有指定对比日期，使用前一交易日
            if previous_date is None:
                previous_date = self._get_previous_trading_day(current_date)
            
            # 获取前日数据（使用5分钟聚合）
            previous_data = self.get_market_minute_data(previous_date, aggregate_minutes=5)
            if previous_data is None:
                print(f"❌ 无法获取 {previous_date} 的分钟数据")
                return None
            
            # 计算对比统计 - 使用累计成交额
            current_total = current_data['总累计成交额'].max() if '总累计成交额' in current_data.columns else current_data['总成交额'].sum()
            previous_total = previous_data['总累计成交额'].max() if '总累计成交额' in previous_data.columns else previous_data['总成交额'].sum()
            change_amount = current_total - previous_total
            change_pct = (change_amount / previous_total * 100) if previous_total > 0 else 0
            
            comparison_data = {
                'current_total': float(current_total),
                'previous_total': float(previous_total),
                'change_amount': float(change_amount),
                'change_pct': float(change_pct),
                'current_date': current_date,
                'previous_date': previous_date
            }
            
            print(f"📊 市场量能对比: {current_date}({current_total:.2f}亿) vs {previous_date}({previous_total:.2f}亿), 变化: {change_amount:.2f}亿({change_pct:.2f}%)")
            
            return {
                'current_data': current_data,
                'previous_data': previous_data,
                'comparison_data': comparison_data
            }
            
        except Exception as e:
            print(f"❌ 获取市场量能对比数据失败: {e}")
            return None
    
    def _get_previous_trading_day(self, date_str: str) -> str:
        """获取前一个交易日"""
        try:
            current_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # 简单地减去1-3天来找前一个交易日
            for i in range(1, 8):  # 最多往前找一周
                prev_date = current_date - timedelta(days=i)
                
                # 跳过周末
                if prev_date.weekday() < 5:  # 周一到周五
                    return prev_date.strftime('%Y-%m-%d')
            
            # 如果找不到，返回一周前
            return (current_date - timedelta(days=7)).strftime('%Y-%m-%d')
            
        except Exception as e:
            print(f"❌ 计算前一交易日失败: {e}")
            return (datetime.strptime(date_str, '%Y-%m-%d').date() - timedelta(days=1)).strftime('%Y-%m-%d')
    
