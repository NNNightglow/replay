
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
    """指数元数据管理器(支持日线与分钟数据)。"""
    def __init__(self, metadata_path: str = None):
        if metadata_path is None:
            self.metadata_path = Path("data_cache/indices/index_daily_metadata.parquet")
        else:
            self.metadata_path = Path(metadata_path)

        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.minute_metadata_path = Path("data_cache/indices/index_minute_metadata.parquet")

        print("指数元数据管理器初始化完成")
    
    def load_metadata(self) -> Optional[pl.DataFrame]:
        """从本地 Parquet 加载指数元数据。"""
        if not os.path.exists(self.metadata_path):
            return None
            
        try:
            return pl.read_parquet(self.metadata_path)
        except Exception as e:
            print(f"加载指数元数据失败:{str(e)}")
            return None
    
    @staticmethod
    def _to_dash_date(date_input: Union[str, date, datetime]) -> Optional[str]:
        """将日期输入标准化为 YYYY-MM-DD。"""
        if date_input is None:
            return None
        if isinstance(date_input, datetime):
            return date_input.date().strftime("%Y-%m-%d")
        if isinstance(date_input, date):
            return date_input.strftime("%Y-%m-%d")
        if isinstance(date_input, str):
            text = date_input.strip()
            if len(text) == 8 and text.isdigit():
                return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
            if len(text) == 10 and "-" in text:
                return text
        return None

    def _fill_missing_index_data_for_date_range(
        self,
        start_date: str,
        end_date: str,
        index_items: List[Dict[str, str]],
    ) -> Optional[pl.DataFrame]:
        """按日期区间补抓指数日线缺失数据。"""
        start_dash = self._to_dash_date(start_date)
        start_compact = start_dash.replace("-", "") if start_dash else None
        end_dash = self._to_dash_date(end_date)
        end_compact = end_dash.replace("-", "") if end_dash else None
        if not start_compact or not end_compact or not index_items:
            return None

        all_filled: List[pl.DataFrame] = []
        for index_info in index_items:
            try:
                df_pl = self._fetch_index_with_fallback(index_info, start_compact, end_compact)
                if df_pl is not None and not df_pl.is_empty():
                    all_filled.append(df_pl)
            except Exception as e:
                print(f"[] {index_info.get('code')} {start_date}~{end_date}{e}")

        if not all_filled:
            return None
        return pl.concat(all_filled, how="vertical")

    def update_metadata(self,  start_date=None, end_date=None, progress_callback=None, fill_gaps: bool = True) -> bool:
        """更新指数元数据,并可选填补缺失交易日。"""
        date_col = "日期"
        code_col = "代码"
        name_col = "名称"
        pct5_col = "5日涨跌幅"
        pct10_col = "10日涨跌幅"
        pct20_col = "20日涨跌幅"
        try:
            if progress_callback:
                progress_callback(0, 100, "开始更新指数元数据")

            index_list = [
                {"code": "000001", "name": "上证指数"},
                {"code": "399001", "name": "深证成指"},
                {"code": "399006", "name": "创业板指"},
                {"code": "000016", "name": "上证50"},
                {"code": "000300", "name": "沪深300"},
                {"code": "000905", "name": "中证500"},
                {"code": "000852", "name": "中证1000"},
                {"code": "000688", "name": "科创50"},
                {"code": "932000", "name": "中证2000"},
                {"code": "899050", "name": "北证50"},
                {"code": "800007", "name": "微盘股"},
            ]

            existing_metadata = self.load_metadata()

            if fill_gaps and existing_metadata is not None and not existing_metadata.is_empty():
                try:
                    from utils.trading_calendar import trading_calendar
                    if date_col in existing_metadata.columns and code_col in existing_metadata.columns:
                        index_date_ranges = existing_metadata.group_by(code_col).agg([
                            pl.col(date_col).min().alias("min_date"),
                            pl.col(date_col).max().alias("max_date"),
                        ])
                        existing_pairs = existing_metadata.select([date_col, code_col]).unique()
                        existing_pair_set = {
                            (row[date_col], str(row[code_col]).zfill(6))
                            for row in existing_pairs.iter_rows(named=True)
                        }
                        code_to_name = {str(item["code"]).zfill(6): item["name"] for item in index_list}
                        if name_col in existing_metadata.columns:
                            for row in existing_metadata.select([code_col, name_col]).drop_nulls().unique().iter_rows(named=True):
                                code_value = str(row[code_col]).zfill(6)
                                code_to_name[code_value] = row[name_col]

                        missing_by_code = {}
                        for row in index_date_ranges.iter_rows(named=True):
                            code_value = str(row[code_col]).zfill(6)
                            min_date = row["min_date"]
                            max_date = row["max_date"]
                            if min_date is None or max_date is None:
                                continue
                            for trading_day in trading_calendar.get_trading_days_in_range(min_date, max_date):
                                if (trading_day, code_value) not in existing_pair_set:
                                    missing_by_code.setdefault(code_value, []).append(trading_day)

                        if missing_by_code:
                            range_to_codes = {}
                            for code_value, dates in missing_by_code.items():
                                if dates:
                                    range_to_codes.setdefault((min(dates), max(dates)), []).append(code_value)

                            gap_filled_data = []
                            for (min_date, max_date), code_list in range_to_codes.items():
                                start_str = self._to_dash_date(min_date)
                                end_str = self._to_dash_date(max_date)
                                targets = [{"code": c, "name": code_to_name.get(c, c)} for c in code_list]
                                filled = self._fill_missing_index_data_for_date_range(start_str, end_str, targets)
                                if filled is not None and not filled.is_empty():
                                    gap_filled_data.append(filled)

                            if gap_filled_data:
                                gap_filled_combined = pl.concat(gap_filled_data, how="vertical").unique(
                                    subset=[date_col, code_col], keep="last"
                                )
                                existing_metadata = pl.concat(
                                    [existing_metadata, gap_filled_combined], how="vertical_relaxed"
                                ).unique(subset=[date_col, code_col], keep="last")
                                existing_metadata = self._calculate_index_ma(existing_metadata.sort([code_col, date_col]))
                                existing_metadata.write_parquet(self.metadata_path)
                except Exception as gap_error:
                    print(f"[] {gap_error}")

            metadata_latest_date = None
            if existing_metadata is not None and not existing_metadata.is_empty() and date_col in existing_metadata.columns:
                metadata_latest_date = existing_metadata[date_col].max()

            if start_date is None and metadata_latest_date is not None:
                latest_date_obj = datetime.strptime(metadata_latest_date, "%Y-%m-%d") if isinstance(metadata_latest_date, str) else metadata_latest_date
                start_date = (latest_date_obj + timedelta(days=1)).strftime("%Y%m%d")
            if end_date is None:
                end_date = datetime.now().strftime("%Y%m%d")

            # 仅在未显式传入 start_date 时，默认回看 30 天
            if start_date is None:
                end_date_dt = datetime.strptime(end_date, "%Y%m%d")
                start_date = (end_date_dt - timedelta(days=30)).strftime("%Y%m%d")
            else:
                # 兼容传入 YYYY-MM-DD
                if isinstance(start_date, str) and len(start_date) == 10 and "-" in start_date:
                    start_date = start_date.replace("-", "")
            if isinstance(end_date, str) and len(end_date) == 10 and "-" in end_date:
                end_date = end_date.replace("-", "")
            all_index_data = []

            for i, index_info in enumerate(index_list):
                try:
                    if progress_callback:
                        progress_callback(10 + int(80 * i / len(index_list)), 100, f" {index_info['name']}{i+1}/{len(index_list)}")
                    df_pl = self._fetch_index_with_fallback(index_info, start_date, end_date)
                    if df_pl is not None and not df_pl.is_empty():
                        all_index_data.append(df_pl)
                except Exception as e:
                    print(f"{index_info['name']}{e}")

            if not all_index_data:
                return False

            new_index_data = self._calculate_index_ma(pl.concat(all_index_data, how="vertical"))
            if existing_metadata is not None and not existing_metadata.is_empty():
                ma_cols = ["MA5", "MA10", "MA20", pct5_col, pct10_col, pct20_col]
                missing_ma_cols = [col for col in ma_cols if col not in existing_metadata.columns]
                if missing_ma_cols:
                    existing_metadata = self._calculate_index_ma(existing_metadata)
                common_cols = [col for col in existing_metadata.columns if col in new_index_data.columns]
                updated_metadata = (
                    pl.concat([existing_metadata.select(common_cols), new_index_data.select(common_cols)], how="vertical")
                    if common_cols else new_index_data
                )
            else:
                updated_metadata = new_index_data

            updated_metadata = updated_metadata.unique(subset=[date_col, code_col], keep="last")
            updated_metadata.write_parquet(self.metadata_path)

            if end_date:
                try:
                    if progress_callback:
                        progress_callback(95, 100, "更新分钟数据中...")
                    now = datetime.now()
                    current_date = now.date()

                    def get_previous_trading_day(from_date):
                        d = from_date - timedelta(days=1)
                        for _ in range(15):
                            is_holiday = False
                            try:
                                import holidays
                                is_holiday = d in holidays.China(years=d.year)
                            except Exception:
                                pass
                            if d.weekday() < 5 and not is_holiday:
                                return d
                            d -= timedelta(days=1)
                        return from_date - timedelta(days=1)

                    current_is_trading = current_date.weekday() < 5
                    if current_is_trading:
                        try:
                            import holidays
                            current_is_trading = current_date not in holidays.China(years=current_date.year)
                        except Exception:
                            pass

                    if not current_is_trading or now.time() < dt_time(9, 30):
                        minute_target_date = get_previous_trading_day(current_date)
                    else:
                        minute_target_date = current_date

                    end_date_formatted = minute_target_date.strftime("%Y-%m-%d")
                    minute_data = self._fetch_and_cache_market_minute_data_akshare(end_date_formatted)
                    if minute_data is None:
                        prev_str = get_previous_trading_day(minute_target_date).strftime("%Y-%m-%d")
                        self._fetch_and_cache_market_minute_data_akshare(prev_str)
                except Exception as e:
                    print(f"[] {e}")

            if progress_callback:
                progress_callback(100, 100, "指数元数据更新完成")
            return True
        except Exception as e:
            print(f"更新指数元数据失败:{e}")
            return False

    def _fetch_index_with_fallback(self, index_info: Dict[str, str], start_date: str, end_date: str) -> Optional[pl.DataFrame]:
        """"""
        try:
            df = self._fetch_index_via_baostock(index_info, start_date, end_date)
            if df is not None and not df.empty:
                standardized = self._standardize_index_dataframe(df, index_info)
                if standardized is not None and not standardized.is_empty():
                    print(f"[] =baostock  {index_info['name']} ")
                    return standardized
        except Exception as fetch_error:
            print(f"[] =baostock  {index_info['name']} {fetch_error}")

        try:
            df = ak.index_zh_a_hist(
                symbol=index_info['code'],
                period="daily",
                start_date=start_date,
                end_date=end_date,
            )
            if df is not None and not df.empty:
                standardized = self._standardize_index_dataframe(df, index_info)
                if standardized is not None and not standardized.is_empty():
                    print(f"[] =ak.index_zh_a_hist  {index_info['name']} ")
                    return standardized
        except Exception as fetch_error:
            print(f"[] =ak.index_zh_a_hist  {index_info['name']} {fetch_error}")

        return None

    def _fetch_index_via_baostock(self, index_info: Dict[str, str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """ baostock """
        try:
            lg = bs.login()
            if lg.error_code != '0':
                print(f"[] baostock {lg.error_msg}")
                return None
            
            code = index_info['code'].zfill(6)
            if code.startswith('000') or code.startswith('001'):
                bs_code = f"sh.{code}"
            elif code.startswith('399'):
                bs_code = f"sz.{code}"
            elif code.startswith('932') or code.startswith('899') or code.startswith('800'):
                bs_code = f"sh.{code}"
            else:
                bs_code = f"sh.{code}"
            
            if not start_date:
                start_date_formatted = datetime.now().strftime('%Y-%m-%d')
            elif len(start_date) == 8 and start_date.isdigit():
                start_date_formatted = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
            else:
                start_date_formatted = start_date

            if not end_date:
                end_date_formatted = datetime.now().strftime('%Y-%m-%d')
            elif len(end_date) == 8 and end_date.isdigit():
                end_date_formatted = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
            else:
                end_date_formatted = end_date
            
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
            
            data_list = []
            while (rs.error_code == '0') and rs.next():
                data_list.append(rs.get_row_data())
            
            bs.logout()
            
            if not data_list:
                return None
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            
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

    def _standardize_index_dataframe(self, df: pd.DataFrame, index_info: Dict[str, str]) -> Optional[pl.DataFrame]:
        if df is None or df.empty:
            return None

        date_col = "日期"
        open_col = "开盘"
        close_col = "收盘"
        high_col = "最高"
        low_col = "最低"
        volume_col = "成交量"
        amount_col = "成交额"
        code_col = "代码"
        name_col = "名称"
        exchange_col = "交易所"

        df_copy = df.copy()
        if "amount" in df_copy.columns and amount_col not in df_copy.columns:
            df_copy[amount_col] = pd.to_numeric(df_copy["amount"], errors="coerce")
        if "volume" in df_copy.columns and volume_col not in df_copy.columns:
            df_copy[volume_col] = pd.to_numeric(df_copy["volume"], errors="coerce")

        column_mapping = {
            "date": date_col,
            "day": date_col,
            "": date_col,
            "open": open_col,
            "": open_col,
            "close": close_col,
            "": close_col,
            "high": high_col,
            "": high_col,
            "low": low_col,
            "": low_col,
            "": volume_col,
            "": amount_col,
        }
        df_copy = df_copy.rename(columns={k: v for k, v in column_mapping.items() if k in df_copy.columns})

        if date_col not in df_copy.columns:
            return None
        df_copy[date_col] = pd.to_datetime(df_copy[date_col], errors="coerce").dt.date
        df_copy = df_copy.dropna(subset=[date_col]).sort_values(date_col)

        numeric_cols = [open_col, close_col, high_col, low_col, volume_col, amount_col]
        for col in numeric_cols:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors="coerce")

        df_pl = pl.from_pandas(df_copy)
        exchange_value = 'sz' if str(index_info["code"]).zfill(6).startswith('399') else 'sh'
        df_pl = df_pl.with_columns([
            pl.lit(str(index_info["code"]).zfill(6)).alias(code_col),
            pl.lit(index_info["name"]).alias(name_col),
            pl.lit(exchange_value).alias(exchange_col),
        ])

        if date_col in df_pl.columns and df_pl[date_col].dtype != pl.Date:
            df_pl = df_pl.with_columns(pl.col(date_col).cast(pl.Date).alias(date_col))

        return df_pl

    def get_index_data(self, code: str, start_date: str = None, 
                      end_date: str = None) -> Optional[pl.DataFrame]:
        """"""
        date_col = "日期"
        code_col = "代码"
        exchange_col = "交易所"

        metadata = self.load_metadata()
        if metadata is None or metadata.is_empty():
            return None

        index_data = metadata.filter(pl.col(code_col) == code)

        if date_col in index_data.columns and index_data[date_col].dtype == pl.Utf8:
            index_data = index_data.with_columns([
                pl.col(date_col).str.strptime(pl.Date, "%Y-%m-%d").alias(date_col)
            ])

        if exchange_col not in index_data.columns and code_col in index_data.columns:
            index_data = index_data.with_columns([
                pl.col(code_col).str.slice(0, 2).alias(exchange_col)
            ])

        if start_date:
            start_date_obj = datetime.strptime(start_date, "%Y%m%d" if len(start_date) == 8 else "%Y-%m-%d").date()
            index_data = index_data.filter(pl.col(date_col) >= pl.lit(start_date_obj))

        if end_date:
            end_date_obj = datetime.strptime(end_date, "%Y%m%d" if len(end_date) == 8 else "%Y-%m-%d").date()
            index_data = index_data.filter(pl.col(date_col) <= pl.lit(end_date_obj))

        return index_data if not index_data.is_empty() else None

    def _calculate_index_ma(self, df: pl.DataFrame) -> pl.DataFrame:
        """"""
        if df is None or df.is_empty():
            return df

        name_col = "名称"
        date_col = "日期"
        close_col = "收盘"
        pct5_col = "5日涨跌幅"
        pct10_col = "10日涨跌幅"
        pct20_col = "20日涨跌幅"

        df_sorted = df.sort([name_col, date_col])
        df_with_changes = df_sorted.with_columns([
            ((pl.col(close_col) / pl.col(close_col).shift(5).over(name_col) - 1) * 100).round(2).alias(pct5_col),
            ((pl.col(close_col) / pl.col(close_col).shift(10).over(name_col) - 1) * 100).round(2).alias(pct10_col),
            ((pl.col(close_col) / pl.col(close_col).shift(20).over(name_col) - 1) * 100).round(2).alias(pct20_col),
        ])
        df_with_ma = df_with_changes.with_columns([
            pl.col(close_col).rolling_mean(window_size=5, min_periods=1).over(name_col).round(2).alias("MA5"),
            pl.col(close_col).rolling_mean(window_size=10, min_periods=1).over(name_col).round(2).alias("MA10"),
            pl.col(close_col).rolling_mean(window_size=20, min_periods=1).over(name_col).round(2).alias("MA20"),
        ])
        print(f"={df_with_ma.height}")
        return df_with_ma

    def _should_initialize_minute_data(self) -> bool:
        """"""
        date_col = "日期"
        try:
            if not self.minute_metadata_path.exists():
                return True

            all_data = pl.read_parquet(self.minute_metadata_path)
            if all_data.is_empty():
                return True

            latest_date = all_data[date_col].max()
            if latest_date is None:
                return True

            if isinstance(latest_date, datetime):
                latest_date_obj = latest_date.date()
            elif isinstance(latest_date, date):
                latest_date_obj = latest_date
            elif isinstance(latest_date, str):
                latest_date_obj = datetime.strptime(latest_date, "%Y-%m-%d").date()
            else:
                return True
            return latest_date_obj < (datetime.now().date() - timedelta(days=7))
        except Exception as e:
            print(f"[] {e}")
            return True

    def _initialize_two_months_minute_data(self) -> bool:
        """ 60 """
        try:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=60)
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")

            print(f"[] {start_date_str} -> {end_date_str}")
            success = self._fetch_and_cache_market_minute_data_akshare_range(start_date_str, end_date_str)
            print("[] " if success else "[] ")
            return success
        except Exception as e:
            print(f"[] {e}")
            import traceback
            traceback.print_exc()
            return False

    def get_market_minute_data(self, target_date: str, aggregate_minutes: int = 5) -> Optional[pl.DataFrame]:
        """"""
        date_col = "日期"
        time_col = "时间"
        try:
            date_str = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:]}" if len(target_date) == 8 else target_date

            if not self.minute_metadata_path.exists() or self._should_initialize_minute_data():
                self._initialize_two_months_minute_data()

            if self.minute_metadata_path.exists():
                try:
                    all_minute_data = pl.read_parquet(self.minute_metadata_path)
                except Exception as read_error:
                    print(f"[] {read_error}")
                    try:
                        os.remove(self.minute_metadata_path)
                    except Exception:
                        pass
                    rebuilt = self._initialize_two_months_minute_data()
                    if not rebuilt or not self.minute_metadata_path.exists():
                        return None
                    all_minute_data = pl.read_parquet(self.minute_metadata_path)

                minute_data = all_minute_data.filter(pl.col(date_col) == date_str)
                if not minute_data.is_empty():
                    if time_col not in minute_data.columns:
                        minute_data = self._fetch_and_cache_market_minute_data_akshare(date_str)
                    if minute_data is not None and aggregate_minutes and aggregate_minutes not in (0, 5) and aggregate_minutes > 1:
                        minute_data = self._aggregate_minute_data(minute_data, aggregate_minutes)
                    return minute_data

                raw_data = self._fetch_and_cache_market_minute_data_akshare(date_str)
                if raw_data is not None and aggregate_minutes and aggregate_minutes not in (0, 5) and aggregate_minutes > 1:
                    return self._aggregate_minute_data(raw_data, aggregate_minutes)
                return raw_data

            raw_data = self._fetch_and_cache_market_minute_data_akshare(date_str)
            if raw_data is not None and aggregate_minutes and aggregate_minutes not in (0, 5) and aggregate_minutes > 1:
                return self._aggregate_minute_data(raw_data, aggregate_minutes)
            return raw_data
        except Exception as e:
            print(f"[] {e}")
            return None

    def _convert_daily_to_minute_data(self, daily_data: pl.DataFrame, date_str: str, aggregate_minutes: int = 5) -> pl.DataFrame:
        """"""
        try:
            date_col = "日期"
            time_col = "时间"
            amount_suffix = "成交额"
            cum_prefix = "累计"

            morning_times = pd.date_range(f"{date_str} 09:30:00", f"{date_str} 11:30:00", freq="5min")
            afternoon_times = pd.date_range(f"{date_str} 13:00:00", f"{date_str} 15:00:00", freq="5min")
            all_times = list(morning_times) + list(afternoon_times)
            if not all_times:
                return daily_data

            turnover_cols = [col for col in daily_data.columns if str(col).endswith(amount_suffix)]
            if not turnover_cols:
                return daily_data

            minute_rows = []
            for row in daily_data.to_dicts():
                for ts in all_times:
                    item = {date_col: row.get(date_col), time_col: ts}
                    for col in turnover_cols:
                        value = row.get(col, 0) or 0
                        item[col] = float(value) / len(all_times)
                    minute_rows.append(item)

            if not minute_rows:
                return daily_data

            minute_df = pl.DataFrame(minute_rows).sort([date_col, time_col])
            for col in turnover_cols:
                cum_col = f"{cum_prefix}{col}"
                minute_df = minute_df.with_columns(pl.col(col).cumsum().over(date_col).alias(cum_col))
            return minute_df
        except Exception as e:
            print(f"[] {e}")
            return daily_data

    def _aggregate_minute_data(self, minute_data: pl.DataFrame, aggregate_minutes: int) -> pl.DataFrame:
        """"""
        try:
            date_col = "日期"
            time_col = "时间"
            amount_suffix = "成交额"
            total_col = "总成交额"

            if time_col not in minute_data.columns or date_col not in minute_data.columns:
                return minute_data

            if minute_data[time_col].dtype == pl.Utf8:
                minute_data = minute_data.with_columns(
                    pl.col(time_col).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").alias(time_col)
                )

            bucket_col = "聚合时间"
            minute_data = minute_data.with_columns(
                pl.col(time_col).dt.truncate(f"{aggregate_minutes}m").alias(bucket_col)
            )

            exchange_cols = [
                col for col in minute_data.columns
                if str(col).endswith(amount_suffix) and col != total_col
            ]
            if not exchange_cols:
                return minute_data

            aggregated = minute_data.group_by([date_col, bucket_col]).agg(
                [pl.col(time_col).first().alias(time_col)] +
                [pl.col(col).sum().alias(col) for col in exchange_cols]
            )
            aggregated = aggregated.with_columns(
                pl.sum_horizontal([pl.col(col) for col in exchange_cols]).alias(total_col)
            ).sort([date_col, bucket_col])
            return aggregated
        except Exception as e:
            print(f"[] {e}")
            return minute_data
    @staticmethod
    def _normalize_minute_columns(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        time_col = "时间"
        amount_col = "成交额"

        out = df.copy()
        rename_map = {}
        for col in out.columns:
            key = str(col).strip().lower()
            if key in {"day", "datetime", "time"}:
                rename_map[col] = time_col
            elif key == "amount":
                rename_map[col] = amount_col

        if rename_map:
            out = out.rename(columns=rename_map)

        return out

    def _prepare_minute_exchange_frame(
        self,
        df: pd.DataFrame,
        target_col: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pl.DataFrame:
        date_col = "日期"
        time_col = "时间"
        amount_col = "成交额"

        if df is None or df.empty:
            return pl.DataFrame(schema={date_col: pl.Utf8, time_col: pl.Datetime, target_col: pl.Float64})

        work = self._normalize_minute_columns(df)
        if time_col not in work.columns or amount_col not in work.columns:
            return pl.DataFrame(schema={date_col: pl.Utf8, time_col: pl.Datetime, target_col: pl.Float64})

        work = work[[time_col, amount_col]].copy()
        work[time_col] = pd.to_datetime(work[time_col], errors="coerce")
        work[amount_col] = pd.to_numeric(work[amount_col], errors="coerce")
        work = work.dropna(subset=[time_col, amount_col])
        if work.empty:
            return pl.DataFrame(schema={date_col: pl.Utf8, time_col: pl.Datetime, target_col: pl.Float64})

        work[date_col] = work[time_col].dt.strftime("%Y-%m-%d")
        if start_date:
            work = work[work[date_col] >= start_date]
        if end_date:
            work = work[work[date_col] <= end_date]
        if work.empty:
            return pl.DataFrame(schema={date_col: pl.Utf8, time_col: pl.Datetime, target_col: pl.Float64})

        work[target_col] = work[amount_col] / 100000000
        return pl.from_pandas(work[[date_col, time_col, target_col]])

    def _merge_exchange_minute_frames(
        self,
        sh_pl: pl.DataFrame,
        sz_pl: pl.DataFrame,
        bj_pl: pl.DataFrame,
    ) -> Optional[pl.DataFrame]:
        date_col = "日期"
        time_col = "时间"
        sh_col = "上证成交额"
        sz_col = "深证成交额"
        bj_col = "北证成交额"
        total_col = "总成交额"
        sz_cum_col = "累计深证成交额"
        sh_cum_col = "累计上证成交额"
        bj_cum_col = "累计北证成交额"
        total_cum_col = "累计总成交额"

        merged_data = None
        for frame in [sh_pl, sz_pl, bj_pl]:
            if frame is None or frame.is_empty():
                continue
            if merged_data is None:
                merged_data = frame
            else:
                merged_data = merged_data.join(frame, on=[date_col, time_col], how="outer")

        if merged_data is None or merged_data.is_empty():
            return None

        for col in [sh_col, sz_col, bj_col]:
            if col not in merged_data.columns:
                merged_data = merged_data.with_columns(pl.lit(0.0).alias(col))
            else:
                merged_data = merged_data.with_columns(pl.col(col).fill_null(0.0).alias(col))

        merged_data = merged_data.with_columns([
            (pl.col(sh_col) + pl.col(sz_col) + pl.col(bj_col)).alias(total_col)
        ])

        merged_data = merged_data.sort([date_col, time_col])
        merged_data = merged_data.with_columns([
            pl.col(sz_col).cumsum().over(date_col).alias(sz_cum_col),
            pl.col(sh_col).cumsum().over(date_col).alias(sh_cum_col),
            pl.col(bj_col).cumsum().over(date_col).alias(bj_cum_col),
            pl.col(total_col).cumsum().over(date_col).alias(total_cum_col),
        ])

        return merged_data

    def _upsert_minute_cache(
        self,
        minute_data: pl.DataFrame,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> None:
        date_col = "日期"
        time_col = "时间"

        if minute_data is None or minute_data.is_empty():
            return

        existing_data = None
        if self.minute_metadata_path.exists():
            try:
                existing_data = pl.read_parquet(self.minute_metadata_path)
            except Exception as e:
                print(f"[] {e}")

        if existing_data is not None and not existing_data.is_empty():
            if start_date and end_date:
                existing_data = existing_data.filter(
                    (pl.col(date_col) < start_date) | (pl.col(date_col) > end_date)
                )
            else:
                target_dates = minute_data.select(date_col).unique().to_series().to_list()
                existing_data = existing_data.filter(~pl.col(date_col).is_in(target_dates))

            all_cols = list(dict.fromkeys(existing_data.columns + minute_data.columns))
            for col in all_cols:
                if col not in existing_data.columns:
                    existing_data = existing_data.with_columns(pl.lit(None).alias(col))
                if col not in minute_data.columns:
                    minute_data = minute_data.with_columns(pl.lit(None).alias(col))

            updated_data = pl.concat(
                [existing_data.select(all_cols), minute_data.select(all_cols)],
                how="vertical_relaxed",
            )
        else:
            updated_data = minute_data

        updated_data = updated_data.unique(subset=[date_col, time_col], keep="last").sort([date_col, time_col])
        updated_data.write_parquet(self.minute_metadata_path)

    def _fetch_and_cache_market_minute_data_akshare_range(self, start_date: str, end_date: str) -> bool:
        """"""
        try:
            sh_col = "上证成交额"
            sz_col = "深证成交额"
            bj_col = "北证成交额"

            print(f"[] {start_date} -> {end_date}")
            start_dt = f"{start_date} 09:30:00"
            end_dt = f"{end_date} 15:00:00"

            def fetch_min_df(symbol: str) -> pd.DataFrame:
                try:
                    df = ak.index_zh_a_hist_min_em(symbol=symbol, period="5", start_date=start_dt, end_date=end_dt)
                    if df is not None and not df.empty:
                        print(f"[] =ak.index_zh_a_hist_min_em ={symbol} =5 ")
                        return df
                except Exception as fetch_error:
                    print(f"[] =ak.index_zh_a_hist_min_em ={symbol} =5 {fetch_error}")
                print(f"[] ={symbol} ")
                return pd.DataFrame()

            sh_min = fetch_min_df("000001")
            sz_min = fetch_min_df("399001")
            bj_min = fetch_min_df("899050")

            if (sh_min is None or sh_min.empty) and (sz_min is None or sz_min.empty):
                print("[] ")
                return False

            sh_pl = self._prepare_minute_exchange_frame(sh_min, sh_col, start_date, end_date)
            sz_pl = self._prepare_minute_exchange_frame(sz_min, sz_col, start_date, end_date)
            bj_pl = self._prepare_minute_exchange_frame(bj_min, bj_col, start_date, end_date)

            merged_data = self._merge_exchange_minute_frames(sh_pl, sz_pl, bj_pl)
            if merged_data is None or merged_data.is_empty():
                print("[] ")
                return False

            self._upsert_minute_cache(merged_data, start_date=start_date, end_date=end_date)
            print(f"[] ={merged_data.height}")
            return True

        except Exception as e:
            print(f"[] {e}")
            import traceback
            traceback.print_exc()
            return False

    def _fetch_and_cache_market_minute_data_akshare(self, date_str: str) -> Optional[pl.DataFrame]:
        """"""
        date_col = "日期"

        success = self._fetch_and_cache_market_minute_data_akshare_range(date_str, date_str)
        if not success:
            return None

        if not self.minute_metadata_path.exists():
            return None

        try:
            all_minute_data = pl.read_parquet(self.minute_metadata_path)
            day_data = all_minute_data.filter(pl.col(date_col) == date_str)
            if day_data.is_empty():
                return None
            return day_data
        except Exception as e:
            print(f"[] {e}")
            return None

    def get_market_volume_comparison(self, current_date: str, previous_date: str = None) -> Optional[Dict]:
        """"""
        total_col = "总成交额"
        total_cum_col = "累计总成交额"
        try:
            current_data = self.get_market_minute_data(current_date, aggregate_minutes=5)
            if current_data is None:
                print(f"{current_date}")
                return None

            if previous_date is None:
                current_date_obj = datetime.strptime(current_date, "%Y-%m-%d").date()
                previous_date = None
                for i in range(1, 8):
                    prev_date = current_date_obj - timedelta(days=i)
                    if prev_date.weekday() < 5:
                        previous_date = prev_date.strftime("%Y-%m-%d")
                        break
                if previous_date is None:
                    previous_date = (current_date_obj - timedelta(days=7)).strftime("%Y-%m-%d")

            previous_data = self.get_market_minute_data(previous_date, aggregate_minutes=5)
            if previous_data is None:
                print(f"{previous_date}")
                return None

            current_total = current_data[total_cum_col].max() if total_cum_col in current_data.columns else current_data[total_col].sum()
            previous_total = previous_data[total_cum_col].max() if total_cum_col in previous_data.columns else previous_data[total_col].sum()
            change_amount = current_total - previous_total
            change_pct = (change_amount / previous_total * 100) if previous_total > 0 else 0

            comparison_data = {
                "current_total": float(current_total),
                "previous_total": float(previous_total),
                "change_amount": float(change_amount),
                "change_pct": float(change_pct),
                "current_date": current_date,
                "previous_date": previous_date,
            }

            return {
                "current_data": current_data,
                "previous_data": previous_data,
                "comparison_data": comparison_data,
            }
        except Exception as e:
            print(f"[] {e}")
            return None

