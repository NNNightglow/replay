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
from typing import Optional, Dict, List, Tuple, Any
from contextlib import contextmanager
import akshare as ak
import pandas as pd
import numpy as np
import baostock as bs
import threading
import tempfile
import shutil
import json
# from akshare.utils.requests_fun import requests_obj

# requests_obj.headers.update({
#     "User-Agent": "Mozilla/5.0 ...",
#     "Referer": "https://quote.eastmoney.com/"
# })
@contextmanager
def temporary_disable_proxy(disable: bool = False):
    """临时禁用代理，避免部分数据源在代理环境下请求失败。"""
    if not disable:
        yield
        return
    proxy_keys = ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY")
    old_env = {k: os.environ.get(k) for k in proxy_keys + ("NO_PROXY", "no_proxy")}
    try:
        for key in proxy_keys:
            os.environ.pop(key, None)
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"
        yield
    finally:
        for key, value in old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def safe_network_request(func, *args, max_retries=3, timeout_seconds=30, disable_proxy=False, **kwargs):
    """安全的网络请求包装器，带重试机制"""
    import time
    import random

    for attempt in range(max_retries):
        try:
            with temporary_disable_proxy(disable_proxy):
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
        self.bak_basic_cache_file = self.metadata_path / "bak_basic_history.parquet"
        self.bak_basic_state_file = self.metadata_path / "bak_basic_sync_state.json"

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

    def load_bak_basic_metadata(self) -> Optional[pl.DataFrame]:
        """加载 bak_basic 历史缓存数据。"""
        try:
            if self.bak_basic_cache_file.exists():
                df = pl.read_parquet(self.bak_basic_cache_file)
                print(f"✅ 成功加载 bak_basic 缓存: {df.height} 条记录")
                return df
            print(f"⚠️ bak_basic 缓存文件不存在: {self.bak_basic_cache_file}")
            return None
        except Exception as e:
            print(f"❌ 加载 bak_basic 缓存失败: {e}")
            return None

    def _read_bak_basic_state(self) -> Dict[str, Any]:
        """读取 bak_basic 同步状态。"""
        if not self.bak_basic_state_file.exists():
            return {}
        try:
            return json.loads(self.bak_basic_state_file.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"⚠️ 读取 bak_basic 状态文件失败: {e}")
            return {}

    def _write_bak_basic_state(self, state: Dict[str, Any]) -> None:
        """写入 bak_basic 同步状态。"""
        try:
            self.bak_basic_state_file.write_text(
                json.dumps(state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            print(f"⚠️ 写入 bak_basic 状态文件失败: {e}")

    @staticmethod
    def _to_compact_date(value: Any) -> Optional[str]:
        """转换为 YYYYMMDD 格式。"""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime("%Y%m%d")
        if isinstance(value, date):
            return value.strftime("%Y%m%d")
        if isinstance(value, str):
            text = value.strip()
            if len(text) == 8 and text.isdigit():
                return text
            if len(text) == 10 and "-" in text:
                return text.replace("-", "")
        return None

    @staticmethod
    def _next_compact_date(compact_date: str) -> Optional[str]:
        """计算下一个自然日，返回 YYYYMMDD。"""
        try:
            d = datetime.strptime(compact_date, "%Y%m%d").date()
            return (d + timedelta(days=1)).strftime("%Y%m%d")
        except Exception:
            return None

    @staticmethod
    def _init_tushare_client():
        """初始化 tushare pro 客户端，并兼容私有 HTTP 网关配置。"""
        try:
            import tushare as ts
        except Exception as e:
            raise RuntimeError(
                "未安装 tushare，请先安装与账号兼容的版本（例如 1.4.24）"
            ) from e

        token = (
            os.getenv("TUSHARE_API_KEY")
            or os.getenv("TUSHARE_TOKEN")
            or os.getenv("TS_TOKEN")
        )
        if not token:
            raise RuntimeError("未找到 tushare token，请在 .env 中配置 TUSHARE_API_KEY")

        pro = ts.pro_api(token)
        # 兼容私有服务：按你的调用要求强制设置 token 与 http_url。
        pro._DataApi__token = token
        pro._DataApi__http_url = os.getenv(
            "TUSHARE_HTTP_URL",
            "http://lianghua.nanyangqiankun.top",
        )
        return pro

    @staticmethod
    def _normalize_bak_basic_frame(df: pd.DataFrame) -> Optional[pl.DataFrame]:
        """标准化 bak_basic 返回字段并转换到 Polars。"""
        if df is None or df.empty:
            return None

        expected_cols = [
            "trade_date", "ts_code", "name", "industry", "area", "pe",
            "float_share", "total_share", "total_assets", "liquid_assets",
            "fixed_assets", "reserved", "reserved_pershare", "eps", "bvps",
            "pb", "list_date", "undp", "per_undp", "rev_yoy", "profit_yoy",
            "gpr", "npr", "holder_num"
        ]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None

        pl_df = pl.from_pandas(df[expected_cols])
        pl_df = pl_df.with_columns(
            [
                pl.col("trade_date").cast(pl.Utf8, strict=False),
                pl.col("ts_code").cast(pl.Utf8, strict=False),
                pl.col("name").cast(pl.Utf8, strict=False),
                pl.col("industry").cast(pl.Utf8, strict=False),
                pl.col("area").cast(pl.Utf8, strict=False),
                pl.col("list_date").cast(pl.Utf8, strict=False),
                pl.col("pe").cast(pl.Float64, strict=False),
                pl.col("float_share").cast(pl.Float64, strict=False),
                pl.col("total_share").cast(pl.Float64, strict=False),
                pl.col("total_assets").cast(pl.Float64, strict=False),
                pl.col("liquid_assets").cast(pl.Float64, strict=False),
                pl.col("fixed_assets").cast(pl.Float64, strict=False),
                pl.col("reserved").cast(pl.Float64, strict=False),
                pl.col("reserved_pershare").cast(pl.Float64, strict=False),
                pl.col("eps").cast(pl.Float64, strict=False),
                pl.col("bvps").cast(pl.Float64, strict=False),
                pl.col("pb").cast(pl.Float64, strict=False),
                pl.col("undp").cast(pl.Float64, strict=False),
                pl.col("per_undp").cast(pl.Float64, strict=False),
                pl.col("rev_yoy").cast(pl.Float64, strict=False),
                pl.col("profit_yoy").cast(pl.Float64, strict=False),
                pl.col("gpr").cast(pl.Float64, strict=False),
                pl.col("npr").cast(pl.Float64, strict=False),
                pl.col("holder_num").cast(pl.Int64, strict=False),
            ]
        )
        return pl_df

    def update_bak_basic_metadata(
        self,
        start_date: str = "20160101",
        end_date: Optional[str] = None,
        force_full: bool = False,
        progress_callback=None,
        save_every_days: int = 20,
        throttle_seconds: float = 0.08,
    ) -> bool:
        """增量更新 bak_basic 历史缓存（默认从 2016-01-01 起）。"""
        try:
            pro = self._init_tushare_client()
            end_compact = self._to_compact_date(end_date) or datetime.now().strftime("%Y%m%d")
            start_compact = self._to_compact_date(start_date) or "20160101"

            existing = None if force_full else self.load_bak_basic_metadata()
            state = {} if force_full else self._read_bak_basic_state()

            latest_existing = None
            if existing is not None and not existing.is_empty() and "trade_date" in existing.columns:
                latest_existing = existing["trade_date"].max()
                latest_existing = self._to_compact_date(latest_existing)

            latest_state = self._to_compact_date(state.get("latest_trade_date"))
            latest_synced = max([d for d in [latest_existing, latest_state] if d], default=None)
            if latest_synced:
                next_date = self._next_compact_date(latest_synced)
                if next_date:
                    start_compact = max(start_compact, next_date)

            if start_compact > end_compact:
                print(
                    f"✅ bak_basic 已是最新，无需更新: start={start_compact}, end={end_compact}"
                )
                return True

            cal = pro.trade_cal(
                exchange="",
                start_date=start_compact,
                end_date=end_compact,
                is_open="1",
                fields="cal_date,is_open",
            )
            if cal is None or cal.empty:
                print("⚠️ 未获取到可更新的交易日，跳过 bak_basic 更新")
                return True

            trade_days = sorted(cal["cal_date"].astype(str).unique().tolist())
            total_days = len(trade_days)
            print(f"📅 bak_basic 待更新交易日: {total_days} 天 ({start_compact} -> {end_compact})")

            all_new_frames: List[pl.DataFrame] = []
            success_days = 0
            for idx, trade_day in enumerate(trade_days, start=1):
                try:
                    day_df = pro.bak_basic(
                        trade_date=trade_day,
                        fields=(
                            "trade_date,ts_code,name,industry,area,pe,float_share,total_share,"
                            "total_assets,liquid_assets,fixed_assets,reserved,reserved_pershare,"
                            "eps,bvps,pb,list_date,undp,per_undp,rev_yoy,profit_yoy,gpr,npr,holder_num"
                        ),
                    )
                    day_pl = self._normalize_bak_basic_frame(day_df)
                    if day_pl is not None and not day_pl.is_empty():
                        all_new_frames.append(day_pl)
                    success_days += 1
                except Exception as e:
                    print(f"⚠️ bak_basic 拉取失败 {trade_day}: {e}")

                if progress_callback:
                    progress_callback(idx, total_days, f"bak_basic 更新中: {trade_day} ({idx}/{total_days})")

                if (
                    save_every_days > 0
                    and idx % save_every_days == 0
                    and all_new_frames
                ):
                    merged = pl.concat(all_new_frames, how="vertical_relaxed")
                    if existing is not None and not existing.is_empty():
                        merged = pl.concat([existing, merged], how="vertical_relaxed")
                    merged = merged.unique(subset=["trade_date", "ts_code"], keep="last").sort(["trade_date", "ts_code"])
                    safe_write_parquet(merged, str(self.bak_basic_cache_file))
                    existing = merged
                    all_new_frames = []

                if throttle_seconds > 0:
                    time.sleep(throttle_seconds)

            if all_new_frames:
                merged = pl.concat(all_new_frames, how="vertical_relaxed")
                if existing is not None and not existing.is_empty():
                    merged = pl.concat([existing, merged], how="vertical_relaxed")
            else:
                merged = existing

            if merged is None:
                print("⚠️ bak_basic 本次未产生可写入数据")
                return False

            merged = merged.unique(subset=["trade_date", "ts_code"], keep="last").sort(["trade_date", "ts_code"])
            ok = safe_write_parquet(merged, str(self.bak_basic_cache_file))
            if not ok:
                return False

            latest_trade_date = merged["trade_date"].max() if "trade_date" in merged.columns else None
            self._write_bak_basic_state(
                {
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "latest_trade_date": latest_trade_date,
                    "rows": merged.height,
                    "source": "tushare.bak_basic",
                    "http_url": os.getenv("TUSHARE_HTTP_URL", "http://lianghua.nanyangqiankun.top"),
                }
            )
            print(
                f"✅ bak_basic 更新完成: 成功处理交易日 {success_days}/{total_days}, "
                f"当前总记录 {merged.height}"
            )
            return True
        except Exception as e:
            print(f"❌ 更新 bak_basic 缓存失败: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _fill_missing_data_for_date_range(
        self,
        start_date: str,
        end_date: str,
        stock_codes: List[str],
        existing_code_to_name: Optional[Dict[str, str]] = None,
        use_existing_bs_session: bool = False
    ) -> Optional[pl.DataFrame]:
        """填补日期区间内的缺失数据
        
        Args:
            start_date: 开始日期字符串，格式为'YYYY-MM-DD'
            end_date: 结束日期字符串，格式为'YYYY-MM-DD'
            stock_codes: 需要填补的股票代码列表
            
        Returns:
            pl.DataFrame: 填补的数据，如果失败则返回None
        """
        try:
            from utils.trading_calendar import trading_calendar
            
            # 检查日期格式
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            if start_date_obj > end_date_obj:
                return None
            
            # 分离主板股票和北交所股票
            mainboard_codes = []  # baostock格式：sh.600000, sz.000001等
            beijiao_codes = []    # 北交所：4, 8, 9开头
            
            # 获取股票列表以确定代码格式
            try:
                stock_info = safe_network_request(
                    ak.stock_info_a_code_name,
                    max_retries=2,
                    timeout_seconds=30,
                    disable_proxy=True
                )
                if stock_info is not None and not stock_info.empty:
                    code_to_name = dict(zip(stock_info['code'], stock_info['name']))
                else:
                    code_to_name = {}
            except:
                code_to_name = {}
            
            # 尝试从现有数据获取股票名称映射（优先使用外部传入，避免重复加载）
            if existing_code_to_name:
                for k, v in existing_code_to_name.items():
                    if k not in code_to_name:
                        code_to_name[k] = v
            else:
                existing_metadata = self.load_metadata()
                if existing_metadata is not None and not existing_metadata.is_empty() and '代码' in existing_metadata.columns and '名称' in existing_metadata.columns:
                    existing_names = existing_metadata.select(['代码', '名称']).unique()
                    for row in existing_names.iter_rows(named=True):
                        if row['代码'] not in code_to_name:
                            code_to_name[row['代码']] = row['名称']
            
            all_filled_data = []
            
            # 处理主板股票（需要转换为baostock格式）
            for code in stock_codes:
                # 判断是否为北交所股票
                if code.startswith(('4', '8', '9')):
                    beijiao_codes.append(code)
                else:
                    # 转换为baostock格式
                    if code.startswith('6'):
                        baostock_code = f'sh.{code}'
                    elif code.startswith(('0', '3')):
                        baostock_code = f'sz.{code}'
                    else:
                        continue
                    mainboard_codes.append((baostock_code, code, code_to_name.get(code, code)))
            
            # 获取主板股票数据
            # fetch mainboard data
            if mainboard_codes:
                try:
                    logged_in_here = False
                    bs_session_ready = use_existing_bs_session
                    if not use_existing_bs_session:
                        lg = bs.login()
                        if lg.error_code != '0':
                            print(f"[fill-gap] baostock login failed: {lg.error_msg}")
                        else:
                            bs_session_ready = True
                            logged_in_here = True

                    if bs_session_ready:
                        for baostock_code, original_code, stock_name in mainboard_codes:
                            try:
                                rs = bs.query_history_k_data_plus(
                                    baostock_code,
                                    "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg",
                                    start_date=start_date,
                                    end_date=end_date,
                                    frequency="d",
                                    adjustflag="2"
                                )

                                if rs.error_code == '0':
                                    data_list = []
                                    while rs.next():
                                        data_list.append(rs.get_row_data())

                                    if data_list:
                                        temp_df = pd.DataFrame(data_list, columns=rs.fields)
                                        temp_df['\u540d\u79f0'] = stock_name
                                        temp_df['\u4ee3\u7801'] = original_code
                                        all_filled_data.append(temp_df)
                            except Exception as e:
                                print(f"[fill-gap] failed to fetch {original_code} from {start_date} to {end_date}: {e}")
                                continue

                    if logged_in_here:
                        bs.logout()
                except Exception as e:
                    print(f"[fill-gap] failed to fetch mainboard data: {e}")

            # fetch Beijing exchange data
            if beijiao_codes:
                try:
                    # convert date format for akshare
                    start_date_akshare = start_date_obj.strftime('%Y%m%d')
                    end_date_akshare = end_date_obj.strftime('%Y%m%d')
                    
                    for code in beijiao_codes:
                        try:
                            df = safe_network_request(
                                ak.stock_zh_a_hist,
                                symbol=code,
                                period='daily',
                                start_date=start_date_akshare,
                                end_date=end_date_akshare,
                                adjust="qfq",
                                max_retries=2,
                                timeout_seconds=30,
                                disable_proxy=True
                            )
                             
                            if df is not None and not df.empty:
                                df = df.rename(columns={'股票代码': '代码'})
                                if code in code_to_name:
                                    df['名称'] = code_to_name[code]
                                all_filled_data.append(df)
                        except Exception as e:
                            print(f"⚠️ 获取北交所股票 {code} 在 {start_date} 到 {end_date} 的数据失败: {e}")
                            continue
                except Exception as e:
                    print(f"⚠️ 获取北交所股票数据失败: {e}")
            
            # 合并所有填补的数据
            if all_filled_data:
                combined_pd = pd.concat(all_filled_data, ignore_index=True)
                
                # 转换为Polars格式
                filled_pl = pl.from_pandas(combined_pd)
                
                # 处理列名和格式
                select_exprs = []
                if 'date' in filled_pl.columns:
                    select_exprs.append(pl.col("date").alias("日期"))
                elif '日期' in filled_pl.columns:
                    select_exprs.append(pl.col("日期"))
                
                if 'open' in filled_pl.columns:
                    select_exprs.append(pl.col("open").cast(pl.Float64, strict=False).alias("开盘"))
                elif '开盘' in filled_pl.columns:
                    select_exprs.append(pl.col("开盘"))
                
                if 'close' in filled_pl.columns:
                    select_exprs.append(pl.col("close").cast(pl.Float64, strict=False).alias("收盘"))
                elif '收盘' in filled_pl.columns:
                    select_exprs.append(pl.col("收盘"))
                
                if 'high' in filled_pl.columns:
                    select_exprs.append(pl.col("high").cast(pl.Float64, strict=False).alias("最高"))
                elif '最高' in filled_pl.columns:
                    select_exprs.append(pl.col("最高"))
                
                if 'low' in filled_pl.columns:
                    select_exprs.append(pl.col("low").cast(pl.Float64, strict=False).alias("最低"))
                elif '最低' in filled_pl.columns:
                    select_exprs.append(pl.col("最低"))
                
                if 'volume' in filled_pl.columns:
                    select_exprs.append(pl.col("volume").cast(pl.Int64, strict=False).alias("成交量"))
                elif '成交量' in filled_pl.columns:
                    select_exprs.append(pl.col("成交量"))
                
                if 'amount' in filled_pl.columns:
                    select_exprs.append(pl.col("amount").cast(pl.Float64, strict=False).alias("成交额"))
                elif '成交额' in filled_pl.columns:
                    select_exprs.append(pl.col("成交额"))
                
                if 'turn' in filled_pl.columns:
                    select_exprs.append(pl.col("turn").cast(pl.Float64, strict=False).alias("换手率"))
                elif '换手率' in filled_pl.columns:
                    select_exprs.append(pl.col("换手率"))
                
                if 'pctChg' in filled_pl.columns:
                    select_exprs.append(pl.col("pctChg").cast(pl.Float64, strict=False).alias("涨跌幅"))
                elif '涨跌幅' in filled_pl.columns:
                    select_exprs.append(pl.col("涨跌幅"))
                
                if '名称' in filled_pl.columns:
                    select_exprs.append(pl.col("名称"))
                
                if '代码' in filled_pl.columns:
                    select_exprs.append(pl.col("代码"))
                elif 'code' in filled_pl.columns:
                    select_exprs.append(pl.col("code").str.slice(3).alias("代码"))
                
                if select_exprs:
                    filled_pl = filled_pl.select(select_exprs)
                    
                    # 计算振幅和涨跌额
                    if all([c in filled_pl.columns for c in ["最高", "最低", "收盘", "涨跌幅"]]):
                        filled_pl = filled_pl.with_columns([
                            ((pl.col("最高") - pl.col("最低")) / pl.col("最低") * 100).round(2).alias("振幅"),
                            (pl.col("收盘") * pl.col("涨跌幅") / 100).round(2).alias("涨跌额")
                        ])
                    
                    # 转换日期格式
                    if '日期' in filled_pl.columns:
                        if filled_pl['日期'].dtype == pl.Utf8:
                            filled_pl = filled_pl.with_columns([
                                pl.col("日期").str.strptime(pl.Date, format='%Y-%m-%d')
                            ])
                    
                    return filled_pl
            
            return None
            
        except Exception as e:
            print(f"⚠️ 填补数据时出错: {e}")
            import traceback
            traceback.print_exc()
            return None


    def update_metadata(self, start_date: str = None, end_date: str = None,
                       progress_callback=None, fill_gaps: bool = False,
                       include_bjs_update: bool = False) -> bool:
        """更新股票元数据
        
        Args:
            start_date: 开始日期，格式为'YYYY-MM-DD'
            end_date: 结束日期，格式为'YYYY-MM-DD'
            progress_callback: 进度回调函数
            fill_gaps: 是否填补数据中的空缺（默认False）
            include_bjs_update: 是否更新北交所数据（默认False）
        """
        if progress_callback:
            progress_callback(0, 100, "开始更新股票元数据")
        
        # 获取现有元数据
        existing_metadata = self.load_metadata()
        
        # 填补数据空缺
        if fill_gaps and existing_metadata is not None and not existing_metadata.is_empty():
            print("🔍 检测并填补数据空缺...")
            try:
                from utils.trading_calendar import trading_calendar
                
                # 确保日期列存在且为Date类型
                if '日期' in existing_metadata.columns and '代码' in existing_metadata.columns:
                    # 按股票代码分组，找出每个股票的日期范围
                    stock_date_ranges = existing_metadata.group_by('代码').agg([
                        pl.col('日期').min().alias('min_date'),
                        pl.col('日期').max().alias('max_date')
                    ])
                    
                    # 获取所有已有的日期-代码组合
                    existing_date_code_pairs = existing_metadata.select(['日期', '代码']).unique()
                    existing_date_code_set = set(
                        (row['日期'], row['代码']) 
                        for row in existing_date_code_pairs.iter_rows(named=True)
                    )
                    
                    # 找出需要填补的日期
                    missing_data = []
                    total_stocks = stock_date_ranges.height
                    filled_count = 0
                    
                    for idx, row in enumerate(stock_date_ranges.iter_rows(named=True)):
                        stock_code = row['代码']
                        min_date = row['min_date']
                        max_date = row['max_date']
                        
                        if min_date is None or max_date is None:
                            continue
                        
                        # 生成该股票日期范围内的所有交易日
                        trading_days = trading_calendar.get_trading_days_in_range(min_date, max_date)
                        
                        # 找出缺失的交易日
                        for trading_day in trading_days:
                            if (trading_day, stock_code) not in existing_date_code_set:
                                missing_data.append({
                                    'code': stock_code,
                                    'date': trading_day
                                })
                        
                        if (idx + 1) % 100 == 0:
                            print(f"  已检查 {idx + 1}/{total_stocks} 只股票的空缺情况...")
                    
                    if missing_data:
                        print(f"📊 发现 {len(missing_data)} 条缺失数据，开始填补...")
                        
                        # 按股票代码分组，找出每只股票的缺失日期范围
                        missing_by_stock = {}
                        for item in missing_data:
                            stock_code = item['code']
                            date = item['date']
                            if stock_code not in missing_by_stock:
                                missing_by_stock[stock_code] = []
                            missing_by_stock[stock_code].append(date)
                        
                        # 找出每只股票的缺失日期范围（最早和最晚日期）
                        stock_date_ranges = {}
                        for stock_code, dates in missing_by_stock.items():
                            if dates:
                                min_date = min(dates)
                                max_date = max(dates)
                                stock_date_ranges[stock_code] = (min_date, max_date)
                        
                        # 按日期范围分组，将具有相同日期范围的股票合并
                        range_to_stocks = {}
                        for stock_code, (min_date, max_date) in stock_date_ranges.items():
                            date_range_key = (min_date, max_date)
                            if date_range_key not in range_to_stocks:
                                range_to_stocks[date_range_key] = []
                            range_to_stocks[date_range_key].append(stock_code)
                        
                        # 获取缺失日期的数据
                        existing_code_to_name = {}
                        if (
                            existing_metadata is not None
                            and not existing_metadata.is_empty()
                            and '代码' in existing_metadata.columns
                            and '名称' in existing_metadata.columns
                        ):
                            name_rows = existing_metadata.select(['代码', '名称']).unique()
                            existing_code_to_name = {
                                row['代码']: row['名称']
                                for row in name_rows.iter_rows(named=True)
                            }

                        gap_filled_data = []
                        total_ranges = len(range_to_stocks)
                        bs_session_ready = False
                        if total_ranges > 0:
                            lg = bs.login()
                            if lg.error_code != '0':
                                print(f"⚠️ 缺口批量填补前 baostock 登录失败，将回退为区间内独立登录: {lg.error_msg}")
                            else:
                                bs_session_ready = True

                        try:
                            for range_idx, ((min_date, max_date), stock_codes) in enumerate(range_to_stocks.items()):
                                start_date_str = min_date.strftime('%Y-%m-%d')
                                end_date_str = max_date.strftime('%Y-%m-%d')
                                print(f"  填补日期区间 {start_date_str} 到 {end_date_str} ({range_idx + 1}/{total_ranges})，涉及 {len(stock_codes)} 只股票...")
                                
                                # 调用填补空缺的方法，使用日期区间批量获取
                                filled = self._fill_missing_data_for_date_range(
                                    start_date_str,
                                    end_date_str,
                                    stock_codes,
                                    existing_code_to_name=existing_code_to_name,
                                    use_existing_bs_session=bs_session_ready
                                )
                                if filled is not None and not filled.is_empty():
                                    gap_filled_data.append(filled)
                                    filled_count += filled.height
                        finally:
                            if bs_session_ready:
                                bs.logout()
                        
                        if gap_filled_data:
                            # 合并填补的数据
                            gap_filled_combined = pl.concat(gap_filled_data, how="vertical")
                            
                            # 与现有数据合并
                            existing_metadata = pl.concat([existing_metadata, gap_filled_combined], how="vertical")
                            existing_metadata = existing_metadata.unique(subset=["日期", "代码"], keep="last")
                            
                            # 保存填补后的数据
                            safe_write_parquet(existing_metadata, str(self.stock_metadata_file))
                            print(f"✅ 成功填补 {filled_count} 条缺失数据")
                        else:
                            print("⚠️ 未能获取到缺失数据")
                    else:
                        print("✅ 未发现数据空缺")
                        
            except Exception as e:
                print(f"⚠️ 填补数据空缺时出错: {e}")
                import traceback
                traceback.print_exc()
        
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
                latest_date = (datetime.now() - timedelta(days=90)).date()

            if start_date is None:
                start_date = (latest_date + timedelta(days=1)).strftime('%Y-%m-%d')
            if end_date is None:
                # 计算应当更新到的“最新交易日”
                from datetime import time as dt_time
                try:
                    from utils.trading_calendar import trading_calendar
                except Exception:
                    trading_calendar = None

                now_dt = datetime.now()
                current_date = now_dt.date()
                update_time = dt_time(18, 0)

                def get_prev_trading_day(d):
                    if trading_calendar is not None:
                        return trading_calendar.get_previous_trading_day(d)
                    # 兜底：仅根据周末回退
                    check = d - timedelta(days=1)
                    for _ in range(15):
                        if check.weekday() < 5:
                            return check
                        check -= timedelta(days=1)
                    return d - timedelta(days=1)

                def is_trading(d):
                    if trading_calendar is not None:
                        return trading_calendar.is_trading_day(d)
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
                from utils.trading_calendar import trading_calendar
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
                if not trading_calendar.is_trading_day(end_date_obj):
                    end_date = trading_calendar.get_previous_trading_day(end_date_obj).strftime('%Y-%m-%d')
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
                    from utils.trading_calendar import trading_calendar
                    d = datetime.strptime(attempt_end_date, '%Y-%m-%d').date()
                    attempt_end_date = trading_calendar.get_previous_trading_day(d).strftime('%Y-%m-%d')
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
                from utils.trading_calendar import trading_calendar
                d = datetime.strptime(attempt_end_date, '%Y-%m-%d').date()
                attempt_end_date = trading_calendar.get_previous_trading_day(d).strftime('%Y-%m-%d')
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
        total_a_stocks = len(a_stocks)
        processed_count = 0
        success_count = 0
        empty_count = 0
        failed_count = 0
        error_reason_counter: Dict[str, int] = {}
        empty_stock_examples: List[Tuple[str, str]] = []
        late_listing_count = 0
        late_listing_examples: List[Tuple[str, str, str]] = []
        total_elapsed_seconds = 0.0
        progress_interval = 100 if total_a_stocks >= 1000 else 20
        batch_pause_every = 100
        batch_pause_seconds = 0.1
        loop_start_ts = time.perf_counter()

        print(
            f"📌 沪深市场拉取任务: {total_a_stocks} 只股票, 日期区间 {start_date} -> {end_date}, "
            f"每{batch_pause_every}只暂停{batch_pause_seconds:.1f}s"
        )

        for index, stock in a_stocks.iterrows():
            stock_code = stock['code']
            stock_name = stock['code_name']
            stock_start_ts = time.perf_counter()
            row_count = 0
            
            try:
                #print(f"正在获取 {stock_code} {stock_name} 的数据...")
                
                rs = bs.query_history_k_data_plus(stock_code,
                    "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg",
                    start_date=start_date, end_date=end_date,
                    frequency="d", adjustflag="2")
                
                if rs.error_code != '0':
                    reason = f"{rs.error_code}:{rs.error_msg}" if rs.error_msg else f"{rs.error_code}:unknown"
                    failed_count += 1
                    error_reason_counter[reason] = error_reason_counter.get(reason, 0) + 1
                    failed_stocks.append((stock_code, stock_name))
                    if failed_count <= 5 or failed_count % 50 == 0:
                        print(f"⚠️ 获取 {stock_code} {stock_name} 数据失败: {reason} (累计失败 {failed_count})")
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
                    success_count += 1
                    row_count = len(data_list)

                    first_trade_date = data_list[0][0] if data_list[0] else None
                    if first_trade_date and first_trade_date > start_date:
                        late_listing_count += 1
                        if len(late_listing_examples) < 10:
                            late_listing_examples.append((stock_code, stock_name, first_trade_date))
                else:
                    empty_count += 1
                    if len(empty_stock_examples) < 10:
                        empty_stock_examples.append((stock_code, stock_name))
                
            except Exception as e:
                failed_count += 1
                reason = f"exception:{str(e)}"
                error_reason_counter[reason] = error_reason_counter.get(reason, 0) + 1
                failed_stocks.append((stock_code, stock_name))
                if failed_count <= 5 or failed_count % 50 == 0:
                    print(f"⚠️ 处理 {stock_code} {stock_name} 时出错: {str(e)} (累计失败 {failed_count})")
                continue
            finally:
                processed_count += 1
                stock_elapsed = time.perf_counter() - stock_start_ts
                total_elapsed_seconds += stock_elapsed


                if processed_count % progress_interval == 0 or processed_count == total_a_stocks:
                    avg_elapsed = total_elapsed_seconds / processed_count if processed_count else 0.0
                    eta_seconds = avg_elapsed * max(total_a_stocks - processed_count, 0)
                    wall_seconds = time.perf_counter() - loop_start_ts
                    print(
                        f"📈 沪深市场进度 {processed_count}/{total_a_stocks} | 成功:{success_count} "
                        f"空数据:{empty_count} 失败:{failed_count} | 平均:{avg_elapsed:.2f}s/只 "
                        f"| 已用:{wall_seconds / 60:.1f}m 预计剩余:{eta_seconds / 60:.1f}m"
                    )

                # 节流：仅在每处理完100只后短暂休息，避免每只股票都等待
                if (
                    processed_count % batch_pause_every == 0
                    and processed_count < total_a_stocks
                ):
                    time.sleep(batch_pause_seconds)

        mainboard_wall_seconds = time.perf_counter() - loop_start_ts
        print(
            f"✅ 沪深市场拉取完成: 处理 {processed_count} 只, 成功 {success_count} 只, "
            f"空数据 {empty_count} 只, 失败 {failed_count} 只, 总耗时 {mainboard_wall_seconds / 60:.1f} 分钟"
        )

        if late_listing_count > 0:
            print(
                f"ℹ️ 有 {late_listing_count} 只股票首个交易日晚于起始日期 {start_date}，"
                "这通常是上市较晚，不属于接口报错。"
            )
            if late_listing_examples:
                late_sample = "，".join(
                    [f"{code}({name})首日={first_date}" for code, name, first_date in late_listing_examples[:5]]
                )
                print(f"   上市较晚样本: {late_sample}")
        else:
            print(f"ℹ️ 未发现首个交易日晚于 {start_date} 的主板样本。")

        if empty_count > 0 and empty_stock_examples:
            empty_sample = "，".join([f"{code}({name})" for code, name in empty_stock_examples[:5]])
            print(f"ℹ️ 空数据样本: {empty_sample}")

        
        if include_bjs_update:
            # 获取所有股票列表（获取北交所股票数据）
            print('正在获取股票列表信息...')
            stock_info = safe_network_request(
                ak.stock_info_a_code_name,
                max_retries=3,
                timeout_seconds=60,
                disable_proxy=True
            )

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
                        df = safe_network_request(
                            ak.stock_zh_a_hist,
                            symbol=code,
                            period='daily',
                            start_date=datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y%m%d'),
                            end_date=datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y%m%d'),
                            adjust="qfq",
                            max_retries=2,
                            timeout_seconds=30,
                            disable_proxy=True
                        )
                        if df is not None and not df.empty:
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
                    print("继续执行主板合并与保存流程...")
            else:
                print('跳过北交所数据处理（股票列表获取失败）')
        else:
            print('已配置为跳过北交所数据更新')

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
            if 'open' in new_data_pl.columns: select_exprs.append(pl.col("open").cast(pl.Float64, strict=False).alias("开盘"))
            if 'close' in new_data_pl.columns: select_exprs.append(pl.col("close").cast(pl.Float64, strict=False).alias("收盘"))
            if 'high' in new_data_pl.columns: select_exprs.append(pl.col("high").cast(pl.Float64, strict=False).alias("最高"))
            if 'low' in new_data_pl.columns: select_exprs.append(pl.col("low").cast(pl.Float64, strict=False).alias("最低"))
            if 'volume' in new_data_pl.columns: select_exprs.append(pl.col("volume").cast(pl.Int64, strict=False).alias("成交量"))
            if 'amount' in new_data_pl.columns: select_exprs.append(pl.col("amount").cast(pl.Float64, strict=False).alias("成交额"))
            if 'turn' in new_data_pl.columns: select_exprs.append(pl.col("turn").cast(pl.Float64, strict=False).alias("换手率"))
            if 'pctChg' in new_data_pl.columns: select_exprs.append(pl.col("pctChg").cast(pl.Float64, strict=False).alias("涨跌幅"))
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

