#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
板块数据管理器
SectorDataManager，专门负责板块数据管理
包含板块成分股管理功能，支持同花顺数据源、东财数据源
"""
import polars as pl
import pandas as pd
import akshare as ak
import requests
import json
import time
import random
import re
import io
import contextlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, time as dt_time, date
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from .market_data_manager import MarketMetadataManager
import warnings
# 屏蔽pandas警告
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)


def _coalesce_to_date_expr(col_name: str = '日期') -> pl.Expr:
    """宽松解析常见日期/时间文本为 Date，兼容不同数据源格式。"""
    text_col = pl.col(col_name).cast(pl.Utf8)
    return pl.coalesce([
        pl.col(col_name).cast(pl.Date, strict=False),
        text_col.str.strptime(pl.Date, format='%Y-%m-%d', strict=False),
        text_col.str.strptime(pl.Date, format='%Y/%m/%d', strict=False),
        text_col.str.strptime(pl.Date, format='%Y%m%d', strict=False),
        text_col.str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S', strict=False).dt.date(),
        text_col.str.strptime(pl.Datetime, format='%Y/%m/%d %H:%M:%S', strict=False).dt.date(),
        text_col.str.strptime(pl.Datetime, format='%Y-%m-%dT%H:%M:%S', strict=False).dt.date(),
        text_col.str.strptime(pl.Datetime, format='%Y-%m-%dT%H:%M:%S%.f', strict=False).dt.date(),
    ])

# 检查问财库是否可用
try:
    import pywencai
    PYWENCAI_AVAILABLE = True
except ImportError:
    PYWENCAI_AVAILABLE = False
    print("⚠️ pywencai未安装，成分股功能将受限")

# 导入数据处理器
from .data_processor import DataProcessor
class ThsDataProvider:
    """同花顺数据提供器 - 专门处理同花顺数据源"""
    def __init__(self):
        self.base_url = "https://d.10jqka.com.cn"
        self.headers = {
            'Referer': 'http://q.10jqka.com.cn/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept': 'application/javascript, */*;q=0.1',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        
        # 错误日志配置
        self.data_dir = Path("data_cache")
        self.sector_dir = self.data_dir / "sectors"
        self.sector_dir.mkdir(parents=True, exist_ok=True)
        self.error_log_file = self.sector_dir / "ths_update_errors.log"

    def test_connection(self) -> bool:
        """测试同花顺连接"""
        if not PYWENCAI_AVAILABLE:
            print("❌ pywencai未安装，无法测试同花顺连接")
            return False
        try:
            # 测试问财库
            test_result = pywencai.get(query="上证指数", query_type="zhishu")
            if len(test_result) > 0:
                print("✅ 同花顺问财库连接成功")
                return True
            else:
                print("❌ 同花顺问财库返回空数据")
                return False
        except Exception as e:
            print(f"❌ 同花顺连接失败: {e}")
            return False

    def get_sector_data_with_retry(self, sector_name: str, sector_type: str, 
                                   start_date: str, end_date: str,
                                   max_retries: int = 3,
                                   verbose: bool = True) -> Optional[pd.DataFrame]:
        """获取单个板块历史数据（带重试机制）
        
        Args:
            sector_name: 板块名称
            sector_type: 板块类型（"行业" 或 "概念"）
            start_date: 开始日期
            end_date: 结束日期
            max_retries: 最大重试次数
            
        Returns:
            板块历史数据DataFrame，失败时返回None
        """
        for attempt in range(max_retries):
            try:
                if verbose:
                    print(f"📊 获取{sector_type}板块数据: {sector_name} (尝试 {attempt + 1}/{max_retries})")
                
                if verbose:
                    if sector_type == "行业":
                        data = ak.stock_board_industry_index_ths(
                            symbol=sector_name,
                            start_date=start_date,
                            end_date=end_date
                        )
                    else:  # 概念
                        data = ak.stock_board_concept_index_ths(
                            symbol=sector_name,
                            start_date=start_date,
                            end_date=end_date
                        )
                else:
                    # 静默模式：抑制下游库进度条/日志输出（如tqdm）
                    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                        if sector_type == "行业":
                            data = ak.stock_board_industry_index_ths(
                                symbol=sector_name,
                                start_date=start_date,
                                end_date=end_date
                            )
                        else:  # 概念
                            data = ak.stock_board_concept_index_ths(
                                symbol=sector_name,
                                start_date=start_date,
                                end_date=end_date
                            )
                
                if data is not None and not data.empty:
                    # 数据质量检查
                    required_columns = ['日期', '开盘价', '收盘价', '最高价', '最低价']
                    missing_columns = [col for col in required_columns if col not in data.columns]
                    
                    if missing_columns:
                        if verbose:
                            print(f"⚠️ 数据缺少必要列: {missing_columns}")
                        continue
                    
                    # 标准化列名
                    data = data.rename(columns={
                        '开盘价': '开盘',
                        '最高价': '最高',
                        '最低价': '最低',
                        '收盘价': '收盘'
                    })
                    
                    # 添加板块信息
                    data['板块名称'] = sector_name
                    data['板块类型'] = sector_type
                    data['数据源'] = '同花顺'
                    
                    # 数据验证
                    data_years = pd.to_datetime(data['日期']).dt.year.unique()
                    coverage_years = len(data_years)
                    
                    if verbose:
                        print(f"✅ 成功获取 {len(data)} 条记录，覆盖 {coverage_years} 年数据")
                    return data
                else:
                    if verbose:
                        print(f"⚠️ 获取数据失败或为空 (尝试 {attempt + 1})")
                    
            except Exception as e:
                error_msg = f"获取{sector_name}数据失败 (尝试 {attempt + 1}): {e}"
                if verbose:
                    print(f"❌ {error_msg}")
                
                # 内联错误日志记录
                try:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    with open(self.error_log_file, 'a', encoding='utf-8') as f:
                        f.write(f"[{timestamp}] {error_msg}\n")
                except Exception as log_e:
                    if verbose:
                        print(f"⚠️ 记录错误日志失败: {log_e}")
                
                if attempt < max_retries - 1:
                    # 指数退避延迟
                    delay = (attempt + 1) * 2 + random.uniform(1, 3)
                    if verbose:
                        print(f"⏳ 等待 {delay:.1f} 秒后重试...")
                    time.sleep(delay)
        
        if verbose:
            print(f"❌ 多次尝试后仍无法获取 {sector_name} 的数据")
        return None
    def get_sector_constituents(self, code: str, name: str, sector_type: str = "概念", max_retries: int = 3) -> Optional[pd.DataFrame]:
        """使用问财库获取同花顺板块成分股数据（带重试机制）"""
        
        for attempt in range(max_retries):
            try:
                if attempt == 0:
                    print(f"📊 使用问财库获取{sector_type}板块成分股: {name}")
                else:
                    print(f"📊 重试获取{sector_type}板块成分股: {name} (尝试 {attempt + 1}/{max_retries})")
                # 构建问财查询语句
                if sector_type == "概念":
                    if attempt > 1:
                        query = f"{name}"
                    else:
                        query = f"所属概念包含{name}"
                else:
                    query = f"所属同花顺行业包含{name}"
                # 使用问财库查询
                stocks_df = pywencai.get(
                    query=query,
                    query_type="stock",
                    loop=True
                )
                if stocks_df is not None and not stocks_df.empty:
                    # 优化的向量化数据处理
                    current_date = datetime.now().strftime('%Y-%m-%d')
                    
                    # 预定义可能的列名映射（问财返回的列名可能不同）
                    code_columns = ['股票代码', 'code', '代码', 'symbol']
                    name_columns = ['股票简称', 'name', '名称', '股票名称']
                    
                    # 找到实际存在的列名
                    code_col = next((col for col in code_columns if col in stocks_df.columns), None)
                    name_col = next((col for col in name_columns if col in stocks_df.columns), None)
                    
                    if not code_col or not name_col:
                        print(f"    ❌ 未找到必要的列: 代码列={code_col}, 名称列={name_col}")
                        print(f"    🔍 可用列名: {list(stocks_df.columns)}")
                        return None
                    
                    # 过滤有效数据
                    valid_mask = stocks_df[code_col].notna() & stocks_df[name_col].notna()
                    valid_df = stocks_df[valid_mask].copy()
                    
                    if valid_df.empty:
                        print(f"    ⚠️ 没有有效的股票数据")
                        return None
                    
                    # 向量化处理股票代码格式：提取前6位数字（处理002569.sz格式）
                    valid_df['股票代码'] = valid_df[code_col].astype(str).str.extract(r'(\d{6})')[0]
                    valid_df['股票名称'] = valid_df[name_col].astype(str)
                    
                    # 过滤掉无法解析的股票代码
                    code_valid_mask = valid_df['股票代码'].notna() & (valid_df['股票代码'].str.len() == 6)
                    valid_df = valid_df[code_valid_mask]
                    
                    if valid_df.empty:
                        print(f"    ⚠️ 没有有效格式的股票代码")
                        return None
                    
                    # 向量化添加元数据
                    valid_df['板块名称'] = name
                    valid_df['板块代码'] = code  
                    valid_df['板块类型'] = sector_type
                    valid_df['更新日期'] = current_date
                    valid_df['数据源'] = '同花顺'
                    
                    # 选择需要的列，避免 DataFrame->dict->DataFrame 的往返开销
                    result_columns = ['股票代码', '股票名称', '板块名称', '板块代码', '板块类型', '更新日期', '数据源']
                    result_df = valid_df[result_columns].copy()
                    if not result_df.empty:
                        print(f"✅ 获取到 {len(result_df)} 只{sector_type}成分股")
                        return result_df
                    print(f"⚠️ 未能解析{sector_type}板块 {name} 的成分股数据")
                    if attempt < max_retries - 1:
                        print(f"⏳ 等待重试...")
                        time.sleep(random.uniform(1, 3))
                        continue
                    return pd.DataFrame()
                else:
                    print(f"⚠️ 问财查询 {name} 无结果")
                    if attempt < max_retries - 1:
                        print(f"⏳ 等待重试...")
                        time.sleep(random.uniform(1, 3))
                        continue
                    return pd.DataFrame()
            except Exception as e:
                print(f"❌ 使用问财库获取{sector_type}成分股失败 (尝试 {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    print(f"⏳ 等待 {random.uniform(1, 3):.1f} 秒后重试...")
                    time.sleep(random.uniform(1, 3))
                    continue
                else:
                    return None
        
        # 如果所有重试都失败
        print(f"❌ 多次尝试后仍无法获取 {name} 的成分股数据")
        return None
    def update_sector_data(
        self,
        sector_dir: Path,
        years_back: int = 1,
        force_update: bool = False,
        start_date: str = None,
        end_date: str = None,
        date_range_by_board: Optional[Dict[Tuple[str, str], Tuple[str, str]]] = None,
    ) -> bool:
        """全面更新同花顺板块数据"""
        try:
            print(f"🚀 开始全面更新同花顺板块数据 (最近{years_back}年)")
            # 检查是否需要更新
            from pathlib import Path
            sector_path = Path(sector_dir)
            sectors_ths_file = sector_path / "sectors_ths.parquet"
            # 计算日期范围（允许上层显式传入 start/end）
            if start_date is None:
                safe_years_back = max(1, int(years_back))
                start_date = (datetime.now() - pd.Timedelta(days=safe_years_back * 365)).strftime('%Y%m%d')
            if end_date is None:
                end_date = (datetime.now() + pd.Timedelta(days=1)).strftime('%Y%m%d')
            print(f"📅 更新日期范围: {start_date} 到 {end_date}")
            # 更新行业数据
            print("\n📊 更新同花顺行业板块数据...")
            industry_success = self._update_board_data(
                start_date=start_date,
                end_date=end_date,
                sector_dir=sector_path,
                board_type="行业",
                list_fetcher=ak.stock_board_industry_name_ths,
                temp_file_name="temp_industry_ths.parquet",
                date_range_by_board=date_range_by_board,
            )
            # 更新概念数据
            print("\n📊 更新同花顺概念板块数据...")
            concept_success = self._update_board_data(
                start_date=start_date,
                end_date=end_date,
                sector_dir=sector_path,
                board_type="概念",
                list_fetcher=ak.stock_board_concept_name_ths,
                temp_file_name="temp_concept_ths.parquet",
                date_range_by_board=date_range_by_board,
            )
            # 合并数据
            if industry_success or concept_success:
                print("\n🔗 合并同花顺板块数据...")
                try:
                    # 收集所有新数据
                    all_new_data = []
                    # 读取新的行业数据
                    industry_temp_file = sector_path / "temp_industry_ths.parquet"
                    if industry_success and industry_temp_file.exists():
                        industry_data = pl.read_parquet(industry_temp_file)
                        all_new_data.append(industry_data)
                        print(f"📊 新行业数据: {industry_data.height} 条记录")
                        # 删除临时文件
                        industry_temp_file.unlink()
                    # 读取新的概念数据
                    concept_temp_file = sector_path / "temp_concept_ths.parquet"
                    if concept_success and concept_temp_file.exists():
                        concept_data = pl.read_parquet(concept_temp_file)
                        all_new_data.append(concept_data)
                        print(f"📊 新概念数据: {concept_data.height} 条记录")
                        # 删除临时文件
                        concept_temp_file.unlink()
                    if all_new_data:
                        # 合并所有新数据
                        new_combined_data = pl.concat(all_new_data)
                        print(f"📊 新数据总计: {new_combined_data.height} 条记录")
                        # 保存原始新数据到临时文件，等待SectorDataManager处理
                        temp_new_data_file = sector_path / "temp_new_data_ths.parquet"
                        new_combined_data.write_parquet(temp_new_data_file)
                        print(f"📊 新数据已保存到临时文件: {temp_new_data_file}")
                        # 返回True，让SectorDataManager继续处理
                        return True
                    else:
                        print("⚠️ 没有新数据需要合并")
                        return False
                except Exception as e:
                    print(f"❌ 合并数据失败: {e}")
                    import traceback
                    traceback.print_exc()
                    return False
            else:
                print("❌ 行业和概念数据更新都失败")
                return False
        except Exception as e:
            print(f"\n❌ 更新同花顺板块数据失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    def _update_board_data(
        self,
        start_date: str,
        end_date: str,
        sector_dir: Path,
        board_type: str,
        list_fetcher,
        temp_file_name: str,
        date_range_by_board: Optional[Dict[Tuple[str, str], Tuple[str, str]]] = None,
    ) -> bool:
        """通用的同花顺板块历史数据更新流程。"""
        print(f"\n📊 更新同花顺{board_type}板块数据...")
        try:
            print(f"📊 获取同花顺{board_type}指数列表...")
            boards_df = list_fetcher()
            if boards_df is None or boards_df.empty:
                print(f"❌ 获取{board_type}列表失败")
                return False
            print(f"✅ 获取到 {len(boards_df)} 个同花顺{board_type}指数")
            print(f"📋 获取到 {len(boards_df)} 个{board_type}板块")
            print(f"🚀 全量模式：处理所有 {len(boards_df)} 个{board_type}")
            boards_to_process = boards_df
            all_board_data: List[pd.DataFrame] = []
            success_count = 0
            total = len(boards_to_process)
            for idx, row in enumerate(boards_to_process.itertuples(index=False), start=1):
                symbol = getattr(row, "code", None)
                name = getattr(row, "name", None)
                if not symbol or not name:
                    print(f"    ❌ 第{idx}行缺少必要列: code/name")
                    continue
                print(f"  📊 处理{board_type}: {name} ({symbol}) [{idx}/{total}]")
                board_start_date = start_date
                board_end_date = end_date
                if date_range_by_board:
                    custom_range = date_range_by_board.get((str(name), board_type))
                    if custom_range:
                        board_start_date, board_end_date = custom_range
                        if board_start_date != start_date or board_end_date != end_date:
                            print(f"    🧩 扩展抓取区间: {board_start_date} - {board_end_date}")
                try:
                    board_data = self.get_sector_data_with_retry(
                        sector_name=name,
                        sector_type=board_type,
                        start_date=board_start_date,
                        end_date=board_end_date,
                    )
                except Exception as e:
                    print(f"    ❌ 处理{board_type} {name}失败: {e}")
                    continue
                if board_data is None or board_data.empty:
                    print("    ⚠️ 未获取到数据")
                    continue
                board_data["板块代码"] = symbol
                all_board_data.append(board_data)
                success_count += 1
                print(f"    ✅ 成功获取 {len(board_data)} 条数据")
            print(f"\n📊 {board_type}数据获取完成: {success_count}/{total} 成功")
            if not all_board_data:
                print(f"❌ 没有获取到任何{board_type}数据")
                return False
            combined_data = pd.concat(all_board_data, ignore_index=True)
            combined_pl = pl.from_pandas(combined_data)
            text_cols = [c for c in ['板块代码', '板块名称', '板块类型', '数据源'] if c in combined_pl.columns]
            if text_cols:
                combined_pl = combined_pl.with_columns([
                    pl.col(c).cast(pl.Utf8).fill_null("").alias(c) for c in text_cols
                ])
            temp_file = sector_dir / temp_file_name
            combined_pl.write_parquet(temp_file)
            print(f"✅ {board_type}板块原始数据保存到临时文件: {combined_pl.height} 条记录")
            return True
        except Exception as e:
            print(f"❌ 更新{board_type}板块数据失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    @staticmethod
    def _save_constituents_excel(all_constituents: List[pd.DataFrame], output_file: Path, source_name: str) -> bool:
        """统一保存成分股数据到Excel。"""
        if not all_constituents:
            print("❌ 没有获取到任何成分股数据")
            return False
        try:
            combined_data = pd.concat(all_constituents, ignore_index=True)
            combined_data['股票代码'] = combined_data['股票代码'].astype(str).str.zfill(6)
            combined_data = combined_data.drop_duplicates(subset=['板块名称', '股票代码'], keep='first')
            combined_data['数据源'] = source_name
            combined_data['更新日期'] = datetime.now().strftime('%Y-%m-%d')
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                combined_data.to_excel(writer, sheet_name='所有数据', index=False)
                for sector_type in combined_data['板块类型'].unique():
                    type_data = combined_data[combined_data['板块类型'] == sector_type]
                    type_data.to_excel(writer, sheet_name=f"{sector_type}板块", index=False)
            print(f"✅ {source_name}成分股数据保存成功: {len(combined_data)} 条记录")
            print(f"📁 保存位置: {output_file}")
            return True
        except Exception as e:
            print(f"❌ 保存{source_name}成分股数据失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    def update_sector_constituents(self, sector_dir: Path) -> bool:
        """更新同花顺所有成分股数据"""
        try:
            print("🚀 开始更新同花顺成分股数据...")
            all_constituents: List[pd.DataFrame] = []

            print("📊 获取同花顺概念指数列表...")
            concepts_df = ak.stock_board_concept_name_ths()
            concept_success = 0
            if concepts_df is not None and not concepts_df.empty:
                total = len(concepts_df)
                for idx, row in enumerate(concepts_df.itertuples(index=False), start=1):
                    code = getattr(row, "code", "")
                    name = getattr(row, "name", "")
                    if not code or not name:
                        continue
                    print(f"  [{idx}/{total}] {name}({code})")
                    stocks_df = self.get_sector_constituents(code, name, "概念")
                    if stocks_df is not None and not stocks_df.empty:
                        all_constituents.append(stocks_df)
                        concept_success += 1
                        print(f"    ✅ {len(stocks_df)} 只股票")
                    else:
                        print("    ❌ 获取失败")
                    if idx % 10 == 0:
                        print("  💤 已处理 {idx} 个概念板块，休息0.5秒...".format(idx=idx))
                        time.sleep(0.5)

            print("📊 获取同花顺行业指数列表...")
            industries_df = ak.stock_board_industry_name_ths()
            industry_success = 0
            if industries_df is not None and not industries_df.empty:
                total = len(industries_df)
                for idx, row in enumerate(industries_df.itertuples(index=False), start=1):
                    code = getattr(row, "code", "")
                    name = getattr(row, "name", "")
                    if not code or not name:
                        continue
                    print(f"  [{idx}/{total}] {name}({code})")
                    stocks_df = self.get_sector_constituents(code, name, "行业")
                    if stocks_df is not None and not stocks_df.empty:
                        all_constituents.append(stocks_df)
                        industry_success += 1
                        print(f"    ✅ {len(stocks_df)} 只股票")
                    else:
                        print("    ❌ 获取失败")
                    if idx % 10 == 0:
                        print("  💤 已处理 {idx} 个行业板块，休息0.5秒...".format(idx=idx))
                        time.sleep(0.5)

            print(f"✅ 同花顺成分股更新完成: 概念{concept_success}, 行业{industry_success}")
            print("\n💾 保存同花顺成分股数据...")
            output_file = sector_dir / "同花顺板块成分股.xlsx"
            return self._save_constituents_excel(all_constituents, output_file, "同花顺")
        except Exception as e:
            print(f"❌ 同花顺成分股更新失败: {e}")
            import traceback
            traceback.print_exc()
            return False

class EastmoneyDataProvider:
    """东方财富数据提供器 - 专门处理东财数据源"""
    def test_connection(self) -> bool:
        """测试东财连接"""
        try:
            # 测试获取概念板块名称
            test_data = ak.stock_board_concept_name_em()
            if test_data is not None and not test_data.empty:
                print("✅ 东方财富连接成功")
                return True
            else:
                print("❌ 东方财富返回空数据")
                return False
        except Exception as e:
            print(f"❌ 东方财富连接失败: {e}")
            return False
    def get_concept_hist_data(self, concept_name: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取东财概念板块历史数据
        Args:
            concept_name: 概念板块名称（如"绿色电力"），不是代码
            start_date: 开始日期
            end_date: 结束日期
        """
        try:
            print(f"📊 获取东财概念板块数据: {concept_name}")
            data = ak.stock_board_concept_hist_em(
                symbol=concept_name,  # 注意：这里需要传递概念名称，不是代码
                start_date=start_date,
                end_date=end_date,
                period="daily",
                adjust=""  # 不复权，保持与其他数据源一致
            )
            if data is not None and not data.empty:
                # 添加板块信息
                data['板块名称'] = concept_name
                data['板块类型'] = '概念'
                data['数据源'] = '东方财富'
                print(f"✅ 获取到 {len(data)} 条概念板块数据")
                return data
            else:
                print(f"⚠️ 概念板块 {concept_name} 无数据")
                return None
        except Exception as e:
            print(f"❌ 获取概念板块数据失败: {e}")
            return None
    def get_industry_hist_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取东财行业板块历史数据"""
        try:
            print(f"📊 获取东财行业板块数据: {symbol}")
            data = ak.stock_board_industry_hist_em(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                period="日k",
                adjust="qfq"  # 前复权
            )
            if data is not None and not data.empty:
                # 添加板块信息
                data['板块名称'] = symbol
                data['板块类型'] = '行业'
                data['数据源'] = '东方财富'
                print(f"✅ 获取到 {len(data)} 条行业板块数据")
                return data
            else:
                print(f"⚠️ 行业板块 {symbol} 无数据")
                return None
        except Exception as e:
            print(f"❌ 获取行业板块数据失败: {e}")
            return None
    def _get_board_constituents(self, symbol: str, sector_type: str, fetcher) -> Optional[pd.DataFrame]:
        """东财成分股通用抓取逻辑。"""
        try:
            print(f"📊 获取东财{sector_type}板块成分股: {symbol}")
            data = fetcher(symbol=symbol)
            if data is None or data.empty:
                print(f"⚠️ {sector_type}板块 {symbol} 无成分股数据")
                return None
            if '代码' not in data.columns or '名称' not in data.columns:
                print(f"⚠️ {sector_type}板块 {symbol} 成分股数据格式异常")
                return None
            result_df = pd.DataFrame({
                '股票代码': data['代码'].astype(str).str.zfill(6),
                '股票名称': data['名称'].astype(str),
                '板块名称': symbol,
                '板块代码': '',
                '板块类型': sector_type,
                '更新日期': datetime.now().date(),
                '数据源': '东方财富'
            })
            print(f"✅ 获取到 {len(result_df)} 只{sector_type}成分股")
            return result_df
        except Exception as e:
            print(f"❌ 获取{sector_type}板块成分股失败: {e}")
            return None
    def update_sector_data(
        self,
        start_date: str,
        end_date: str,
        sector_dir: Path,
        date_range_by_board: Optional[Dict[Tuple[str, str], Tuple[str, str]]] = None,
    ) -> bool:
        """使用东财数据源更新板块数据"""
        try:
            all_data = []
            # 获取概念板块
            print("📊 获取东财概念板块名称...")
            try:
                concepts_df = ak.stock_board_concept_name_em()
                if concepts_df is not None and not concepts_df.empty:
                    print(f"✅ 获取到 {len(concepts_df)} 个东财概念板块")
                    print(f"📋 获取到 {len(concepts_df)} 个概念板块")
                else:
                    print("⚠️ 东财概念板块数据为空")
                    concepts_df = None
            except Exception as e:
                print(f"❌ 获取东财概念板块名称失败: {e}")
                concepts_df = None
            if concepts_df is not None:
                concept_iter_df = concepts_df.rename(columns={'板块代码': 'board_code', '板块名称': 'board_name'})
                for row in concept_iter_df.itertuples(index=False):
                    try:
                        symbol = getattr(row, 'board_code', None)
                        name = getattr(row, 'board_name', None)
                        if not symbol or not name:
                            continue
                        fetch_start = start_date
                        fetch_end = end_date
                        if date_range_by_board:
                            custom_range = date_range_by_board.get((str(name), "概念"))
                            if custom_range:
                                fetch_start, fetch_end = custom_range
                                if fetch_start != start_date or fetch_end != end_date:
                                    print(f"    🧩 概念扩展抓取区间: {fetch_start} - {fetch_end}")
                        # 东财概念板块API需要传递板块名称，不是代码
                        concept_data = self.get_concept_hist_data(name, fetch_start, fetch_end)
                        if concept_data is not None:
                            concept_data['板块名称'] = name
                            concept_data['板块代码'] = symbol
                            concept_data['板块类型'] = '概念'
                            concept_data['数据源'] = '东方财富'
                            all_data.append(concept_data)
                    except Exception as e:
                        print(f"处理概念板块 {name} 失败: {e}")
                        continue
            # 获取行业板块
            print("📊 获取东财行业板块名称...")
            try:
                industries_df = ak.stock_board_industry_name_em()
                if industries_df is not None and not industries_df.empty:
                    print(f"✅ 获取到 {len(industries_df)} 个东财行业板块")
                    print(f"📋 获取到 {len(industries_df)} 个行业板块")
                else:
                    print("⚠️ 东财行业板块数据为空")
                    industries_df = None
            except Exception as e:
                print(f"❌ 获取东财行业板块名称失败: {e}")
                industries_df = None
            if industries_df is not None:
                industry_iter_df = industries_df.rename(columns={'板块代码': 'board_code', '板块名称': 'board_name'})
                for row in industry_iter_df.itertuples(index=False):
                    try:
                        symbol = getattr(row, 'board_code', None)
                        name = getattr(row, 'board_name', None)
                        if not symbol or not name:
                            continue
                        fetch_start = start_date
                        fetch_end = end_date
                        if date_range_by_board:
                            custom_range = date_range_by_board.get((str(name), "行业"))
                            if custom_range:
                                fetch_start, fetch_end = custom_range
                                if fetch_start != start_date or fetch_end != end_date:
                                    print(f"    🧩 行业扩展抓取区间: {fetch_start} - {fetch_end}")
                        industry_data = self.get_industry_hist_data(symbol, fetch_start, fetch_end)
                        if industry_data is not None:
                            industry_data['板块名称'] = name
                            industry_data['板块代码'] = symbol
                            industry_data['板块类型'] = '行业'
                            industry_data['数据源'] = '东方财富'
                            all_data.append(industry_data)
                    except Exception as e:
                        print(f"处理行业板块 {name} 失败: {e}")
                        continue
            # 保存数据
            if all_data:
                # 简单保存
                combined_data = pd.concat(all_data, ignore_index=True)
                combined_pl = pl.from_pandas(combined_data)
                output_file = sector_dir / "sectors_dc.parquet"
                combined_pl.write_parquet(output_file)
                print(f"✅ 东财板块数据保存成功: {len(combined_data)} 条记录")
                return True
            else:
                print("❌ 没有获取到任何东财数据")
                return False
        except Exception as e:
            print(f"❌ 东财数据更新失败: {e}")
            return False
    def _collect_constituents_by_name(self, boards_df: pd.DataFrame, sector_type: str, fetch_fn) -> Tuple[List[pd.DataFrame], int]:
        """按板块名称批量抓取成分股。"""
        collected: List[pd.DataFrame] = []
        success_count = 0
        if boards_df is None or boards_df.empty:
            return collected, success_count
        if "板块名称" not in boards_df.columns:
            return collected, success_count
        iter_df = boards_df.rename(columns={"板块名称": "board_name"})
        total = len(iter_df)
        for idx, row in enumerate(iter_df.itertuples(index=False), start=1):
            name = getattr(row, "board_name", "")
            if not name:
                continue
            print(f"  [{idx}/{total}] {name}")
            stocks_df = fetch_fn(name)
            if stocks_df is not None and not stocks_df.empty:
                collected.append(stocks_df)
                success_count += 1
                print(f"    ✅ {len(stocks_df)} 只股票")
            else:
                print("    ❌ 获取失败")
            if idx % 10 == 0:
                print(f"  💤 已处理 {idx} 个东财{sector_type}板块，休息0.5秒...")
                time.sleep(0.5)
        return collected, success_count
    @staticmethod
    def _save_constituents_excel(all_constituents: List[pd.DataFrame], output_file: Path, source_name: str) -> bool:
        """统一保存成分股数据到Excel。"""
        if not all_constituents:
            print("❌ 没有获取到任何成分股数据")
            return False
        try:
            combined_data = pd.concat(all_constituents, ignore_index=True)
            combined_data['股票代码'] = combined_data['股票代码'].astype(str).str.zfill(6)
            combined_data = combined_data.drop_duplicates(subset=['板块名称', '股票代码'], keep='first')
            combined_data['数据源'] = source_name
            combined_data['更新日期'] = datetime.now().strftime('%Y-%m-%d')
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                combined_data.to_excel(writer, sheet_name='所有数据', index=False)
                for sector_type in combined_data['板块类型'].unique():
                    type_data = combined_data[combined_data['板块类型'] == sector_type]
                    type_data.to_excel(writer, sheet_name=f"{sector_type}板块", index=False)
            print(f"✅ {source_name}成分股数据保存成功: {len(combined_data)} 条记录")
            print(f"📁 保存位置: {output_file}")
            return True
        except Exception as e:
            print(f"❌ 保存{source_name}成分股数据失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    def update_all_constituents(self, sector_dir: Path) -> bool:
        """更新东财所有成分股数据"""
        try:
            print("🚀 开始更新东财成分股数据...")
            print("📊 获取东财概念板块名称...")
            concepts_df = ak.stock_board_concept_name_em()
            concept_constituents, concept_success = self._collect_constituents_by_name(
                concepts_df,
                "概念",
                lambda board_name: self._get_board_constituents(
                    symbol=board_name,
                    sector_type='概念',
                    fetcher=ak.stock_board_concept_cons_em
                )
            )
            print("📊 获取东财行业板块名称...")
            industries_df = ak.stock_board_industry_name_em()
            industry_constituents, industry_success = self._collect_constituents_by_name(
                industries_df,
                "行业",
                lambda board_name: self._get_board_constituents(
                    symbol=board_name,
                    sector_type='行业',
                    fetcher=ak.stock_board_industry_cons_em
                )
            )
            all_constituents = concept_constituents + industry_constituents
            print(f"✅ 东财成分股更新完成: 概念{concept_success}, 行业{industry_success}")
            print("\n💾 保存东财成分股数据...")
            output_file = sector_dir / "东财板块成分股.xlsx"
            return self._save_constituents_excel(all_constituents, output_file, "东方财富")
        except Exception as e:
            print(f"❌ 东财成分股更新失败: {e}")
            import traceback
            traceback.print_exc()
            return False

class SectorDataManager:
    """板块数据管理器 - 专注于加载本地数据、协调更新、提供数据接口"""
    def __init__(self, data_dir: str = "data_cache", preferred_source: str = "ths"):
        self.data_dir = Path(data_dir)
        self.sector_dir = self.data_dir / "sectors"
        self.sector_dir.mkdir(parents=True, exist_ok=True)
        # 文件路径配置
        self.ths_file = self.sector_dir / "sectors_ths.parquet"
        self.dc_file = self.sector_dir / "sectors_dc.parquet"
        self.ths_constituents_file = self.sector_dir / "同花顺板块成分股.xlsx"
        self.dc_constituents_file = self.sector_dir / "东财板块成分股.xlsx"
        # 数据提供器
        self.ths_provider = ThsDataProvider()
        self.eastmoney_provider = EastmoneyDataProvider()
        self.preferred_source = preferred_source
        
        # 成分股数据缓存 - 性能优化
        self._constituents_cache = {}
        self._cache_timestamps = {}
        self._cache_expire_seconds = 300  # 缓存5分钟
        
        # 索引缓存 - 为频繁查询的列建立索引
        self._index_cache = {}
    def load_sector_data(self, source: str = None, days_back: int = None, include_sectors: bool = True, include_concepts: bool = True, target_date: str = None) -> pl.DataFrame:
        """
        加载板块数据
        Args:
            source: 数据源 ("ths" 或 "eastmoney")，默认使用preferred_source
            days_back: 加载最近多少天的数据
            include_sectors: 是否包含行业板块
            include_concepts: 是否包含概念板块
            target_date: 目标日期（可选），如果指定则从该日期开始往前计算
        Returns:
            pl.DataFrame: 板块数据
        """
        if source is None:
            source = self.preferred_source
        try:
            # 根据指定的数据源加载数据
            if source == "ths" and self.ths_file.exists():
                print(f"📊 加载同花顺板块数据: {self.ths_file}")
                df = pl.read_parquet(self.ths_file)
            elif source == "eastmoney" and self.dc_file.exists():
                print(f"📊 加载东财板块数据: {self.dc_file}")
                df = pl.read_parquet(self.dc_file)
            else:
                # 尝试加载任何可用的数据
                if self.ths_file.exists():
                    print(f"📊 加载同花顺板块数据: {self.ths_file}")
                    df = pl.read_parquet(self.ths_file)
                elif self.dc_file.exists():
                    print(f"📊 加载东财板块数据: {self.dc_file}")
                    df = pl.read_parquet(self.dc_file)
                else:
                    print("⚠️ 没有找到板块数据文件")
                    return pl.DataFrame()
            # 按板块类型筛选
            if not include_sectors and not include_concepts:
                return pl.DataFrame()
            elif include_sectors and not include_concepts:
                df = df.filter(pl.col('板块类型') == "行业")
            elif not include_sectors and include_concepts:
                df = df.filter(pl.col('板块类型') == "概念")
            # 如果两者都为True，则不筛选
            # 确保日期列为Date类型
            if '日期' in df.columns and not df.is_empty():
                if df['日期'].dtype == pl.Utf8:
                    df = df.with_columns([
                        pl.col('日期').str.strptime(pl.Date, format='%Y-%m-%d', strict=False).alias('日期')
                    ])
                elif df['日期'].dtype.base_type() == pl.Datetime:
                    df = df.with_columns([
                        pl.col('日期').dt.date().alias('日期')
                    ])
            # 如果指定了天数，则筛选最近的数据
            if days_back is not None and not df.is_empty() and '日期' in df.columns:
                # 如果指定了target_date，则从该日期开始往前计算
                if target_date:
                    try:
                        if isinstance(target_date, str):
                            end_date = datetime.strptime(target_date, '%Y-%m-%d').date()
                        else:
                            end_date = target_date
                        cutoff_date = end_date - timedelta(days=days_back)
                        df = df.filter((pl.col('日期') >= cutoff_date) & (pl.col('日期') <= end_date))
                        print(f"📅 使用指定日期范围: {cutoff_date} 至 {end_date}")
                    except Exception as e:
                        print(f"⚠️ 解析target_date失败，使用当前日期: {e}")
                        cutoff_date = datetime.now().date() - timedelta(days=days_back)
                        df = df.filter(pl.col('日期') >= cutoff_date)
                else:
                    cutoff_date = datetime.now().date() - timedelta(days=days_back)
                    df = df.filter(pl.col('日期') >= cutoff_date)
            return df.sort(['日期', '板块类型', '板块名称']) if not df.is_empty() else df
        except Exception as e:
            print(f"❌ 加载板块数据失败: {e}")
            return pl.DataFrame()
    def get_sector_kline_data(self, sector_name: str, days_back: int = 30, target_date: str = None) -> pl.DataFrame:
        """
        获取单个板块的K线数据，用于前端原生ECharts渲染
        
        Args:
            sector_name: 板块名称
            days_back: 获取最近多少天的数据
            target_date: 目标日期（可选），如果指定则从该日期开始往前计算
            
        Returns:
            pl.DataFrame: 包含日期、开盘、收盘、最高、最低、成交量、成交额等列的数据
        """
        try:
            print(f"🔍 获取板块K线数据: {sector_name}, 天数: {days_back}, 目标日期: {target_date}")
            
            # 加载所有板块数据，传递target_date参数
            all_data = self.load_sector_data(days_back=days_back, target_date=target_date)
            
            if all_data.is_empty():
                print(f"❌ 未找到板块数据")
                return pl.DataFrame()
            
            print(f"📊 加载的板块数据范围: {all_data['日期'].min()} 至 {all_data['日期'].max()}")
            
            # 筛选指定板块的数据
            sector_data = all_data.filter(pl.col('板块名称') == sector_name)
            
            if sector_data.is_empty():
                print(f"❌ 未找到板块 '{sector_name}' 的数据")
                return pl.DataFrame()
            
            print(f"✅ 找到板块 '{sector_name}' 数据: {sector_data.height} 条记录")
            
            # 按日期排序
            sector_data = sector_data.sort('日期')
            
            # 确保包含必要的列，如果缺少则补充
            # 基础必需列（用于计算与K线展示）
            base_required_columns = ['日期', '开盘', '收盘', '最高', '最低']
            optional_columns = ['成交量', '成交额', '换手率', '总市值']
            # 先校验基础必需列
            base_missing = [col for col in base_required_columns if col not in sector_data.columns]
            if base_missing:
                print(f"❌ 板块数据缺少基础必需列: {base_missing}")
                return pl.DataFrame()
            # 若缺少涨跌幅则按 (收盘-开盘)/开盘*100 计算补齐，避免因缺列而整体失败
            if '涨跌幅' not in sector_data.columns:
                try:
                    sector_data = sector_data.with_columns([
                        pl.when(pl.col('开盘').is_not_null() & (pl.col('开盘') != 0))
                          .then(((pl.col('收盘') - pl.col('开盘')) / pl.col('开盘') * 100))
                          .otherwise(pl.lit(0.0))
                          .cast(pl.Float64)
                          .alias('涨跌幅')
                    ])
                    print("🔧 已自动补齐缺失列: 涨跌幅")
                except Exception as _e:
                    print(f"❌ 计算涨跌幅失败: {_e}")
                    return pl.DataFrame()
            
            # 补充可选列（如果缺少则设为0）
            for col in optional_columns:
                if col not in sector_data.columns:
                    sector_data = sector_data.with_columns([
                        pl.lit(0.0).alias(col)
                    ])
            
            # 选择和重命名列，确保与前端期望的格式一致
            kline_data = sector_data.select([
                pl.col('日期'),
                pl.col('开盘').alias('开盘'),
                pl.col('收盘').alias('收盘'), 
                pl.col('最高').alias('最高'),
                pl.col('最低').alias('最低'),
                pl.col('成交量').alias('成交量'),
                pl.col('成交额').alias('成交额'),
                pl.col('涨跌幅').alias('涨跌幅')
            ])
            
            print(f"✅ 成功获取板块 '{sector_name}' 的K线数据: {kline_data.height} 条记录")
            print(f"📅 数据日期范围: {kline_data['日期'].min()} 至 {kline_data['日期'].max()}")
            
            return kline_data
            
        except Exception as e:
            print(f"❌ 获取板块K线数据失败: {e}")
            import traceback
            traceback.print_exc()
            return pl.DataFrame()
    def get_sector_daily_data(self, date_str: str = None) -> pl.DataFrame:
        """从本地数据获取指定日期的板块数据"""
        try:
            df = self.load_sector_data()
            if df.is_empty():
                print("⚠️ 本地无行业板块数据，请先更新数据")
                return pl.DataFrame()
            if date_str is not None:
                # 转换日期格式
                if len(date_str) == 8:  # YYYYMMDD格式
                    target_date = datetime.strptime(date_str, '%Y%m%d').date()
                else:  # YYYY-MM-DD格式
                    target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                # 筛选指定日期的数据（现在日期列应该是Date类型）
                df = df.filter(pl.col('日期') == target_date)
                if df.is_empty():
                    print(f"⚠️ 未找到 {date_str} 的行业板块数据")
                    return pl.DataFrame()
            else:
                # 如果没有指定日期，返回最新日期的数据
                if '日期' in df.columns:
                    latest_date = df['日期'].max()
                    df = df.filter(pl.col('日期') == latest_date)
            print(f"📊 获取到 {df.height} 条行业板块数据")
            return df
        except Exception as e:
            print(f"❌ 获取板块数据失败: {e}")
            return pl.DataFrame()
    def _fill_missing_sector_data_for_date_range(
        self,
        start_date: str,
        end_date: str,
        sector_items: List[Dict[str, str]],
        source: str
    ) -> Optional[pl.DataFrame]:
        """按日期区间批量回补板块缺失数据"""
        if not sector_items:
            return None
        source_name = "同花顺" if source == "ths" else "东方财富"
        max_workers = min(8, max(1, len(sector_items)))
        def fetch_one(item: Dict[str, str]) -> Optional[pd.DataFrame]:
            sector_name = item.get('板块名称')
            sector_type = item.get('板块类型')
            sector_code = item.get('板块代码')
            if not sector_name or not sector_type:
                return None
            try:
                data = None
                if source == "ths":
                    data = self.ths_provider.get_sector_data_with_retry(
                        sector_name=sector_name,
                        sector_type=sector_type,
                        start_date=start_date,
                        end_date=end_date,
                        max_retries=2,
                        verbose=False
                    )
                elif source == "eastmoney":
                    if sector_type == "概念":
                        data = self.eastmoney_provider.get_concept_hist_data(
                            concept_name=sector_name,
                            start_date=start_date,
                            end_date=end_date
                        )
                    else:
                        em_symbol = sector_code if sector_code else sector_name
                        data = self.eastmoney_provider.get_industry_hist_data(
                            symbol=em_symbol,
                            start_date=start_date,
                            end_date=end_date
                        )
                if data is None or data.empty:
                    return None
                rename_map = {
                    '开盘价': '开盘',
                    '收盘价': '收盘',
                    '最高价': '最高',
                    '最低价': '最低',
                }
                for old_name, new_name in rename_map.items():
                    if old_name in data.columns and new_name not in data.columns:
                        data = data.rename(columns={old_name: new_name})
                data['板块名称'] = sector_name
                data['板块类型'] = sector_type
                data['数据源'] = source_name
                if sector_code:
                    data['板块代码'] = str(sector_code)
                elif '板块代码' not in data.columns:
                    data['板块代码'] = ''
                return data
            except Exception as e:
                print(f"⚠️ 回补板块数据失败: {sector_name}({sector_type}) {start_date}-{end_date}, err={e}")
                return None
        all_filled_data: List[pd.DataFrame] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_one, item) for item in sector_items]
            total_tasks = len(futures)
            completed_tasks = 0
            for future in as_completed(futures):
                completed_tasks += 1
                data = future.result()
                if data is not None and not data.empty:
                    all_filled_data.append(data)
                # 进度显示：当前完成/需要更新的板块数量
                if completed_tasks == 1 or completed_tasks % 10 == 0 or completed_tasks == total_tasks:
                    progress = (completed_tasks / total_tasks) if total_tasks > 0 else 1.0
                    bar_len = 20
                    filled_len = int(progress * bar_len)
                    bar = "#" * filled_len + "-" * (bar_len - filled_len)
                    print(f"    回补进度 [{bar}] {completed_tasks}/{total_tasks}")
        if not all_filled_data:
            return None
        filled_pl = pl.from_pandas(pd.concat(all_filled_data, ignore_index=True))
        if '日期' in filled_pl.columns:
            filled_pl = filled_pl.with_columns([
                pl.coalesce([
                    pl.col('日期').cast(pl.Date, strict=False),
                    pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, format='%Y-%m-%d', strict=False),
                    pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, format='%Y/%m/%d', strict=False),
                    pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, format='%Y%m%d', strict=False),
                ]).alias('日期')
            ])
        text_cols = ['板块名称', '板块类型', '板块代码', '数据源']
        present_text_cols = [col for col in text_cols if col in filled_pl.columns]
        if present_text_cols:
            filled_pl = filled_pl.with_columns([
                pl.col(col).cast(pl.Utf8).fill_null('').alias(col) for col in present_text_cols
            ])
        return filled_pl
    @staticmethod
    def _parse_date_input(date_input) -> Optional[date]:
        if date_input is None:
            return None
        if isinstance(date_input, datetime):
            return date_input.date()
        if isinstance(date_input, date):
            return date_input
        if isinstance(date_input, str):
            text = date_input.strip()
            if not text:
                return None
            for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"):
                try:
                    return datetime.strptime(text, fmt).date()
                except ValueError:
                    continue
        return None
    def _collect_sector_missing_ranges(self, source: str) -> Tuple[Dict[Tuple[str, str], Tuple[date, date]], int]:
        """收集各板块缺失交易日最小闭区间。"""
        metadata_file = self.ths_file if source == "ths" else self.dc_file if source == "eastmoney" else None
        if metadata_file is None or not metadata_file.exists():
            return {}, 0
        try:
            existing_metadata = pl.read_parquet(metadata_file)
            if existing_metadata is None or existing_metadata.is_empty():
                return {}, 0
            required_cols = {'日期', '板块名称', '板块类型'}
            if not required_cols.issubset(set(existing_metadata.columns)):
                return {}, 0
            from utils.trading_calendar import trading_calendar
            existing_metadata = existing_metadata.with_columns([
                pl.coalesce([
                    pl.col('日期').cast(pl.Date, strict=False),
                    pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, format='%Y-%m-%d', strict=False),
                    pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, format='%Y/%m/%d', strict=False),
                    pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, format='%Y%m%d', strict=False),
                ]).alias('日期'),
                pl.col('板块名称').cast(pl.Utf8).fill_null('').alias('板块名称'),
                pl.col('板块类型').cast(pl.Utf8).fill_null('').alias('板块类型'),
            ]).drop_nulls(subset=['日期'])
            key_cols = ['日期', '板块名称', '板块类型']
            existing_keys = existing_metadata.select(key_cols).unique()
            existing_key_set = set(
                (row['日期'], row['板块名称'], row['板块类型'])
                for row in existing_keys.iter_rows(named=True)
            )
            sector_ranges = existing_metadata.group_by(['板块名称', '板块类型']).agg([
                pl.col('日期').min().alias('min_date'),
                pl.col('日期').max().alias('max_date'),
            ])
            missing_ranges: Dict[Tuple[str, str], Tuple[date, date]] = {}
            total_missing = 0
            for row in sector_ranges.iter_rows(named=True):
                sector_name = row['板块名称']
                sector_type = row['板块类型']
                min_date = row['min_date']
                max_date = row['max_date']
                if not sector_name or not sector_type or min_date is None or max_date is None:
                    continue
                missing_dates: List[date] = []
                for trading_day in trading_calendar.get_trading_days_in_range(min_date, max_date):
                    if (trading_day, sector_name, sector_type) not in existing_key_set:
                        missing_dates.append(trading_day)
                if missing_dates:
                    missing_ranges[(sector_name, sector_type)] = (min(missing_dates), max(missing_dates))
                    total_missing += len(missing_dates)
            return missing_ranges, total_missing
        except Exception as e:
            print(f"⚠️ 收集板块缺失区间失败(忽略): {e}")
            return {}, 0
    def _build_sector_fetch_ranges_for_update(
        self,
        source: str,
        start_date: str,
        end_date: str,
    ) -> Dict[Tuple[str, str], Tuple[str, str]]:
        """将增量更新区间与缺口区间合并成板块级抓取窗口。"""
        global_start = self._parse_date_input(start_date)
        global_end = self._parse_date_input(end_date)
        if global_start and global_end and global_start > global_end:
            global_start, global_end = global_end, global_start
        missing_ranges, total_missing = self._collect_sector_missing_ranges(source)
        if not missing_ranges:
            return {}
        merged_ranges: Dict[Tuple[str, str], Tuple[str, str]] = {}
        for key, (gap_start, gap_end) in missing_ranges.items():
            merged_start = min(global_start, gap_start) if global_start else gap_start
            merged_end = max(global_end, gap_end) if global_end else gap_end
            merged_ranges[key] = (
                merged_start.strftime('%Y%m%d'),
                merged_end.strftime('%Y%m%d')
            )
        print(
            f"🔍 检测到历史缺失交易日 {total_missing} 条，"
            f"涉及 {len(merged_ranges)} 个板块，将在本轮更新扩展抓取区间"
        )
        return merged_ranges
    def _detect_and_fill_missing_sector_data(self, source: str) -> bool:
        """检测板块历史数据缺失的交易日并进行回补"""
        metadata_file = self.ths_file if source == "ths" else self.dc_file if source == "eastmoney" else None
        if metadata_file is None or not metadata_file.exists():
            return True
        try:
            existing_metadata = pl.read_parquet(metadata_file)
            if existing_metadata is None or existing_metadata.is_empty():
                return True
            required_cols = {'日期', '板块名称', '板块类型'}
            if not required_cols.issubset(set(existing_metadata.columns)):
                return True
            from utils.trading_calendar import trading_calendar
            existing_metadata = existing_metadata.with_columns([
                pl.coalesce([
                    pl.col('日期').cast(pl.Date, strict=False),
                    pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, format='%Y-%m-%d', strict=False),
                    pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, format='%Y/%m/%d', strict=False),
                    pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, format='%Y%m%d', strict=False),
                ]).alias('日期'),
                pl.col('板块名称').cast(pl.Utf8).fill_null('').alias('板块名称'),
                pl.col('板块类型').cast(pl.Utf8).fill_null('').alias('板块类型'),
            ]).drop_nulls(subset=['日期'])
            key_cols = ['日期', '板块名称', '板块类型']
            existing_keys = existing_metadata.select(key_cols).unique()
            existing_key_set = set(
                (row['日期'], row['板块名称'], row['板块类型'])
                for row in existing_keys.iter_rows(named=True)
            )
            code_mapping: Dict[Tuple[str, str], str] = {}
            if '板块代码' in existing_metadata.columns:
                code_rows = (
                    existing_metadata
                    .select(['板块名称', '板块类型', '板块代码'])
                    .drop_nulls(subset=['板块名称', '板块类型'])
                    .with_columns([pl.col('板块代码').cast(pl.Utf8).fill_null('').alias('板块代码')])
                    .unique()
                )
                for row in code_rows.iter_rows(named=True):
                    if row['板块代码']:
                        code_mapping[(row['板块名称'], row['板块类型'])] = row['板块代码']
            sector_ranges = existing_metadata.group_by(['板块名称', '板块类型']).agg([
                pl.col('日期').min().alias('min_date'),
                pl.col('日期').max().alias('max_date'),
            ])
            missing_by_sector: Dict[Tuple[str, str], List[date]] = {}
            total_missing = 0
            for row in sector_ranges.iter_rows(named=True):
                sector_name = row['板块名称']
                sector_type = row['板块类型']
                min_date = row['min_date']
                max_date = row['max_date']
                if not sector_name or not sector_type or min_date is None or max_date is None:
                    continue
                for trading_day in trading_calendar.get_trading_days_in_range(min_date, max_date):
                    if (trading_day, sector_name, sector_type) not in existing_key_set:
                        missing_by_sector.setdefault((sector_name, sector_type), []).append(trading_day)
                        total_missing += 1
            if total_missing == 0:
                print("✅ 板块数据未发现交易日缺失")
                return True
            print(f"📊 发现板块缺失交易日数据: {total_missing} 条（全区间），开始回补...")
            range_to_sectors: Dict[Tuple[date, date], List[Dict[str, str]]] = {}
            missing_rows = []
            min_missing_date = None
            max_missing_date = None
            for (sector_name, sector_type), missing_dates in missing_by_sector.items():
                if not missing_dates:
                    continue
                range_key = (min(missing_dates), max(missing_dates))
                sector_code = code_mapping.get((sector_name, sector_type), '')
                range_to_sectors.setdefault(range_key, []).append({
                    '板块名称': sector_name,
                    '板块类型': sector_type,
                    '板块代码': sector_code
                })
                for d in missing_dates:
                    missing_rows.append({
                        '日期': d,
                        '板块名称': sector_name,
                        '板块类型': sector_type
                    })
                    min_missing_date = d if min_missing_date is None else min(min_missing_date, d)
                    max_missing_date = d if max_missing_date is None else max(max_missing_date, d)
            gap_filled_frames: List[pl.DataFrame] = []
            for (range_start, range_end), sector_items in range_to_sectors.items():
                start_compact = range_start.strftime('%Y%m%d')
                end_compact = range_end.strftime('%Y%m%d')
                print(f"  回补区间 {start_compact}-{end_compact}，板块数: {len(sector_items)}")
                filled = self._fill_missing_sector_data_for_date_range(
                    start_date=start_compact,
                    end_date=end_compact,
                    sector_items=sector_items,
                    source=source
                )
                if filled is not None and not filled.is_empty():
                    gap_filled_frames.append(filled)
            if not gap_filled_frames:
                print("⚠️ 未获取到可回补的板块缺失数据")
                return False
            gap_filled_data = pl.concat(gap_filled_frames, how='vertical_relaxed')
            if gap_filled_data.is_empty():
                print("⚠️ 回补结果为空")
                return False
            missing_pairs_pl = pl.DataFrame(missing_rows).with_columns([
                pl.col('日期').cast(pl.Date).alias('日期'),
                pl.col('板块名称').cast(pl.Utf8).fill_null('').alias('板块名称'),
                pl.col('板块类型').cast(pl.Utf8).fill_null('').alias('板块类型'),
            ])
            gap_filled_data = gap_filled_data.join(
                missing_pairs_pl,
                on=['日期', '板块名称', '板块类型'],
                how='inner'
            )
            if gap_filled_data.is_empty():
                print("⚠️ 回补接口返回的数据未命中实际缺失日期")
                return False
            merged = pl.concat([existing_metadata, gap_filled_data], how='vertical_relaxed').unique(
                subset=['日期', '板块名称', '板块类型'],
                keep='last'
            )
            if min_missing_date is not None and max_missing_date is not None:
                try:
                    window_start = min_missing_date - timedelta(days=90)
                    window_end = max_missing_date
                    window_mask = (pl.col('日期') >= window_start) & (pl.col('日期') <= window_end)
                    window_df = merged.filter(window_mask)
                    if not window_df.is_empty():
                        window_recalc = self._calculate_technical_indicators(window_df)
                        merged = pl.concat([merged.filter(~window_mask), window_recalc], how='vertical_relaxed').unique(
                            subset=['日期', '板块名称', '板块类型'],
                            keep='last'
                        )
                except Exception as e:
                    print(f"⚠️ 回补后窗口重算技术指标失败(忽略): {e}")
            merged.write_parquet(metadata_file)
            print(f"✅ 板块缺失数据回补完成: {gap_filled_data.height} 条")
            return True
        except Exception as e:
            print(f"⚠️ 检测或回补板块缺失数据失败: {e}")
            return False
    def update_sector_data(self, source: str = None, start_date: str = None, end_date: str = None, fill_gaps: bool = True) -> bool:
        """使用指定数据源更新板块数据"""
        if source is None:
            source = self.preferred_source
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        print(f"📊 使用{source}数据源更新板块数据 ({start_date} - {end_date})")
        date_range_by_board: Dict[Tuple[str, str], Tuple[str, str]] = {}
        if fill_gaps:
            date_range_by_board = self._build_sector_fetch_ranges_for_update(source, start_date, end_date)
            if date_range_by_board:
                print("🧩 融合更新模式：按板块动态区间抓取（每个板块单次请求）")
            else:
                print("🧩 融合更新模式：未发现历史缺口，按给定区间抓取")
        if not fill_gaps:
            print("⚡ 单轮更新模式：跳过缺失回补，仅按给定日期范围抓取一次")
        all_data = []
        success = False
        if source == "ths":
            # 使用同花顺数据源更新板块数据
            try:
                print("📊 使用同花顺数据源更新板块数据...")
                # 调用同花顺数据提供器的板块数据更新方法
                # update_all_sectors只接受sector_dir参数，用于更新成分股数据
                # 对于板块历史数据，使用update_sector_data方法
                success = self.ths_provider.update_sector_data(
                    self.sector_dir,
                    years_back=1,
                    force_update=False,
                    start_date=start_date,
                    end_date=end_date,
                    date_range_by_board=date_range_by_board if fill_gaps else None,
                )
                if success:
                    # ThsDataProvider已经将新数据保存到临时文件，现在处理技术指标和合并
                    temp_new_data_file = self.sector_dir / "temp_new_data_ths.parquet"
                    if temp_new_data_file.exists():
                        print("📊 处理ThsDataProvider保存的新数据...")
                        new_data = pl.read_parquet(temp_new_data_file)
                        print(f"📊 新数据: {new_data.height} 条记录")
                        # 规范新数据的关键列类型，避免与历史数据拼接时报 dtype 冲突
                        cols_to_utf8 = ['板块代码', '板块名称', '板块类型', '数据源']
                        present_cols = [c for c in cols_to_utf8 if c in new_data.columns]
                        if present_cols:
                            new_data = new_data.with_columns([
                                pl.col(c).cast(pl.Utf8).fill_null("").alias(c) for c in present_cols
                            ])
                        # 将日期与数值列统一为与历史文件一致的类型
                        numeric_cols = [
                            '开盘', '收盘', '最高', '最低', '成交量', '成交额',
                            '换手率', '涨跌幅', '振幅', '5日涨跌幅', '10日涨跌幅',
                            'MA5', 'MA10', 'MA20', '成交额量比'
                        ]
                        present_numeric = [c for c in numeric_cols if c in new_data.columns]
                        cast_exprs = []
                        if '日期' in new_data.columns:
                            # 统一为日期，兼容多种输入格式
                            cast_exprs.append(
                                _coalesce_to_date_expr('日期').alias('日期')
                            )
                        for c in present_numeric:
                            cast_exprs.append(pl.col(c).cast(pl.Float64).alias(c))
                        if cast_exprs:
                            new_data = new_data.with_columns(cast_exprs)
                        # 为新数据添加技术指标
                        print("📊 为新数据计算技术指标...")
                        new_data_with_indicators = self._calculate_technical_indicators(new_data)
                        print(f"📊 技术指标计算完成: {new_data_with_indicators.height} 条记录, {len(new_data_with_indicators.columns)} 列")
                        # 合并历史数据和新数据
                        if self.ths_file.exists():
                            print(f"📊 读取历史数据: {self.ths_file}")
                            historical_data = pl.read_parquet(self.ths_file)
                            # 同样规范历史数据，以确保类型一致
                            present_cols_hist = [c for c in cols_to_utf8 if c in historical_data.columns]
                            if present_cols_hist:
                                historical_data = historical_data.with_columns([
                                    pl.col(c).cast(pl.Utf8).fill_null("").alias(c) for c in present_cols_hist
                                ])
                            print(f"📊 历史数据: {historical_data.height} 条记录")
                            # 确保列顺序一致
                            # Use union of columns to avoid dropping new indicators
                            historical_columns = set(historical_data.columns)
                            new_columns = set(new_data_with_indicators.columns)
                            all_columns = sorted(historical_columns.union(new_columns))
                            print(f"📊 列并集数: {len(all_columns)}")
                            # Align new data types to historical schema where possible
                            try:
                                target_schema = historical_data.schema
                                align_exprs = []
                                for col in all_columns:
                                    try:
                                        target_dtype = target_schema.get(col)
                                        if target_dtype is not None and col in new_data_with_indicators.columns:
                                            align_exprs.append(pl.col(col).cast(target_dtype).alias(col))
                                    except Exception:
                                        pass
                                if align_exprs:
                                    new_data_with_indicators = new_data_with_indicators.with_columns(align_exprs)
                            except Exception as _e:
                                print(f"⚠️ 列类型对齐时出错: {_e}")
                            # Fill missing columns
                            for col in all_columns:
                                if col not in historical_data.columns:
                                    historical_data = historical_data.with_columns([pl.lit(None).alias(col)])
                                if col not in new_data_with_indicators.columns:
                                    new_data_with_indicators = new_data_with_indicators.with_columns([pl.lit(None).alias(col)])
                            # 如果历史列是Null类型而新数据有具体类型，提升历史列类型，避免vstack类型冲突
                            try:
                                hist_schema = historical_data.schema
                                new_schema = new_data_with_indicators.schema
                                cast_hist_exprs = []
                                for col in all_columns:
                                    hist_dtype = hist_schema.get(col)
                                    new_dtype = new_schema.get(col)
                                    if hist_dtype == pl.Null and new_dtype is not None and new_dtype != pl.Null:
                                        cast_hist_exprs.append(pl.col(col).cast(new_dtype).alias(col))
                                if cast_hist_exprs:
                                    historical_data = historical_data.with_columns(cast_hist_exprs)
                            except Exception as _e:
                                print(f"⚠️ 历史列类型提升失败(忽略): {_e}")
                            historical_data_aligned = historical_data.select(all_columns)
                            new_data_aligned = new_data_with_indicators.select(all_columns)
                            unified_data = pl.concat([historical_data_aligned, new_data_aligned], how="vertical_relaxed")
                            # 按日期、板块名称去重，保留最新的数据
                            unified_data = unified_data.unique(subset=['日期', '板块名称','板块类型'], keep='last')
                            print(f"📊 去重后数据: {unified_data.height} 条记录")
                        else:
                            print("� 创建新的数据文件")
                            unified_data = new_data_with_indicators
                        # 保存合并后的数据
                        # Recompute indicators within window after merge
                        try:
                            if '日期' in unified_data.columns and not unified_data.is_empty():
                                unified_data = unified_data.with_columns([
                                    _coalesce_to_date_expr('日期').alias('日期')
                                ])
                                new_dates = new_data_with_indicators.select(
                                    _coalesce_to_date_expr('日期').alias('日期')
                                ).drop_nulls()
                                if not new_dates.is_empty():
                                    min_new_date = new_dates['日期'].min()
                                    max_new_date = new_dates['日期'].max()
                                    window_days = 90
                                    window_start = min_new_date - timedelta(days=window_days)
                                    window_end = max_new_date
                                    window_mask = (pl.col('日期') >= window_start) & (pl.col('日期') <= window_end)
                                    window_df = unified_data.filter(window_mask)
                                    if not window_df.is_empty():
                                        print(f"📊 窗口重算指标: {window_start} ~ {window_end} (共{window_df.height}条)")
                                        window_recalc = self._calculate_technical_indicators(window_df)
                                        unified_no_window = unified_data.filter(~window_mask)
                                        unified_data = pl.concat([unified_no_window, window_recalc], how="vertical_relaxed").unique(
                                            subset=['日期', '板块名称', '板块类型'], keep='last'
                                        )
                        except Exception as _e:
                            print(f"⚠️ 窗口重算指标失败(忽略): {_e}")
                        unified_data.write_parquet(self.ths_file)
                        print(f"✅ 数据保存成功: {self.ths_file}")
                        # 显示新增数据统计
                        new_records = unified_data.height - historical_data_aligned.height
                        print(f"📊 新增数据统计:")
                        print(f"  原有记录数: {historical_data_aligned.height}")
                        print(f"  当前记录数: {unified_data.height}")
                        print(f"  新增记录数: {new_records}")
                        # 清理临时文件
                        #temp_new_data_file.unlink()
                        #print("🗑️ 临时文件已清理")
                        success = True
                    else:
                        print("❌ 未找到ThsDataProvider保存的临时文件")
                        success = False
            except Exception as e:
                print(f"❌ 同花顺数据源更新失败: {e}")
                success = False
        elif source == "eastmoney":
            # 使用东财数据源更新板块数据
            try:
                print("📊 使用东财数据源更新板块数据...")
                # 调用东财数据提供器的板块数据更新方法
                success = self.eastmoney_provider.update_sector_data(
                    start_date,
                    end_date,
                    self.sector_dir,
                    date_range_by_board=date_range_by_board if fill_gaps else None,
                )
                if success:
                    # 加载更新后的数据
                    if self.dc_file.exists():
                        df = pl.read_parquet(self.dc_file)
                        # 检查是否包含技术指标
                        required_columns = ['涨跌幅', '振幅', '换手率', '5日涨跌幅', '10日涨跌幅', 'MA5', 'MA10', 'MA20', '成交额量比']
                        missing_columns = [col for col in required_columns if col not in df.columns]
                        if missing_columns:
                            print(f"⚠️ 数据缺少技术指标列: {missing_columns}")
                            print("📊 重新计算技术指标...")
                            df = self._calculate_technical_indicators(df)
                        all_data.append(df)
                        # 保存更新后的数据
                        print(f"💾 保存东财板块数据到文件...")
                        df.write_parquet(self.dc_file)
                        print(f"✅ 数据已保存: {df.height} 条记录, {len(df.columns)} 列")
                        success = True
                    else:
                        success = False
            except Exception as e:
                print(f"❌ 东财数据源更新失败: {e}")
                success = False
        else:
            print(f"❌ 不支持的数据源: {source}")
            return False
        if fill_gaps and success:
            print("✅ 融合更新已完成：缺口已在本轮抓取中一并处理")
        return success
    def _calculate_technical_indicators(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算技术指标"""
        try:
            print("📊 计算技术指标...")
            if df is None or df.is_empty():
                return df
            required_cols = {'板块名称', '日期', '开盘', '收盘', '最高', '最低'}
            missing_required = required_cols.difference(set(df.columns))
            if missing_required:
                print(f"⚠️ 计算技术指标缺少必要列: {sorted(missing_required)}")
                return df
            working_df = df
            if '成交额' not in working_df.columns:
                working_df = working_df.with_columns([pl.lit(0.0).alias('成交额')])
            working_df = working_df.with_columns([
                pl.coalesce([
                    pl.col('日期').cast(pl.Date, strict=False),
                    pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, format='%Y-%m-%d', strict=False),
                    pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, format='%Y/%m/%d', strict=False),
                    pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, format='%Y%m%d', strict=False),
                ]).alias('日期'),
                pl.col('开盘').cast(pl.Float64).alias('开盘'),
                pl.col('收盘').cast(pl.Float64).alias('收盘'),
                pl.col('最高').cast(pl.Float64).alias('最高'),
                pl.col('最低').cast(pl.Float64).alias('最低'),
                pl.col('成交额').cast(pl.Float64).fill_null(0.0).alias('成交额'),
            ]).sort(['板块名称', '日期'])
            working_df = working_df.with_columns([
                (pl.col('收盘') > pl.col('开盘')).alias('is_positive_day'),
                pl.when(pl.col('开盘').is_not_null() & (pl.col('开盘') != 0))
                .then((pl.col('收盘') - pl.col('开盘')) / pl.col('开盘') * 100)
                .otherwise(0.0)
                .alias('涨跌幅'),
                pl.when(pl.col('开盘').is_not_null() & (pl.col('开盘') != 0))
                .then((pl.col('最高') - pl.col('最低')) / pl.col('开盘') * 100)
                .otherwise(0.0)
                .alias('振幅'),
                pl.lit(0.0).alias('换手率'),
                pl.col('收盘').rolling_mean(window_size=5).over('板块名称').fill_null(0.0).alias('MA5'),
                pl.col('收盘').rolling_mean(window_size=10).over('板块名称').fill_null(0.0).alias('MA10'),
                pl.col('收盘').rolling_mean(window_size=20).over('板块名称').fill_null(0.0).alias('MA20'),
                pl.col('成交额').rolling_mean(window_size=5).over('板块名称').alias('_成交额MA5'),
            ])
            close_shift_5 = pl.col('收盘').shift(5).over('板块名称')
            close_shift_10 = pl.col('收盘').shift(10).over('板块名称')
            working_df = working_df.with_columns([
                pl.when(close_shift_5.is_not_null() & (close_shift_5 != 0))
                .then((pl.col('收盘') - close_shift_5) / close_shift_5 * 100)
                .otherwise(0.0)
                .alias('5日涨跌幅'),
                pl.when(close_shift_10.is_not_null() & (close_shift_10 != 0))
                .then((pl.col('收盘') - close_shift_10) / close_shift_10 * 100)
                .otherwise(0.0)
                .alias('10日涨跌幅'),
                pl.when(pl.col('_成交额MA5').is_not_null() & (pl.col('_成交额MA5') != 0))
                .then(pl.col('成交额') / pl.col('_成交额MA5'))
                .otherwise(0.0)
                .alias('成交额量比'),
            ])
            # 旧版 Polars 不支持在一个表达式里嵌套 over(window)；拆成多步避免 window 嵌套
            working_df = working_df.with_columns([
                pl.col('is_positive_day')
                .shift(1)
                .over('板块名称')
                .alias('_prev_positive_day')
            ])
            working_df = working_df.with_columns([
                pl.when(pl.col('is_positive_day'))
                .then(
                    (pl.col('is_positive_day') != pl.col('_prev_positive_day'))
                    .cast(pl.Int64)
                    .cumsum()
                    .over('板块名称')
                )
                .otherwise(None)
                .alias('_positive_group')
            ]).with_columns([
                pl.when(pl.col('is_positive_day'))
                .then(
                    pl.col('is_positive_day')
                    .cast(pl.Int64)
                    .cumsum()
                    .over(['板块名称', '_positive_group'])
                )
                .otherwise(0)
                .cast(pl.Int64)
                .alias('连阳天数'),
                pl.lit(0).cast(pl.Int64).alias('涨停数'),
            ]).drop(['_positive_group', '_成交额MA5', '_prev_positive_day'])
            float_cols = ['涨跌幅', '振幅', '换手率', '5日涨跌幅', '10日涨跌幅', 'MA5', 'MA10', 'MA20', '成交额量比']
            present_float_cols = [c for c in float_cols if c in working_df.columns]
            if present_float_cols:
                working_df = working_df.with_columns([
                    pl.col(c).cast(pl.Float64).fill_null(0.0).alias(c) for c in present_float_cols
                ])
            for int_col in ['连阳天数', '涨停数']:
                if int_col in working_df.columns:
                    working_df = working_df.with_columns([
                        pl.col(int_col).cast(pl.Int64).fill_null(0).alias(int_col)
                    ])
            final_df = self._attach_sector_limit_up_counts(working_df)
            sector_count = final_df['板块名称'].n_unique() if '板块名称' in final_df.columns else 0
            print(f"✅ 技术指标计算完成，处理了 {sector_count} 个板块")
            return final_df
        except Exception as e:
            print(f"❌ 计算技术指标失败: {e}")
            import traceback
            traceback.print_exc()
            return df  # 返回原始数据
    def _attach_sector_limit_up_counts(self, df: pl.DataFrame) -> pl.DataFrame:
        """Attach sector limit-up counts per date if data is available."""
        try:
            if df is None or df.is_empty():
                return df
            source = getattr(self, "preferred_source", None) or "ths"
            _, constituents_pl = self._load_constituents_with_fallback(source)
            if constituents_pl is None:
                return df
            if constituents_pl is None or constituents_pl.is_empty():
                return df
            if "板块名称" not in constituents_pl.columns and "所属板块" in constituents_pl.columns:
                constituents_pl = constituents_pl.with_columns([pl.col("所属板块").alias("板块名称")])
            if "代码" not in constituents_pl.columns and "股票代码" in constituents_pl.columns:
                constituents_pl = constituents_pl.with_columns([pl.col("股票代码").alias("代码")])
            if "板块名称" not in constituents_pl.columns or "代码" not in constituents_pl.columns:
                return df
            constituents_pl = constituents_pl.select(["板块名称", "代码"]).with_columns([
                pl.col("板块名称").cast(pl.Utf8).fill_null(""),
                pl.col("代码").cast(pl.Utf8).str.extract(r"(\\d{6})", 0)
            ]).filter(pl.col("代码").is_not_null())
            market_states = MarketMetadataManager().load_market_states()
            if market_states is None or market_states.is_empty():
                return df
            if "日期" not in market_states.columns or "涨停" not in market_states.columns:
                return df
            code_col = "代码" if "代码" in market_states.columns else ("股票代码" if "股票代码" in market_states.columns else None)
            if code_col is None:
                return df
            market_states = market_states.with_columns([
                _coalesce_to_date_expr("日期").alias("日期"),
                pl.col(code_col).cast(pl.Utf8).str.extract(r"(\\d{6})", 0).alias("代码")
            ])
            df_dates = df.select(_coalesce_to_date_expr("日期").alias("日期")).drop_nulls().unique()
            if df_dates.is_empty():
                return df
            market_states = market_states.join(df_dates, on="日期", how="inner")
            limit_up = market_states.filter(pl.col("涨停") == True).select(["日期", "代码"])
            if limit_up.is_empty():
                return df.with_columns([pl.lit(0).cast(pl.Int64).alias("涨停数")]) if "涨停数" not in df.columns else df
            joined = limit_up.join(constituents_pl, on="代码", how="inner")
            if joined.is_empty():
                return df
            counts = joined.group_by(["日期", "板块名称"]).agg(pl.count().alias("涨停数"))
            result = df.join(counts, on=["日期", "板块名称"], how="left")
            if "涨停数" in result.columns:
                result = result.with_columns([pl.col("涨停数").fill_null(0).cast(pl.Int64).alias("涨停数")])
            else:
                result = result.with_columns([pl.lit(0).cast(pl.Int64).alias("涨停数")])
            return result
        except Exception as e:
            print(f"⚠️ 计算板块涨停数失败(忽略): {e}")
            return df
    def update_constituents_data(self, source: str = None, force_update: bool = False) -> bool:
        """
        更新成分股数据 - 协调调用对应的数据提供器
        Args:
            source: 数据源 ("ths" 或 "eastmoney")，默认使用preferred_source
            force_update: 是否强制更新
        Returns:
            更新是否成功
        """
        if source is None:
            source = self.preferred_source
        print(f"🚀 使用{source}数据源更新成分股数据...")
        # 调用对应的数据提供器
        try:
            if source == "ths":
                # 同花顺数据提供器的成分股更新方法名为update_sector_constituents
                return self.ths_provider.update_sector_constituents(self.sector_dir)
            elif source == "eastmoney":
                return self.eastmoney_provider.update_all_constituents(self.sector_dir)
            else:
                print(f"❌ 不支持的数据源: {source}")
                return False
        except Exception as e:
            print(f"❌ 更新成分股数据失败: {e}")
            return False
    def _load_constituents_with_fallback(self, source: str) -> Tuple[str, Optional[pl.DataFrame]]:
        """
        统一加载成分股数据，支持缓存与源回退。
        返回: (实际加载源, 数据)
        """
        candidates = [source]
        for fallback in ("ths", "eastmoney"):
            if fallback not in candidates:
                candidates.append(fallback)
        for candidate in candidates:
            cached = self._get_cached_constituents(candidate)
            if cached is not None:
                return candidate, cached
            if candidate == "ths" and self.ths_constituents_file.exists():
                print(f"📊 使用同花顺成分股数据: {self.ths_constituents_file}")
                constituents_data = pd.read_excel(self.ths_constituents_file, sheet_name="所有数据")
                constituents_pl = pl.from_pandas(constituents_data)
                self._cache_constituents(candidate, constituents_pl)
                return candidate, constituents_pl
            if candidate == "eastmoney" and self.dc_constituents_file.exists():
                print(f"📊 使用东财成分股数据: {self.dc_constituents_file}")
                constituents_data = pd.read_excel(self.dc_constituents_file, sheet_name="Sheet1")
                constituents_pl = pl.from_pandas(constituents_data)
                self._cache_constituents(candidate, constituents_pl)
                return candidate, constituents_pl
        return source, None
    def _get_cached_constituents(self, source: str) -> Optional[pl.DataFrame]:
        """获取缓存的成分股数据"""
        cache_key = f"constituents_{source}"
        
        # 检查缓存是否存在且未过期
        if cache_key in self._constituents_cache:
            cache_time = self._cache_timestamps.get(cache_key, 0)
            if time.time() - cache_time < self._cache_expire_seconds:
                print(f"💾 使用缓存的成分股数据 ({source})")
                return self._constituents_cache[cache_key]
            else:
                print(f"⏰ 成分股缓存已过期，重新加载 ({source})")
                # 清理过期缓存
                del self._constituents_cache[cache_key]
                del self._cache_timestamps[cache_key]
        
        return None
    
    def _cache_constituents(self, source: str, data: pl.DataFrame):
        """缓存成分股数据"""
        cache_key = f"constituents_{source}"
        self._constituents_cache[cache_key] = data
        self._cache_timestamps[cache_key] = time.time()
        
        # 为板块名称列建立索引以加速查询
        self._build_sector_index(cache_key, data)
        
        print(f"💾 已缓存成分股数据 ({source}), 行数: {len(data)}")
    
    def _build_sector_index(self, cache_key: str, data: pl.DataFrame):
        """为板块名称建立索引"""
        try:
            sector_col = None
            if '板块名称' in data.columns:
                sector_col = '板块名称'
            elif '所属板块' in data.columns:
                sector_col = '所属板块'
            
            if sector_col:
                # 建立板块名称到行索引的映射
                index_dict = {}
                sector_values = data.get_column(sector_col).to_list()
                for i, sector_name in enumerate(sector_values):
                    if sector_name:
                        if sector_name not in index_dict:
                            index_dict[sector_name] = []
                        index_dict[sector_name].append(i)
                
                self._index_cache[f"{cache_key}_sector_index"] = {
                    'column': sector_col,
                    'index': index_dict
                }
                print(f"🔍 已建立板块索引，包含 {len(index_dict)} 个板块")
        except Exception as e:
            print(f"⚠️ 建立索引失败: {e}")
    
    def _get_stocks_by_sector_fast(self, cache_key: str, sector_name: str, data: pl.DataFrame) -> Optional[pl.DataFrame]:
        """使用索引快速获取板块成分股"""
        index_key = f"{cache_key}_sector_index"
        
        if index_key in self._index_cache:
            index_info = self._index_cache[index_key]
            sector_index = index_info['index']
            
            # 精确匹配
            if sector_name in sector_index:
                row_indices = sector_index[sector_name]
                return data[row_indices]
            
            # 模糊匹配
            for indexed_sector in sector_index:
                if sector_name in indexed_sector or indexed_sector in sector_name:
                    row_indices = sector_index[indexed_sector]
                    return data[row_indices]
        
        return None
    def get_sector_stocks(self, sector_name: str, source: str = None) -> Optional[pl.DataFrame]:
        """
        获取板块成分股
        Args:
            sector_name: 板块名称
            source: 数据源 ("ths" 或 "eastmoney")，默认使用preferred_source
        Returns:
            成分股数据DataFrame，包含股票代码、股票名称等信息
        """
        try:
            if source is None:
                source = self.preferred_source
            resolved_source, constituents_pl = self._load_constituents_with_fallback(source)
            if constituents_pl is None:
                print("⚠️ 没有找到成分股数据文件")
                return None
            # 筛选指定板块的成分股 - 使用索引优化
            cache_key = f"constituents_{resolved_source}"
            sector_stocks = self._get_stocks_by_sector_fast(cache_key, sector_name, constituents_pl)
            
            # 如果索引查询失败，回退到传统查询方法
            if sector_stocks is None or sector_stocks.is_empty():
                print("🔍 索引查询未找到结果，使用传统查询方法")
                sector_stocks = pl.DataFrame()
                if '板块名称' in constituents_pl.columns:
                    # 精确匹配
                    sector_stocks = constituents_pl.filter(pl.col('板块名称') == sector_name)
                    # 如果精确匹配失败，尝试模糊匹配
                    if sector_stocks.is_empty():
                        sector_stocks = constituents_pl.filter(pl.col('板块名称').str.contains(sector_name))
                elif '所属板块' in constituents_pl.columns:
                    # 精确匹配
                    sector_stocks = constituents_pl.filter(pl.col('所属板块') == sector_name)
                    # 如果精确匹配失败，尝试模糊匹配
                    if sector_stocks.is_empty():
                        sector_stocks = constituents_pl.filter(pl.col('所属板块').str.contains(sector_name))
                else:
                    print(f"❌ 成分股数据中没有找到板块名称列，可用列: {constituents_pl.columns}")
                    return None
            else:
                print("⚡ 使用索引快速查询板块成分股")
            if not sector_stocks.is_empty():
                # 确保股票代码为6位格式
                sector_stocks = sector_stocks.with_columns([
                    pl.col('股票代码').cast(pl.Utf8).str.zfill(6).alias('股票代码')
                ])
                # 统一列名，确保有标准的列名
                if '所属板块' in sector_stocks.columns and '板块名称' not in sector_stocks.columns:
                    sector_stocks = sector_stocks.rename({'所属板块': '板块名称'})
                # 确保有必要的列名映射
                if '股票代码' in sector_stocks.columns and '代码' not in sector_stocks.columns:
                    sector_stocks = sector_stocks.with_columns([
                        pl.col('股票代码').alias('代码')
                    ])
                if '股票名称' in sector_stocks.columns and '名称' not in sector_stocks.columns:
                    sector_stocks = sector_stocks.with_columns([
                        pl.col('股票名称').alias('名称')
                    ])
                # 去重：按标准化后的代码唯一
                try:
                    if '代码' in sector_stocks.columns:
                        sector_stocks = sector_stocks.unique(subset=['代码'], keep='first')
                    elif '股票代码' in sector_stocks.columns:
                        sector_stocks = sector_stocks.unique(subset=['股票代码'], keep='first')
                except Exception as _e:
                    print(f"⚠️ 成分股去重失败(忽略): {_e}")
                return sector_stocks
            else:
                return None
        except Exception as e:
            print(f"❌ 获取板块成分股失败: {e}")
            return None
    def get_sector_names(self, sector_type: str = 'both') -> dict:
        """
        统一获取板块名称的方法
        Args:
            sector_type: 'sectors', 'concepts', 'both'
        Returns:
            dict: 包含板块名称的字典
        """
        try:
            result = {}
            if sector_type in ['sectors', 'both']:
                # 获取行业板块名称
                try:
                    df = self.load_sector_data(include_sectors=True, include_concepts=False)
                    if not df.is_empty():
                        sector_names = df.filter(pl.col('板块类型') == '行业')['板块名称'].unique().to_list()
                        sector_names = sorted(sector_names)
                    else:
                        sector_names = []
                except Exception as e:
                    print(f"❌ 获取行业板块名称失败: {e}")
                    sector_names = []
                result['sector_names'] = sector_names
                result['sector_count'] = len(sector_names)
            if sector_type in ['concepts', 'both']:
                # 获取概念板块名称
                try:
                    df = self.load_sector_data(include_sectors=False, include_concepts=True)
                    if not df.is_empty():
                        concept_names = df.filter(pl.col('板块类型') == '概念')['板块名称'].unique().to_list()
                        concept_names = sorted(concept_names)
                    else:
                        concept_names = []
                except Exception as e:
                    print(f"❌ 获取概念板块名称失败: {e}")
                    concept_names = []
                result['concept_names'] = concept_names
                result['concept_count'] = len(concept_names)
            return result
        except Exception as e:
            print(f"❌ 统一获取板块名称失败: {e}")
            return {}
