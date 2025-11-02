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
from datetime import datetime, timedelta, time as dt_time, date
from pathlib import Path
from typing import Optional, List, Dict, Tuple
import warnings

# 屏蔽pandas警告
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)

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
                                   max_retries: int = 3) -> Optional[pd.DataFrame]:
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
                print(f"📊 获取{sector_type}板块数据: {sector_name} (尝试 {attempt + 1}/{max_retries})")
                
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
                    
                    print(f"✅ 成功获取 {len(data)} 条记录，覆盖 {coverage_years} 年数据")
                    return data
                else:
                    print(f"⚠️ 获取数据失败或为空 (尝试 {attempt + 1})")
                    
            except Exception as e:
                error_msg = f"获取{sector_name}数据失败 (尝试 {attempt + 1}): {e}"
                print(f"❌ {error_msg}")
                
                # 内联错误日志记录
                try:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    with open(self.error_log_file, 'a', encoding='utf-8') as f:
                        f.write(f"[{timestamp}] {error_msg}\n")
                except Exception as log_e:
                    print(f"⚠️ 记录错误日志失败: {log_e}")
                
                if attempt < max_retries - 1:
                    # 指数退避延迟
                    delay = (attempt + 1) * 2 + random.uniform(1, 3)
                    print(f"⏳ 等待 {delay:.1f} 秒后重试...")
                    time.sleep(delay)
        
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
                    
                    # 选择需要的列
                    result_columns = ['股票代码', '股票名称', '板块名称', '板块代码', '板块类型', '更新日期', '数据源']
                    stocks_data = valid_df[result_columns].to_dict('records')

                    if stocks_data:
                        result_df = pd.DataFrame(stocks_data)
                        print(f"✅ 获取到 {len(result_df)} 只{sector_type}成分股")
                        return result_df
                    else:
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

    def update_sector_data(self, sector_dir: Path, years_back: int = 1, force_update: bool = False) -> bool:
        """全面更新同花顺板块数据"""
        try:
            print(f"🚀 开始全面更新同花顺板块数据 (最近{years_back}年)")

            # 检查是否需要更新
            from pathlib import Path
            sector_path = Path(sector_dir)
            sectors_ths_file = sector_path / "sectors_ths.parquet"

            # 计算日期范围
            end_date = (datetime.now() + pd.Timedelta(days=1)).strftime('%Y%m%d')
            start_date = (datetime.now() - pd.Timedelta(days=years_back*31)).strftime('%Y%m%d')

            print(f"📅 更新日期范围: {start_date} 到 {end_date}")

            # 更新行业数据
            print("\n📊 更新同花顺行业板块数据...")
            industry_success = self._update_industries(start_date, end_date, sector_path)

            # 更新概念数据
            print("\n📊 更新同花顺概念板块数据...")
            concept_success = self._update_concepts(start_date, end_date, sector_path)

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

    def _update_industries(self, start_date: str, end_date: str, sector_dir: Path) -> bool:
        """更新同花顺行业板块数据"""
        print("\n📊 更新同花顺行业板块数据...")

        try:
            # 直接使用AKShare获取行业列表
            print("📊 获取同花顺行业指数列表...")
            industries_df = ak.stock_board_industry_name_ths()
            if industries_df is None or industries_df.empty:
                print("❌ 获取行业列表失败")
                return False

            print(f"✅ 获取到 {len(industries_df)} 个同花顺行业指数")

            print(f"📋 获取到 {len(industries_df)} 个行业板块")

            all_industry_data = []
            success_count = 0

            # 检查是否有测试限制（默认全量处理）
            test_limit = getattr(self, '_test_industry_limit', None)
            if test_limit is None:
                print(f"🚀 全量模式：处理所有 {len(industries_df)} 个行业")
                industries_to_process = industries_df
            else:
                print(f"🧪 测试模式：只处理前 {test_limit} 个行业")
                industries_to_process = industries_df.head(test_limit)

            for index, row in industries_to_process.iterrows():
                try:
                    # 使用实际的列名：code 和 name
                    if 'code' not in row or 'name' not in row:
                        print(f"    ❌ 第{index+1}行缺少必要列，可用列: {list(row.index)}")
                        continue

                    symbol = row['code']
                    name = row['name']

                    print(f"  📊 处理行业: {name} ({symbol}) [{index+1}/{len(industries_df)}]")

                    # 使用带重试机制的方法获取行业指数数据
                    industry_data = self.get_sector_data_with_retry(
                        sector_name=name,
                        sector_type="行业",
                        start_date=start_date,
                        end_date=end_date
                    )

                    if industry_data is not None and not industry_data.empty:
                        # 添加板块代码（板块名称已在方法内添加）
                        industry_data['板块代码'] = symbol

                        all_industry_data.append(industry_data)
                        success_count += 1
                        print(f"    ✅ 成功获取 {len(industry_data)} 条数据")
                    else:
                        print(f"    ⚠️ 未获取到数据")

                except Exception as e:
                    # 安全的异常处理，避免引用未定义的变量
                    row_info = f"第{index+1}行" if 'name' not in locals() else f"行业 {name}"
                    print(f"    ❌ 处理{row_info}失败: {e}")
                    continue

            total_industries = len(industries_to_process) if test_limit is not None else len(industries_df)
            print(f"\n📊 行业数据获取完成: {success_count}/{total_industries} 成功")

            # 保存行业数据到临时文件
            if all_industry_data:
                # 合并所有行业数据
                combined_data = pd.concat(all_industry_data, ignore_index=True)
                combined_pl = pl.from_pandas(combined_data)

                # 在保存前，统一关键列的数据类型，避免后续拼接类型不一致
                to_cast_cols = ['板块代码', '板块名称', '板块类型', '数据源']
                for col in to_cast_cols:
                    if col in combined_pl.columns:
                        # 将列统一为字符串，并将空值转为空字符串，避免出现 Null dtype
                        combined_pl = combined_pl.with_columns([
                            pl.col(col).cast(pl.Utf8).fill_null("").alias(col)
                        ])

                # 保存原始数据到临时文件，等待SectorDataManager添加技术指标
                temp_file = sector_dir / "temp_industry_ths.parquet"
                combined_pl.write_parquet(temp_file)

                print(f"✅ 行业板块原始数据保存到临时文件: {combined_pl.height} 条记录")
                return True
            else:
                print("❌ 没有获取到任何行业数据")
                return False

        except Exception as e:
            print(f"❌ 更新行业板块数据失败: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _update_concepts(self, start_date: str, end_date: str, sector_dir: Path) -> bool:
        """更新同花顺概念板块数据"""
        print("\n📊 更新同花顺概念板块数据...")

        try:
            # 直接使用AKShare获取概念列表
            print("📊 获取同花顺概念指数列表...")
            concepts_df = ak.stock_board_concept_name_ths()
            if concepts_df is None or concepts_df.empty:
                print("❌ 获取概念列表失败")
                return False

            print(f"✅ 获取到 {len(concepts_df)} 个同花顺概念指数")
            print(f"📋 获取到 {len(concepts_df)} 个概念板块")

            all_concept_data = []
            success_count = 0

            # 检查是否有测试限制（默认全量处理）
            test_limit = getattr(self, '_test_concept_limit', None)
            if test_limit is None:
                print(f"🚀 全量模式：处理所有 {len(concepts_df)} 个概念")
                concepts_to_process = concepts_df
            else:
                print(f"🧪 测试模式：只处理前 {test_limit} 个概念")
                concepts_to_process = concepts_df.head(test_limit)

            for index, row in concepts_to_process.iterrows():
                try:
                    # 使用AKShare返回的列名：code 和 name
                    if 'code' not in row or 'name' not in row:
                        print(f"    ❌ 第{index+1}行缺少必要列，可用列: {list(row.index)}")
                        continue

                    symbol = row['code']
                    name = row['name']

                    print(f"  📊 处理概念: {name} ({symbol}) [{index+1}/{len(concepts_df)}]")

                    # 使用带重试机制的方法获取概念指数数据
                    concept_data = self.get_sector_data_with_retry(
                        sector_name=name,
                        sector_type="概念",
                        start_date=start_date,
                        end_date=end_date
                    )

                    if concept_data is not None and not concept_data.empty:
                        # 添加板块代码（板块名称已在方法内添加）
                        concept_data['板块代码'] = symbol

                        all_concept_data.append(concept_data)
                        success_count += 1
                        print(f"    ✅ 成功获取 {len(concept_data)} 条数据")

                        # 每处理50个概念保存一次进度
                        if len(all_concept_data) % 50 == 0:
                            print(f"  💾 保存进度: 已处理 {len(all_concept_data)} 个概念")
                    else:
                        print(f"    ⚠️ 未获取到数据")

                except Exception as e:
                    # 安全的异常处理，避免引用未定义的变量
                    row_info = f"第{index+1}行" if 'name' not in locals() else f"概念 {name}"
                    print(f"    ❌ 处理{row_info}失败: {e}")
                    continue

            total_concepts = len(concepts_to_process) if test_limit is not None else len(concepts_df)
            print(f"\n📊 概念数据获取完成: {success_count}/{total_concepts} 成功")

            # 保存概念数据到临时文件
            if all_concept_data:
                # 如果没有data_processor，使用简单保存
                combined_data = pd.concat(all_concept_data, ignore_index=True)
                combined_pl = pl.from_pandas(combined_data)

                # 在保存前，统一关键列的数据类型，避免后续拼接类型不一致
                to_cast_cols = ['板块代码', '板块名称', '板块类型', '数据源']
                for col in to_cast_cols:
                    if col in combined_pl.columns:
                        combined_pl = combined_pl.with_columns([
                            pl.col(col).cast(pl.Utf8).fill_null("").alias(col)
                        ])

                # 保存到临时文件，等待后续合并
                temp_file = sector_dir / "temp_concept_ths.parquet"
                combined_pl.write_parquet(temp_file)

                print(f"✅ 概念板块原始数据保存到临时文件: {len(combined_data)} 条记录")
                return True
            else:
                print("❌ 没有获取到任何概念数据")
                return False

        except Exception as e:
            print(f"❌ 更新概念板块数据失败: {e}")
            import traceback
            traceback.print_exc()
            return False

    def update_sector_constituents(self, sector_dir: Path) -> bool:
        """更新同花顺所有成分股数据"""
        try:
            print("🚀 开始更新同花顺成分股数据...")

            all_constituents = []
            concept_success = 0
            industry_success = 0

            # 直接使用AKShare获取概念指数列表
            print("📊 获取同花顺概念指数列表...")
            concepts_df = ak.stock_board_concept_name_ths()
            if concepts_df is not None and not concepts_df.empty:
                print(f"📊 更新同花顺概念板块成分股 ({len(concepts_df)} 个)...")
                for idx, row in concepts_df.iterrows():
                    code = row.get('code', '')
                    name = row.get('name', '')  # AKShare返回的是'name'列

                    if code and name:
                        print(f"  [{idx+1}/{len(concepts_df)}] {name}({code})")
                        stocks_df = self.get_sector_constituents(code, name, "概念")

                        if stocks_df is not None and not stocks_df.empty:
                            all_constituents.append(stocks_df)
                            concept_success += 1
                            print(f"    ✅ {len(stocks_df)} 只股票")
                        else:
                            print(f"    ❌ 获取失败")

                        # 每十个板块休息0.5秒
                        if (idx + 1) % 10 == 0:
                            print(f"  💤 已处理 {idx + 1} 个概念板块，休息0.5秒...")
                            import time
                            time.sleep(0.5)

            # 直接使用AKShare获取行业指数列表
            print("📊 获取同花顺行业指数列表...")
            industries_df = ak.stock_board_industry_name_ths()
            if industries_df is not None and not industries_df.empty:
                print(f"📊 更新同花顺行业板块成分股 ({len(industries_df)} 个)...")
                for idx, row in industries_df.iterrows():
                    # AKShare返回的列名是'code'和'name'
                    code = row.get('code', '')
                    name = row.get('name', '')

                    if code and name:
                        print(f"  [{idx+1}/{len(industries_df)}] {name}({code})")
                        stocks_df = self.get_sector_constituents(code, name, "行业")

                        if stocks_df is not None and not stocks_df.empty:
                            all_constituents.append(stocks_df)
                            industry_success += 1
                            print(f"    ✅ {len(stocks_df)} 只股票")
                        else:
                            print(f"    ❌ 获取失败")

                        # 每十个板块休息0.5秒
                        if (idx + 1) % 10 == 0:
                            print(f"  💤 已处理 {idx + 1} 个行业板块，休息0.5秒...")
                            import time
                            time.sleep(0.5)

            print(f"✅ 同花顺成分股更新完成: 概念{concept_success}, 行业{industry_success}")

            # 保存成分股数据
            if all_constituents:
                print("\n💾 保存同花顺成分股数据...")

                try:
                    # 合并所有成分股数据
                    combined_data = pd.concat(all_constituents, ignore_index=True)

                    # 确保股票代码是6位格式（不足的用0填充）
                    combined_data['股票代码'] = combined_data['股票代码'].astype(str).str.zfill(6)
                    
                    # 按板块名称和股票代码去重，保留第一个
                    combined_data = combined_data.drop_duplicates(subset=['板块名称', '股票代码'], keep='first')

                    # 添加数据源标识
                    combined_data['数据源'] = '同花顺'
                    combined_data['更新日期'] = datetime.now().strftime('%Y-%m-%d')

                    # 保存到Excel文件
                    output_file = sector_dir / "同花顺板块成分股.xlsx"

                    # 创建Excel写入器
                    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                        # 保存所有数据到"所有数据"工作表
                        combined_data.to_excel(writer, sheet_name='所有数据', index=False)

                        # 按板块类型分别保存
                        for sector_type in combined_data['板块类型'].unique():
                            type_data = combined_data[combined_data['板块类型'] == sector_type]
                            sheet_name = f"{sector_type}板块"
                            type_data.to_excel(writer, sheet_name=sheet_name, index=False)

                    print(f"✅ 同花顺成分股数据保存成功: {len(combined_data)} 条记录")
                    print(f"📁 保存位置: {output_file}")

                    return True

                except Exception as e:
                    print(f"❌ 保存同花顺成分股数据失败: {e}")
                    import traceback
                    traceback.print_exc()
                    return False
            else:
                print("❌ 没有获取到任何成分股数据")
                return False

        except Exception as e:
            print(f"❌ 同花顺成分股更新失败: {e}")
            import traceback
            traceback.print_exc()
            return False




class EastmoneyDataProvider:
    """东方财富数据提供器 - 专门处理东财数据源"""

    def __init__(self):
        self.source_name = "东方财富"

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

    def get_concept_constituents(self, symbol: str) -> Optional[pd.DataFrame]:
        """获取东财概念板块成分股"""
        try:
            print(f"📊 获取东财概念板块成分股: {symbol}")
            data = ak.stock_board_concept_cons_em(symbol=symbol)
            if data is not None and not data.empty:
                # 标准化数据
                current_date = datetime.now().date()

                # 确保有必要的列
                if '代码' in data.columns and '名称' in data.columns:
                    result_data = []
                    for _, row in data.iterrows():
                        result_data.append({
                            '股票代码': str(row['代码']).zfill(6),
                            '股票名称': row['名称'],
                            '板块名称': symbol,
                            '板块代码': '',  # 东财接口不提供板块代码
                            '板块类型': '概念',
                            '更新日期': current_date,
                            '数据源': '东方财富'
                        })

                    if result_data:
                        result_df = pd.DataFrame(result_data)
                        print(f"✅ 获取到 {len(result_df)} 只概念成分股")
                        return result_df

                print(f"⚠️ 概念板块 {symbol} 成分股数据格式异常")
                return None
            else:
                print(f"⚠️ 概念板块 {symbol} 无成分股数据")
                return None
        except Exception as e:
            print(f"❌ 获取概念板块成分股失败: {e}")
            return None

    def get_industry_constituents(self, symbol: str) -> Optional[pd.DataFrame]:
        """获取东财行业板块成分股"""
        try:
            print(f"📊 获取东财行业板块成分股: {symbol}")
            data = ak.stock_board_industry_cons_em(symbol=symbol)
            if data is not None and not data.empty:
                # 标准化数据
                current_date = datetime.now().date()

                # 确保有必要的列
                if '代码' in data.columns and '名称' in data.columns:
                    result_data = []
                    for _, row in data.iterrows():
                        result_data.append({
                            '股票代码': str(row['代码']).zfill(6),
                            '股票名称': row['名称'],
                            '板块名称': symbol,
                            '板块代码': '',  # 东财接口不提供板块代码
                            '板块类型': '行业',
                            '更新日期': current_date,
                            '数据源': '东方财富'
                        })

                    if result_data:
                        result_df = pd.DataFrame(result_data)
                        print(f"✅ 获取到 {len(result_df)} 只行业成分股")
                        return result_df

                print(f"⚠️ 行业板块 {symbol} 成分股数据格式异常")
                return None
            else:
                print(f"⚠️ 行业板块 {symbol} 无成分股数据")
                return None
        except Exception as e:
            print(f"❌ 获取行业板块成分股失败: {e}")
            return None

    def update_sector_data(self, start_date: str, end_date: str, sector_dir: Path) -> bool:
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

                for index, row in concepts_df.iterrows():
                    try:
                        symbol = row['板块代码']
                        name = row['板块名称']

                        # 东财概念板块API需要传递板块名称，不是代码
                        concept_data = self.get_concept_hist_data(name, start_date, end_date)
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

                for index, row in industries_df.iterrows():
                    try:
                        symbol = row['板块代码']
                        name = row['板块名称']

                        industry_data = self.get_industry_hist_data(symbol, start_date, end_date)
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

    def update_all_constituents(self, sector_dir: Path) -> bool:
        """更新东财所有成分股数据"""
        try:
            print("🚀 开始更新东财成分股数据...")

            all_constituents = []
            concept_success = 0
            industry_success = 0

            # 获取概念板块列表
            print("📊 获取东财概念板块名称...")
            try:
                concepts_df = ak.stock_board_concept_name_em()
                if concepts_df is not None and not concepts_df.empty:
                    print(f"✅ 获取到 {len(concepts_df)} 个东财概念板块")
                    print(f"📊 更新东财概念板块成分股 ({len(concepts_df)} 个)...")
                else:
                    print("⚠️ 东财概念板块数据为空")
                    concepts_df = None
            except Exception as e:
                print(f"❌ 获取东财概念板块名称失败: {e}")
                concepts_df = None

            if concepts_df is not None:
                for idx, row in concepts_df.iterrows():
                    name = row.get('板块名称', '')

                    if name:
                        print(f"  [{idx+1}/{len(concepts_df)}] {name}")
                        stocks_df = self.get_concept_constituents(name)

                        if stocks_df is not None and not stocks_df.empty:
                            all_constituents.append(stocks_df)
                            concept_success += 1
                            print(f"    ✅ {len(stocks_df)} 只股票")
                        else:
                            print(f"    ❌ 获取失败")

                        # 每十个板块休息0.5秒
                        if (idx + 1) % 10 == 0:
                            print(f"  💤 已处理 {idx + 1} 个东财概念板块，休息0.5秒...")
                            import time
                            time.sleep(0.5)

            # 获取行业板块列表
            print("📊 获取东财行业板块名称...")
            try:
                industries_df = ak.stock_board_industry_name_em()
                if industries_df is not None and not industries_df.empty:
                    print(f"✅ 获取到 {len(industries_df)} 个东财行业板块")
                    print(f"📊 更新东财行业板块成分股 ({len(industries_df)} 个)...")
                else:
                    print("⚠️ 东财行业板块数据为空")
                    industries_df = None
            except Exception as e:
                print(f"❌ 获取东财行业板块名称失败: {e}")
                industries_df = None

            if industries_df is not None:
                for idx, row in industries_df.iterrows():
                    name = row.get('板块名称', '')

                    if name:
                        print(f"  [{idx+1}/{len(industries_df)}] {name}")
                        stocks_df = self.get_industry_constituents(name)

                        if stocks_df is not None and not stocks_df.empty:
                            all_constituents.append(stocks_df)
                            industry_success += 1
                            print(f"    ✅ {len(stocks_df)} 只股票")
                        else:
                            print(f"    ❌ 获取失败")

                        # 每十个板块休息0.5秒
                        if (idx + 1) % 10 == 0:
                            print(f"  💤 已处理 {idx + 1} 个东财行业板块，休息0.5秒...")
                            import time
                            time.sleep(0.5)

            print(f"✅ 东财成分股更新完成: 概念{concept_success}, 行业{industry_success}")

            # 保存成分股数据
            if all_constituents:
                print("\n💾 保存东财成分股数据...")

                try:
                    # 合并所有成分股数据
                    combined_data = pd.concat(all_constituents, ignore_index=True)

                    # 确保股票代码是6位格式（不足的用0填充）
                    combined_data['股票代码'] = combined_data['股票代码'].astype(str).str.zfill(6)
                    
                    # 按板块名称和股票代码去重，保留第一个
                    combined_data = combined_data.drop_duplicates(subset=['板块名称', '股票代码'], keep='first')

                    # 添加数据源标识
                    combined_data['数据源'] = '东方财富'
                    combined_data['更新日期'] = datetime.now().strftime('%Y-%m-%d')

                    # 保存到Excel文件
                    output_file = sector_dir / "东财板块成分股.xlsx"

                    # 创建Excel写入器
                    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                        # 保存所有数据到"所有数据"工作表
                        combined_data.to_excel(writer, sheet_name='所有数据', index=False)

                        # 按板块类型分别保存
                        for sector_type in combined_data['板块类型'].unique():
                            type_data = combined_data[combined_data['板块类型'] == sector_type]
                            sheet_name = f"{sector_type}板块"
                            type_data.to_excel(writer, sheet_name=sheet_name, index=False)

                    print(f"✅ 东财成分股数据保存成功: {len(combined_data)} 条记录")
                    print(f"📁 保存位置: {output_file}")

                    return True

                except Exception as e:
                    print(f"❌ 保存东财成分股数据失败: {e}")
                    import traceback
                    traceback.print_exc()
                    return False
            else:
                print("❌ 没有获取到任何成分股数据")
                return False

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

    def is_latest_trading_day(self) -> bool:
        """检查板块数据是否是最新交易日的数据

        逻辑：
        1. 获取现有数据的最新日期
        2. 获取当前应该更新到的最新交易日期
        3. 判断当天是否为交易日，是否已过18:00
        4. 考虑周末和节假日的影响
        """
        try:
            # 1. 获取现有数据的最新日期
            df = self.load_sector_data()
            if df.is_empty():
                print("板块数据为空，需要更新")
                return False

            if '日期' not in df.columns:
                print("警告: 板块数据中缺少日期列")
                return False

            # 解析现有数据的最新日期
            latest_date_raw = df['日期'].max()
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
                print("✅ 板块数据已是最新，无需更新")
            else:
                print("📊 板块数据需要更新")

            return is_latest

        except Exception as e:
            print(f"❌ 检查是否为最新交易日失败: {e}")
            return False

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

    def update_sector_data(self, source: str = None, start_date: str = None, end_date: str = None) -> bool:
        """使用指定数据源更新板块数据"""
        if source is None:
            source = self.preferred_source

        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        print(f"📊 使用{source}数据源更新板块数据 ({start_date} - {end_date})")

        all_data = []
        success = False

        if source == "ths":
            # 使用同花顺数据源更新板块数据
            try:
                print("📊 使用同花顺数据源更新板块数据...")
                # 调用同花顺数据提供器的板块数据更新方法
                # update_all_sectors只接受sector_dir参数，用于更新成分股数据
                # 对于板块历史数据，使用update_sector_data方法
                success = self.ths_provider.update_sector_data(self.sector_dir, years_back=1, force_update=False)

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
                            # 先统一为字符串再宽松解析为日期
                            cast_exprs.append(
                                pl.col('日期')
                                .cast(pl.Utf8)
                                .str.strptime(pl.Date, strict=False)
                                .alias('日期')
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
                            historical_columns = set(historical_data.columns)
                            new_columns = set(new_data_with_indicators.columns)
                            common_columns = list(historical_columns.intersection(new_columns))

                            print(f"📊 共同列数: {len(common_columns)}")

                            # 在重排列之前，先将新数据列类型对齐到历史数据的schema
                            try:
                                target_schema = historical_data.schema
                                align_exprs = []
                                for col in common_columns:
                                    try:
                                        target_dtype = target_schema.get(col)
                                        if target_dtype is not None:
                                            # 对齐为目标类型
                                            align_exprs.append(pl.col(col).cast(target_dtype).alias(col))
                                    except Exception:
                                        pass
                                if align_exprs:
                                    new_data_with_indicators = new_data_with_indicators.with_columns(align_exprs)
                            except Exception as _e:
                                print(f"⚠️ 列类型对齐时出错: {_e}")

                            # 重新排序列，确保一致性
                            historical_data_aligned = historical_data.select(common_columns)
                            new_data_aligned = new_data_with_indicators.select(common_columns)

                            # 合并数据
                            unified_data = pl.concat([historical_data_aligned, new_data_aligned])

                            # 按日期、板块名称去重，保留最新的数据
                            unified_data = unified_data.unique(subset=['日期', '板块名称','板块类型'], keep='last')
                            print(f"📊 去重后数据: {unified_data.height} 条记录")
                        else:
                            print("� 创建新的数据文件")
                            unified_data = new_data_with_indicators

                        # 保存合并后的数据
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
                success = self.eastmoney_provider.update_sector_data(start_date, end_date, self.sector_dir)

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

        return success


    def _calculate_technical_indicators(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算技术指标"""
        try:
            print("📊 计算技术指标...")

            # 按板块名称分组计算
            result_dfs = []

            # 确保关键基础列存在，避免后续计算报错
            base_numeric_defaults = {
                '成交额': 0.0,
            }
            for base_col, default_val in base_numeric_defaults.items():
                if base_col not in df.columns:
                    df = df.with_columns([pl.lit(float(default_val)).alias(base_col)])

            for sector_name in df['板块名称'].unique():
                sector_df = df.filter(pl.col('板块名称') == sector_name).sort('日期')

                # 需要统一补齐的技术指标列（全部为浮点）
                indicator_numeric_columns = [
                    '涨跌幅', '振幅', '换手率', '5日涨跌幅', '10日涨跌幅',
                    'MA5', 'MA10', 'MA20', '成交额量比', '连阳天数'
                ]

                if sector_df.height < 2:
                    # 行数过少，直接补齐技术指标为0.0，确保列齐全且类型一致
                    sector_df = sector_df.with_columns([
                        pl.lit(0.0).alias('涨跌幅'),
                        pl.lit(0.0).alias('振幅'),
                        pl.lit(0.0).alias('换手率'),
                        pl.lit(0.0).alias('5日涨跌幅'),
                        pl.lit(0.0).alias('10日涨跌幅'),
                        pl.lit(0.0).alias('MA5'),
                        pl.lit(0.0).alias('MA10'),
                        pl.lit(0.0).alias('MA20'),
                        pl.lit(0.0).alias('成交额量比'),
                        pl.lit(0).alias('连阳天数'),
                    ])

                    # 强制为Float64，避免concat时出现 Null dtype（连阳天数为整数类型）
                    sector_df = sector_df.with_columns([
                        pl.col(c).cast(pl.Float64).alias(c) if c != '连阳天数' else pl.col(c).cast(pl.Int64).alias(c)
                        for c in indicator_numeric_columns
                    ])

                    result_dfs.append(sector_df)
                    continue

                # 计算涨跌幅
                sector_df = sector_df.with_columns([
                    # 当日涨跌幅 = (收盘价 - 开盘价) / 开盘价 * 100
                    ((pl.col('收盘') - pl.col('开盘')) / pl.col('开盘') * 100).alias('涨跌幅'),

                    # 振幅 = (最高价 - 最低价) / 开盘价 * 100
                    ((pl.col('最高') - pl.col('最低')) / pl.col('开盘') * 100).alias('振幅'),

                    # 换手率（暂时设为0，需要流通股本数据）
                    pl.lit(0.0).alias('换手率')
                ])

                # 计算5日和10日涨跌幅（需要足够的历史数据）
                if sector_df.height >= 5:
                    # 5日涨跌幅 = (当前收盘价 - 5日前收盘价) / 5日前收盘价 * 100
                    sector_df = sector_df.with_columns([
                        ((pl.col('收盘') - pl.col('收盘').shift(5)) / pl.col('收盘').shift(5) * 100).alias('5日涨跌幅')
                    ])
                else:
                    sector_df = sector_df.with_columns([pl.lit(0.0).alias('5日涨跌幅')])

                if sector_df.height >= 10:
                    # 10日涨跌幅 = (当前收盘价 - 10日前收盘价) / 10日前收盘价 * 100
                    sector_df = sector_df.with_columns([
                        ((pl.col('收盘') - pl.col('收盘').shift(10)) / pl.col('收盘').shift(10) * 100).alias('10日涨跌幅')
                    ])
                else:
                    sector_df = sector_df.with_columns([pl.lit(0.0).alias('10日涨跌幅')])

                # 计算移动平均线 - 确保数据类型一致
                if sector_df.height >= 5:
                    sector_df = sector_df.with_columns([
                        pl.col('收盘').rolling_mean(window_size=5).alias('MA5')
                    ])
                else:
                    sector_df = sector_df.with_columns([pl.lit(0.0).alias('MA5')])

                if sector_df.height >= 10:
                    sector_df = sector_df.with_columns([
                        pl.col('收盘').rolling_mean(window_size=10).alias('MA10')
                    ])
                else:
                    sector_df = sector_df.with_columns([pl.lit(0.0).alias('MA10')])

                if sector_df.height >= 20:
                    sector_df = sector_df.with_columns([
                        pl.col('收盘').rolling_mean(window_size=20).alias('MA20')
                    ])
                else:
                    sector_df = sector_df.with_columns([pl.lit(0.0).alias('MA20')])

                # 计算成交额量比（需要历史平均成交额）
                if sector_df.height >= 5:
                    # 成交额量比 = 当日成交额 / 5日平均成交额
                    sector_df = sector_df.with_columns([
                        (pl.col('成交额') / pl.col('成交额').rolling_mean(window_size=5)).alias('成交额量比')
                    ])
                else:
                    sector_df = sector_df.with_columns([pl.lit(0.0).alias('成交额量比')])

                # 计算连阳天数（连续收盘价大于开盘价的天数）
                if sector_df.height >= 1:
                    # 判断当日是否为阳线（收盘价 > 开盘价）
                    sector_df = sector_df.with_columns([
                        (pl.col('收盘') > pl.col('开盘')).alias('is_positive_day')
                    ])
                    
                    # 计算连阳天数
                    def calculate_consecutive_positive_days(is_positive_series):
                        """计算连续阳线天数"""
                        consecutive_days = 0
                        result = []
                        
                        for is_positive in is_positive_series:
                            if is_positive:
                                consecutive_days += 1
                            else:
                                consecutive_days = 0
                            result.append(consecutive_days)
                        
                        return result
                    
                    # 应用连阳天数计算
                    is_positive_list = sector_df['is_positive_day'].to_list()
                    consecutive_days_list = calculate_consecutive_positive_days(is_positive_list)
                    
                    sector_df = sector_df.with_columns([
                        pl.Series(consecutive_days_list).alias('连阳天数')
                    ])
                else:
                    sector_df = sector_df.with_columns([pl.lit(0).alias('连阳天数')])

                # 确保所有指标列存在并统一类型（连阳天数为整数，其他为浮点）
                sector_df = sector_df.with_columns([
                    pl.col(c).cast(pl.Float64).alias(c) if c in sector_df.columns and c != '连阳天数' else 
                    pl.col(c).cast(pl.Int64).alias(c) if c == '连阳天数' and c in sector_df.columns else
                    pl.lit(0.0).cast(pl.Float64).alias(c) if c != '连阳天数' else
                    pl.lit(0).cast(pl.Int64).alias(c)
                    for c in indicator_numeric_columns
                ])

                result_dfs.append(sector_df)

            # 合并所有结果
            if result_dfs:
                # 确保所有DataFrame的列完全一致
                all_columns = set()
                for df_item in result_dfs:
                    all_columns.update(df_item.columns)

                # 定义不同类型的列集合，防止误将文本列转为浮点
                numeric_price_cols = {'开盘', '收盘', '最高', '最低', '成交量', '成交额', '换手率', '涨跌幅', '振幅', '5日涨跌幅', '10日涨跌幅', 'MA5', 'MA10', 'MA20', '成交额量比'}
                int_cols = {'连阳天数'}
                bool_cols = {'is_positive_day'}
                date_cols = {'日期'}
                text_cols = {'板块名称', '板块类型', '板块代码', '数据源'}

                # 为每个DataFrame补齐缺失的列并统一数据类型（仅对已知列做强制类型）
                standardized_dfs = []
                for df_item in result_dfs:
                    # 补齐缺失列
                    for col in all_columns:
                        if col not in df_item.columns:
                            if col in int_cols:
                                df_item = df_item.with_columns([pl.lit(0).cast(pl.Int64).alias(col)])
                            elif col in bool_cols:
                                df_item = df_item.with_columns([pl.lit(False).cast(pl.Boolean).alias(col)])
                            elif col in date_cols:
                                df_item = df_item.with_columns([pl.lit(None).cast(pl.Date).alias(col)])
                            elif col in text_cols:
                                df_item = df_item.with_columns([pl.lit("").cast(pl.Utf8).alias(col)])
                            elif col in numeric_price_cols:
                                df_item = df_item.with_columns([pl.lit(0.0).cast(pl.Float64).alias(col)])
                            else:
                                # 未知列：保持缺省不强制设类型，给个空字符串/0.0更安全？此处保持为空列以避免错误
                                df_item = df_item.with_columns([pl.lit(None).alias(col)])

                    # 统一现有列类型
                    unify_exprs = []
                    for col in all_columns:
                        if col in int_cols:
                            unify_exprs.append(pl.col(col).cast(pl.Int64).alias(col))
                        elif col in bool_cols:
                            unify_exprs.append(pl.col(col).cast(pl.Boolean).alias(col))
                        elif col in date_cols:
                            # 宽松解析到Date，不直接访问dtype
                            unify_exprs.append(
                                pl.coalesce([
                                    pl.col(col).cast(pl.Utf8).str.strptime(pl.Date, strict=False),
                                    pl.col(col).cast(pl.Date)
                                ]).alias(col)
                            )
                        elif col in text_cols:
                            unify_exprs.append(pl.col(col).cast(pl.Utf8).fill_null("").alias(col))
                        elif col in numeric_price_cols:
                            unify_exprs.append(pl.col(col).cast(pl.Float64).alias(col))
                        else:
                            # 未知列：不改变其类型
                            unify_exprs.append(pl.col(col).alias(col))

                    df_item = df_item.with_columns(unify_exprs)

                    # 统一列顺序
                    df_item = df_item.select(sorted(all_columns))
                    standardized_dfs.append(df_item)

                # 现在可以安全拼接
                final_df = pl.concat(standardized_dfs)
            else:
                final_df = df

            print(f"✅ 技术指标计算完成，处理了 {len(result_dfs)} 个板块")
            return final_df

        except Exception as e:
            print(f"❌ 计算技术指标失败: {e}")
            import traceback
            traceback.print_exc()
            return df  # 返回原始数据

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
                for i, row in enumerate(data.to_dicts()):
                    sector_name = row.get(sector_col)
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

            # 首先尝试从缓存获取数据
            constituents_pl = self._get_cached_constituents(source)
            
            if constituents_pl is None:
                # 缓存未命中，从文件加载数据
                if source == "ths" and self.ths_constituents_file.exists():
                    print(f"📊 使用同花顺成分股数据: {self.ths_constituents_file}")
                    constituents_data = pd.read_excel(self.ths_constituents_file, sheet_name='所有数据')
                    constituents_pl = pl.from_pandas(constituents_data)
                    self._cache_constituents(source, constituents_pl)
                elif source == "eastmoney" and self.dc_constituents_file.exists():
                    print(f"📊 使用东财成分股数据: {self.dc_constituents_file}")
                    # 东财成分股文件使用Sheet1
                    constituents_data = pd.read_excel(self.dc_constituents_file, sheet_name='Sheet1')
                    constituents_pl = pl.from_pandas(constituents_data)
                    self._cache_constituents(source, constituents_pl)
                else:
                    # 如果指定数据源不可用，尝试其他数据源
                    if self.ths_constituents_file.exists():
                        print(f"📊 回退到同花顺成分股数据: {self.ths_constituents_file}")
                        constituents_data = pd.read_excel(self.ths_constituents_file, sheet_name='所有数据')
                        constituents_pl = pl.from_pandas(constituents_data)
                        self._cache_constituents("ths", constituents_pl)
                    elif self.dc_constituents_file.exists():
                        print(f"📊 回退到东财成分股数据: {self.dc_constituents_file}")
                        constituents_data = pd.read_excel(self.dc_constituents_file, sheet_name='Sheet1')
                        constituents_pl = pl.from_pandas(constituents_data)
                        self._cache_constituents("eastmoney", constituents_pl)
                    else:
                        print("⚠️ 没有找到成分股数据文件")
                        return None

            # 筛选指定板块的成分股 - 使用索引优化
            cache_key = f"constituents_{source}"
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
