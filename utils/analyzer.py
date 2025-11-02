import polars as pl
from typing import Dict, List, Tuple
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np
from collections import defaultdict





class MarketAnalyzer:
    @staticmethod
    def analyze_market_sentiment(sentiment_data: dict, previous_data: dict = None) -> dict:
        """分析市场情绪，支持与上个交易日对比"""
        # 获取各种数据框
        limit_up_df = sentiment_data.get('limit_up', pl.DataFrame())
        limit_down_df = sentiment_data.get('limit_down', pl.DataFrame())
        market_df = sentiment_data.get('market_overview', pl.DataFrame())
        strong_stocks_df = sentiment_data.get('strong_stocks', pl.DataFrame())
        previous_limit_up_df = sentiment_data.get('previous_limit_up', pl.DataFrame())
        break_limit_up_df = sentiment_data.get('break_limit_up', pl.DataFrame())
        big_deal_df = sentiment_data.get('big_deal', pl.DataFrame())
        
        # 默认值
        result = {
            'limit_up_count': 0,
            'limit_down_count': 0,
            'up_count': 0,
            'down_count': 0,
            'flat_count': 0,
            'strong_stocks_count': 0,
            'previous_limit_up_count': 0,
            'break_limit_up_count': 0,
            'big_deal_count': 0,
            'red_ratio': 0,
            'break_ratio': 0,
            'total_amount': 0,
            # 新增：与上个交易日的变化
            'changes': {
                'limit_up_change': 0,
                'limit_down_change': 0,
                'red_ratio_change': 0,
                'total_amount_change': 0,
                'total_amount_change_pct': 0
            }
        }
        
        # 更新各个计数
        result['limit_up_count'] = limit_up_df.height if not limit_up_df.is_empty() else 0
        result['limit_down_count'] = limit_down_df.height if not limit_down_df.is_empty() else 0
        result['strong_stocks_count'] = strong_stocks_df.height if not strong_stocks_df.is_empty() else 0
        result['previous_limit_up_count'] = previous_limit_up_df.height if not previous_limit_up_df.is_empty() else 0
        result['break_limit_up_count'] = break_limit_up_df.height if not break_limit_up_df.is_empty() else 0
        result['big_deal_count'] = big_deal_df.height if not big_deal_df.is_empty() else 0
        
        # 如果市场概览数据为空，直接返回默认结果
        if market_df.is_empty():
            return result
            
        # 找到涨跌幅列
        change_col = '涨跌幅'
        if change_col not in market_df.columns:
            # 尝试其他可能的列名
            for col in ['change_pct', 'pct_change']:
                if col in market_df.columns:
                    change_col = col
                    break
            else:
                # 如果找不到涨跌幅列，返回默认结果
                return result
        
        try:
            # 计算涨跌家数
            up_count = market_df.filter(pl.col(change_col) > 0).height
            down_count = market_df.filter(pl.col(change_col) < 0).height
            total_count = market_df.height
            
            result['up_count'] = up_count
            result['down_count'] = down_count
            result['flat_count'] = total_count - up_count - down_count
            
            # 计算红盘率（保留两位小数）
            result['red_ratio'] = round((up_count / total_count * 100), 2) if total_count > 0 else 0.00

            # 计算涨跌幅分布
            change_distribution = MarketAnalyzer._calculate_change_distribution(market_df, change_col)
            result['change_distribution'] = change_distribution

            # 计算炸板率（保留两位小数）
            limit_up_count = result['limit_up_count']
            break_count = result['break_limit_up_count']
            result['break_ratio'] = round((break_count / (break_count + limit_up_count) * 100), 2) if (break_count + limit_up_count) > 0 else 0.00

            # 找到成交额列
            amount_col = '成交额'
            if amount_col not in market_df.columns:
                # 尝试其他可能的列名
                for col in ['amount', 'trade_amount', '成交金额', '总市值']:
                    if col in market_df.columns:
                        amount_col = col
                        break
                else:
                    # 如果找不到成交额列，尝试计算沪深两市总成交额
                    print("未找到成交额列，尝试获取沪深两市成交额")
                    try:
                        # 获取沪深两市成交额
                        import akshare as ak
                        from datetime import datetime
                        today = datetime.now().strftime('%Y%m%d')

                        # 获取沪深两市成交额
                        sh_amount = 0
                        sz_amount = 0
                        try:
                            # 获取上证指数成交额
                            sh_data = ak.index_zh_a_hist(symbol="000001", period="daily", start_date=today, end_date=today)
                            if not sh_data.empty:
                                sh_amount = sh_data['成交额'].iloc[-1] if '成交额' in sh_data.columns else 0
                        except:
                            pass

                        try:
                            # 获取深证成指成交额
                            sz_data = ak.index_zh_a_hist(symbol="399001", period="daily", start_date=today, end_date=today)
                            if not sz_data.empty:
                                sz_amount = sz_data['成交额'].iloc[-1] if '成交额' in sz_data.columns else 0
                        except:
                            pass

                        result['total_amount'] = round((sh_amount + sz_amount) / 100000000, 2)  # 转换为亿元
                        return result
                    except Exception as e:
                        print(f"获取沪深两市成交额失败: {str(e)}")
                        result['total_amount'] = 0.00
                        return result

            # 计算市场量能（保留两位小数）
            result['total_amount'] = round(market_df[amount_col].sum() / 100000000, 2)  # 转换为亿元
        
        except Exception as e:
            print(f"分析市场情绪时出错: {str(e)}")

        # 计算与上个交易日的变化
        if previous_data:
            try:
                # 涨停数变化
                prev_limit_up = previous_data.get('limit_up_count', 0)
                result['changes']['limit_up_change'] = result['limit_up_count'] - prev_limit_up

                # 跌停数变化
                prev_limit_down = previous_data.get('limit_down_count', 0)
                result['changes']['limit_down_change'] = result['limit_down_count'] - prev_limit_down

                # 红盘率变化
                prev_red_ratio = previous_data.get('red_ratio', 0)
                result['changes']['red_ratio_change'] = round(result['red_ratio'] - prev_red_ratio, 2)

                # 成交额变化
                prev_total_amount = previous_data.get('total_amount', 0)
                result['changes']['total_amount_change'] = round(result['total_amount'] - prev_total_amount, 2)

                # 成交额变化百分比
                if prev_total_amount > 0:
                    result['changes']['total_amount_change_pct'] = round(
                        (result['total_amount'] - prev_total_amount) / prev_total_amount * 100, 2
                    )
                else:
                    result['changes']['total_amount_change_pct'] = 0

            except Exception as e:
                print(f"计算市场情绪变化时出错: {str(e)}")

        return result
    
    @staticmethod
    def analyze_market_history(market_data: pl.DataFrame, days: int = 30) -> dict:
        """分析历史市场数据"""
        # 确保数据按日期排序
        # 检查列名并进行标准化
        column_mapping = {
            'date': 'date',
            '日期': 'date',
            'trade_date': 'date',
            'trading_date': 'date',
            '成交额': 'amount',
            'amount': 'amount',
            'trade_amount': 'amount',
            '涨跌幅': 'change_pct',
            'change_pct': 'change_pct',
            'pct_change': 'change_pct'
        }
        
        # 重命名列
        renamed_cols = []
        for col in market_data.columns:
            if col in column_mapping:
                renamed_cols.append(pl.col(col).alias(column_mapping[col]))
            else:
                renamed_cols.append(col)
        
        market_data = market_data.select(renamed_cols)
        
        # 确保日期格式正确
        if market_data['date'].dtype == pl.Utf8:
            market_data = market_data.with_columns([
                pl.col('date').str.strptime(pl.Date, '%Y-%m-%d').alias('date')
            ])
        
        market_data = market_data.sort('date')
        
        # 获取最近N天的数据
        recent_data = market_data.tail(days)
        
        # 计算每日红盘率
        daily_stats = []
        for date in recent_data['date'].unique():
            day_data = recent_data.filter(pl.col('date') == date)
            up_count = day_data.filter(pl.col('change_pct') > 0).height
            total_count = day_data.height
            red_ratio = (up_count / total_count * 100) if total_count > 0 else 0
            
            # 计算当日成交额
            total_amount = day_data['amount'].sum() / 100000000  # 转换为亿元
            
            daily_stats.append({
                'date': date,
                'red_ratio': red_ratio,
                'total_amount': total_amount
            })
        
        # 计算历史平均值
        avg_red_ratio = sum(d['red_ratio'] for d in daily_stats) / len(daily_stats)
        avg_amount = sum(d['total_amount'] for d in daily_stats) / len(daily_stats)
        
        return {
            'daily_stats': daily_stats,
            'avg_red_ratio': avg_red_ratio,
            'avg_amount': avg_amount
        }
    
    @staticmethod
    def analyze_limit_up_history(limit_up_data: dict, days: int = 30) -> dict:
        """分析历史涨停数据"""
        # 提取每日涨停和炸板数据
        daily_stats = []
        dates = limit_up_data.get('dates', [])
        limit_up_counts = limit_up_data.get('limit_up_counts', [])
        break_counts = limit_up_data.get('break_counts', [])
        
        for i in range(min(days, len(dates))):
            daily_stats.append({
                'date': dates[i],
                'limit_up_count': limit_up_counts[i],
                'break_count': break_counts[i],
                'break_ratio': (break_counts[i] / (break_counts[i] + limit_up_counts[i]) * 100) 
                              if (break_counts[i] + limit_up_counts[i]) > 0 else 0
            })
        
        # 计算历史平均值
        avg_limit_up = sum(d['limit_up_count'] for d in daily_stats) / len(daily_stats)
        avg_break_ratio = sum(d['break_ratio'] for d in daily_stats) / len(daily_stats)
        
        return {
            'daily_stats': daily_stats,
            'avg_limit_up': avg_limit_up,
            'avg_break_ratio': avg_break_ratio
        }
    
    @staticmethod
    def analyze_sectors(sector_data: dict) -> Dict[str, List[dict]]:
        """分析行业板块表现"""
        # 直接返回从 get_sectors_summary 获取的数据
        return sector_data
    
    @staticmethod
    def analyze_concepts(concept_data: dict) -> Dict[str, List[dict]]:
        """分析概念板块表现"""
        # 直接返回从 get_concepts_summary 获取的数据
        return concept_data
    
    @staticmethod
    def analyze_concept_status(concept_pl, market_states):
        """
        分析涨停板块并识别龙头、中军、后排
        """
        
        # 判断涨停类型的函数
        def get_limit_type(code):
            """根据股票代码判断涨停类型"""
            if code.startswith('30') or code.startswith('68'):
                return '20cm'
            elif code.startswith('8') or code.startswith('4') or code.startswith('9'):
                return '30cm'
            else:
                return '10cm'
        
        # 假设market_states有以下列：日期、代码、名称、涨停、成交额、换手率、涨停时间
        # 获取涨停股票
        limit_up_data = market_states.filter(
            (pl.col('涨停') == True) & ( ~pl.col('名称').str.contains('ST'))
        )

        # 合并概念信息
        merged_data = limit_up_data.join(
            concept_pl.select(['代码', '概念']),  # 只选择代码和概念列
            on='代码',  # 只用代码join
            how='inner'
        )
        # 添加涨停类型
        merged_data = merged_data.with_columns(
            pl.col('代码').map_elements(get_limit_type, return_dtype=pl.Utf8).alias('涨停类型')
        )
        
        # 按日期和概念分组，统计涨停数
        concept_stats = merged_data.group_by(['日期', '概念']).agg([
            pl.count().alias('涨停数')
        ])
        # 找出曾经涨停数 > 3 的概念
        hot_concept_names = concept_stats.filter(pl.col('涨停数') > 3).select('概念').unique()

        # 获取这些概念的所有日期数据
        hot_concept_stocks = merged_data.join(
            hot_concept_names,
            on='概念',
            how='inner'
        )
        # print(hot_concept_stocks)
        # 分析每个热门概念的龙头、中军、后排
        result_list = []
        
        for date in hot_concept_stocks['日期'].unique().to_list():
            date_data = hot_concept_stocks.filter(pl.col('日期') == date)
            
            for concept in date_data['概念'].unique().to_list():
                concept_data = date_data.filter(pl.col('概念') == concept)
                sorted_stocks = concept_data.sort('成交额', descending=True).to_pandas()
                total_stocks = len(sorted_stocks)
                
                # 龙头
                leader_count = max(1, min(2, int(total_stocks * 0.2)))
                leaders = sorted_stocks.head(leader_count)
                
                for _, stock in leaders.iterrows():
                    result_list.append({
                        '日期': date,
                        '概念': concept,
                        '角色': '龙头',
                        '涨停类型': stock['涨停类型'],
                        '代码': stock['代码'],
                        '名称': stock['名称'],
                        '成交额': stock['成交额'],
                        '排名': leaders.index.get_loc(stock.name) + 1
                    })
                
                # 中军
                if total_stocks >= 5:
                    middle_start = leader_count
                    middle_count = max(1, int(total_stocks * 0.3))
                    middle_stocks = sorted_stocks.iloc[middle_start:middle_start + middle_count]
                    
                    for i, (_, stock) in enumerate(middle_stocks.iterrows()):
                        result_list.append({
                            '日期': date,
                            '概念': concept,
                            '角色': '中军',
                            '涨停类型': stock['涨停类型'],
                            '代码': stock['代码'],
                            '名称': stock['名称'],
                            '成交额': stock['成交额'],
                            '排名': middle_start + i + 1
                        })
                
                # 后排
                rear_start = leader_count + (middle_count if total_stocks >= 5 else 0)
                if rear_start < total_stocks:
                    rear_stocks = sorted_stocks.iloc[rear_start:]
                    
                    for i, (_, stock) in enumerate(rear_stocks.iterrows()):
                        result_list.append({
                            '日期': date,
                            '概念': concept,
                            '角色': '后排',
                            '涨停类型': stock['涨停类型'],
                            '代码': stock['代码'],
                            '名称': stock['名称'],
                            '成交额': stock['成交额'],
                            '排名': rear_start + i + 1
                        })
        
        return pd.DataFrame(result_list)
 
    @staticmethod
    def analyze_limit_up_details(sentiment_data: dict) -> Dict[str, List[dict]]:
        """分析涨停板详细信息"""
        # 处理强势股数据
        strong_stocks = sentiment_data['strong_stocks']
        if not strong_stocks.is_empty():
            # 确保选择正确的列名
            cols_to_select = []
            for col in ['代码', '股票代码', '名称', '股票简称', '涨跌幅', '最新价', '成交价格', '换手率']:
                if col in strong_stocks.columns:
                    cols_to_select.append(col)
            strong_stocks = strong_stocks.head(10).select(cols_to_select)
        
        # 处理昨日涨停股数据
        previous_limit_up = sentiment_data['previous_limit_up']
        if not previous_limit_up.is_empty():
            cols_to_select = []
            for col in ['代码', '股票代码', '名称', '股票简称', '涨跌幅', '最新价', '成交价格', '换手率']:
                if col in previous_limit_up.columns:
                    cols_to_select.append(col)
            previous_limit_up = previous_limit_up.head(10).select(cols_to_select)
        
        # 处理炸板股数据
        break_limit_up = sentiment_data['break_limit_up']
        if not break_limit_up.is_empty():
            cols_to_select = []
            for col in ['代码', '股票代码', '名称', '股票简称', '涨跌幅', '最新价', '成交价格', '换手率']:
                if col in break_limit_up.columns:
                    cols_to_select.append(col)
            break_limit_up = break_limit_up.head(10).select(cols_to_select)
        
        # 处理大单交易数据
        big_deal = sentiment_data['big_deal']
        if not big_deal.is_empty():
            cols_to_select = []
            for col in ['股票代码', '股票简称', '成交价格', '成交量', '成交额', '大单性质', '涨跌幅', '涨跌额']:
                if col in big_deal.columns:
                    cols_to_select.append(col)
            big_deal = big_deal.head(10).select(cols_to_select)
        
        return {
            'strong_stocks': strong_stocks.to_dicts(),
            'previous_limit_up': previous_limit_up.to_dicts(),
            'break_limit_up': break_limit_up.to_dicts(),
            'big_deal': big_deal.to_dicts()
        }
    
    @staticmethod
    def analyze_model_one_stocks(previous_limit_up_data: pl.DataFrame, stock_details: dict) -> List[dict]:
        """模型一选股策略"""
        # 检查数据框是否为空
        if previous_limit_up_data.is_empty():
            print("昨日涨停股数据为空，无法进行模型一选股")
            return []
            
        print(f"模型一选股开始，可用列: {previous_limit_up_data.columns}")
        
        # 定义筛选条件
        filter_conditions = []
        
        # 检查所需列是否存在
        if '换手率' in previous_limit_up_data.columns:
            filter_conditions.append((pl.col('换手率') >= 5))
            filter_conditions.append((pl.col('换手率') <= 30))
        
        # 检查委比列是否存在
        if '委比' in previous_limit_up_data.columns:
            filter_conditions.append((pl.col('委比') > 0))
        
        # 检查量比列是否存在
        if '量比' in previous_limit_up_data.columns:
            filter_conditions.append((pl.col('量比') > 1))
        
        # 检查主力净量列是否存在
        if '主力净量' in previous_limit_up_data.columns:
            filter_conditions.append((pl.col('主力净量') > 0))
        
        # 检查名称列是否存在
        name_col = None
        for col in ['名称', '股票简称']:
            if col in previous_limit_up_data.columns:
                name_col = col
                filter_conditions.append((~pl.col(name_col).str.contains('ST')))
                break
        
        # 检查代码列是否存在
        code_col = None
        for col in ['代码', '股票代码']:
            if col in previous_limit_up_data.columns:
                code_col = col
                filter_conditions.append((~pl.col(code_col).str.contains('^688|^300|^301')))
                break
        
        # 如果没有代码或名称列，无法继续
        if code_col is None:
            print("数据中没有可用的股票代码列")
            return []
        
        # 如果没有筛选条件，使用基本条件
        if not filter_conditions:
            print("没有可用的筛选条件，使用基本筛选")
            # 只筛选非科创板和创业板
            if code_col:
                filter_conditions.append((~pl.col(code_col).str.contains('^688|^300|^301')))
            # 如果有名称列，排除ST股
            if name_col:
                filter_conditions.append((~pl.col(name_col).str.contains('ST')))
        
        # 使用存在的筛选条件进行过滤
        try:
            if filter_conditions:
                combined_condition = filter_conditions[0]
                for condition in filter_conditions[1:]:
                    combined_condition = combined_condition & condition
                
                filtered_stocks = previous_limit_up_data.filter(combined_condition)
            else:
                # 如果没有筛选条件，直接使用原始数据
                filtered_stocks = previous_limit_up_data
                
            print(f"第一步筛选后剩余股票数: {filtered_stocks.height}")
        except Exception as e:
            print(f"筛选股票时出错: {str(e)}")
            # 如果筛选出错，返回原始数据
            filtered_stocks = previous_limit_up_data
        
        # 转换为字典列表
        filtered_stocks_dicts = filtered_stocks.to_dicts()
        
        # 第二步筛选：技术面和形态过滤
        # 如果没有股票详细数据，跳过这一步
        if not stock_details:
            print("没有股票详细数据，跳过技术面和形态筛选")
            return filtered_stocks_dicts[:20]  # 最多返回20只股票
        
        # 第二步筛选：技术面和形态过滤
        result_stocks = []
        for stock in filtered_stocks_dicts:
            code = stock.get(code_col)
            if not code:
                continue
                
            # 获取个股详细数据
            stock_data = stock_details.get(code)
            if stock_data is None or stock_data.is_empty():
                continue
                
            # 获取最近交易日数据
            recent_data = stock_data.tail(10)  # 获取最近10个交易日数据
            if recent_data.height < 3:
                continue
                
            # 检查涨跌幅列是否存在
            change_col = None
            for col in ['涨跌幅', 'change_pct']:
                if col in stock:
                    change_col = col
                    break
            
            # 过滤条件
            try:
                should_skip = False
                
                # 检查涨跌幅
                if change_col and stock[change_col] > -3:
                    should_skip = True
                
                # 检查技术指标，避免使用 DataFrame 的布尔值判断
                if not should_skip:
                    has_upper_shadow = MarketAnalyzer._has_upper_shadow(recent_data)
                    is_multi_pump = MarketAnalyzer._is_multi_pump(recent_data)
                    has_previous_multi_limit_up = MarketAnalyzer._has_previous_multi_limit_up(recent_data)
                    is_high_position = MarketAnalyzer._is_high_position(recent_data)
                    is_zhihuji_pattern = MarketAnalyzer._is_zhihuji_pattern(recent_data)
                    
                    if (has_upper_shadow or is_multi_pump or has_previous_multi_limit_up or 
                        is_high_position or is_zhihuji_pattern):
                        should_skip = True
                    
                if not should_skip:
                    result_stocks.append(stock)
                    
            except Exception as e:
                print(f"分析股票 {code} 时出错: {str(e)}")
                continue
            
        print(f"第二步筛选后剩余股票数: {len(result_stocks)}")
        return result_stocks[:20]  # 最多返回20只股票
        
    @staticmethod
    def _has_upper_shadow(data: pl.DataFrame) -> bool:
        """检查是否有明显上影线"""
        latest = data.tail(1)
        if latest.is_empty():
            return False
            
        high = float(latest['high'].item())
        close = float(latest['close'].item())
        open_price = float(latest['open'].item())
        
        upper_shadow = high - max(close, open_price)
        body = abs(close - open_price)
        
        return upper_shadow > body * 0.5  # 上影线长度超过实体的50%
        
    @staticmethod
    def _is_multi_pump(data: pl.DataFrame) -> bool:
        """检查是否存在多次拉升"""
        if data.height < 3:
            return False
            
        # 计算日内振幅
        try:
            data = data.with_columns([
                ((pl.col('high') - pl.col('low')) / pl.col('low') * 100).alias('amplitude')
            ])
            
            # 统计大振幅天数
            large_amplitude_days = data.filter(pl.col('amplitude') > 5).height  # 振幅超过5%算大振幅
            
            return large_amplitude_days >= 2  # 2天以上大振幅视为多次拉升
        except Exception as e:
            print(f"检查多次拉升时出错: {str(e)}")
            return False
        
    @staticmethod
    def _has_previous_multi_limit_up(data: pl.DataFrame) -> bool:
        """检查下跌前是否有连续涨停"""
        if data.height < 3:
            return False
            
        try:
            # 获取最近3天数据
            recent_data = data.tail(3)
            if recent_data.is_empty() or recent_data.height < 3:
                return False
                
            # 确保涨跌幅列存在
            change_col = 'change_pct' if 'change_pct' in recent_data.columns else '涨跌幅'
            if change_col not in recent_data.columns:
                return False
                
            changes = recent_data[change_col].to_list()
            
            # 检查是否存在连续涨停后下跌
            for i in range(len(changes)-2):
                if changes[i] >= 9.8 and changes[i+1] >= 9.8 and changes[i+2] < 0:
                    return True
            return False
        except Exception as e:
            print(f"检查连续涨停时出错: {str(e)}")
            return False
        
    @staticmethod
    def _is_high_position(data: pl.DataFrame) -> bool:
        """检查是否处于高位"""
        if data.height < 10:
            return False
            
        try:
            latest = data.tail(1)
            if latest.is_empty():
                return False
                
            latest_close = float(latest['close'].item())
            min_price = float(data['low'].min())
            
            # 当前价格超过最低价50%视为高位
            return (latest_close - min_price) / min_price > 0.5
        except Exception as e:
            print(f"检查高位时出错: {str(e)}")
            return False
        
    @staticmethod
    def _is_zhihuji_pattern(data: pl.DataFrame) -> bool:
        """检查是否为织布机形态"""
        if data.height < 3:
            return False
            
        try:
            recent_data = data.tail(3)
            if recent_data.is_empty() or recent_data.height < 3:
                return False
                
            # 计算实体和影线
            recent_data = recent_data.with_columns([
                (pl.col('close') - pl.col('open')).alias('body'),
                (pl.col('high') - pl.col('low')).alias('total_range'),
                ((pl.col('high') - pl.max_horizontal(pl.col('close'), pl.col('open'))) +
                 (pl.min_horizontal(pl.col('close'), pl.col('open')) - pl.col('low'))).alias('shadows')
            ])
            
            # 织布机特征：小实体、长上下影线、交替出现
            small_body = abs(recent_data['body']) < recent_data['total_range'] * 0.3
            long_shadows = recent_data['shadows'] > recent_data['total_range'] * 0.6
            
            # 计算符合条件的天数
            condition_met_days = (small_body & long_shadows).sum()
            
            return condition_met_days >= 2  # 3天内出现2次以上织布机形态
        except Exception as e:
            print(f"检查织布机形态时出错: {str(e)}")
            return False
    
    @staticmethod
    def analyze_new_high_stocks(market_states_data: pl.DataFrame, days: int = 5, selected_date=None,
                               exclude_st: bool = True, include_non_main_board: bool = False) -> List[dict]:
        """分析新高股票"""
        try:
            import time
            from datetime import datetime, timedelta

            if selected_date is None:
                selected_date = datetime.now().date()
            elif isinstance(selected_date, str):
                selected_date = datetime.strptime(selected_date, '%Y-%m-%d').date()

            print(f"🚀 开始分析{days}日新高股票...")
            start_time = time.time()

            # 1. 改进日期范围计算
            end_date = selected_date - timedelta(days=1)
            # 考虑交易日，而不是自然日
            start_date = end_date - timedelta(days=days * 2)  # 预留足够的天数

            # 2. 改进历史数据获取
            historical_data = market_states_data.filter(
                (pl.col('日期') >= start_date) &
                (pl.col('日期') <= end_date)
            )

            if historical_data.is_empty():
                print(f"⚠️  未找到历史数据 ({start_date} 到 {end_date})")
                return []

            # 3. 改进历史最高价计算
            historical_highs = historical_data.group_by('代码').agg([
                pl.col('最高').max().alias('历史最高价'),
                pl.col('日期').n_unique().alias('交易天数')  # 使用n_unique更准确
            ]).filter(
                pl.col('交易天数') >= min(days, 3)  # 至少3个交易日
            )

            # 4. 获取目标日期数据
            target_data = market_states_data.filter(pl.col('日期') == selected_date)

            if target_data.is_empty():
                print(f"⚠️  未找到 {selected_date} 的数据")
                return []

            # 5. 应用过滤选项
            if exclude_st:
                # 过滤掉ST股票（名称包含ST、*ST、退等）
                target_data = target_data.filter(
                    ~pl.col('名称').str.contains(r'ST|退|暂停')
                )
                print(f"🔧 已过滤ST股票，剩余 {target_data.height} 只股票")

            if not include_non_main_board:
                # 只保留主板股票（代码以00、60开头）
                target_data = target_data.filter(
                    pl.col('代码').str.starts_with('00') |
                    pl.col('代码').str.starts_with('60')
                )
                print(f"🔧 已过滤非主板股票，剩余 {target_data.height} 只股票")

            # 6. 改进结果计算和列选择
            # 首先检查目标数据中实际存在的列
            available_columns = target_data.columns
            print(f"目标数据可用列: {available_columns}")

            # 基础必需列
            base_columns = ['代码', '名称', '收盘', '成交量', '涨跌幅']

            # 可选的涨跌幅列
            optional_columns = ['5日涨跌幅', '10日涨跌幅', '20日涨跌幅']

            # 构建实际可用的列列表
            select_columns = base_columns.copy()
            for col in optional_columns:
                if col in available_columns:
                    select_columns.append(col)
                else:
                    print(f"警告: 列 '{col}' 不存在于数据中")

            result = target_data.join(historical_highs, on='代码', how='inner').filter(
                pl.col('收盘') > pl.col('历史最高价')
            ).with_columns([
                ((pl.col('收盘') - pl.col('历史最高价')) / pl.col('历史最高价') * 100)
                .round(2).alias('突破幅度'),
                pl.col('收盘').alias('收盘价'),
                pl.col('历史最高价').alias('历史最高'),
            ])

            # 添加计算出的列到选择列表，并重命名涨跌幅列以匹配前端期望
            final_columns = ['代码', '名称', '收盘价', '历史最高', '突破幅度', '成交量', '涨跌幅']
            rename_mapping = {}

            for col in optional_columns:
                if col in available_columns:
                    final_columns.append(col)
                    # 为前端添加百分号标识
                    new_col_name = f"{col}(%)"
                    rename_mapping[col] = new_col_name

            result = result.select(final_columns)

            # 重命名涨跌幅列
            if rename_mapping:
                result = result.rename(rename_mapping)

            result = result.sort('突破幅度', descending=True)

            elapsed_time = time.time() - start_time

            if not result.is_empty():
                print(f"✅ 找到 {result.height} 只{days}日新高股票 (耗时: {elapsed_time:.3f}秒)")
                return result.to_dicts()
            else:
                print(f"ℹ️  未找到{days}日新高股票 (耗时: {elapsed_time:.3f}秒)")
                return []

        except Exception as e:
            print(f"❌ 分析新高股票失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    @staticmethod
    def analyze_continuous_limit_up(limit_up_data: pl.DataFrame) -> List[dict]:
        """分析近期连板高度"""
        if limit_up_data.is_empty():
            print("涨停板数据为空，无法分析连板高度")
            return []
            
        # 寻找包含连板天数的列
        continuous_days_col = None
        for col in ['连板天数', '连续涨停天数', '昨日连板数', '涨停统计']:
            if col in limit_up_data.columns:
                continuous_days_col = col
                break
                
        # 寻找股票名称列和代码列
        name_col = None
        for col in ['名称', '股票简称']:
            if col in limit_up_data.columns:
                name_col = col
                break
                
        code_col = None
        for col in ['代码', '股票代码']:
            if col in limit_up_data.columns:
                code_col = col
                break
                
        # 如果没有找到需要的列，返回空结果
        if continuous_days_col is None or name_col is None or code_col is None:
            print(f"数据缺少连板天数、名称或代码列，现有列: {limit_up_data.columns}")
            return []
            
        try:
            # 尝试将连板天数转换为数值
            # 如果连板天数是以"N板"、"N连板"等形式存储的，需要提取数字
            if limit_up_data[continuous_days_col].dtype == pl.Utf8:
                # 尝试从字符串中提取数字
                try:
                    # 尝试直接转换为数字
                    limit_up_data = limit_up_data.with_columns([
                        pl.col(continuous_days_col).cast(pl.Int64).alias('连板数')
                    ])
                except:
                    # 如果失败，尝试从字符串中提取数字
                    def extract_number(s):
                        import re
                        if s is None:
                            return 1  # 默认为1板
                        match = re.search(r'(\d+)', str(s))
                        return int(match.group(1)) if match else 1
                    
                    # 使用polars的自定义函数
                    limit_up_data = limit_up_data.with_columns([
                        pl.col(continuous_days_col).map_elements(extract_number).alias('连板数')
                    ])
            else:
                # 如果已经是数值类型，直接使用
                limit_up_data = limit_up_data.with_columns([
                    pl.col(continuous_days_col).alias('连板数')
                ])
                
            # 按连板数排序，取前20只股票
            result = (
                limit_up_data
                .sort('连板数', descending=True)
                .head(20)
                .select([code_col, name_col, '连板数'])
                .to_dicts()
            )
            
            return result
            
        except Exception as e:
            print(f"分析连板高度时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    @staticmethod
    def get_beijing_microcap_analysis(date_str: str) -> dict:
        """获取北证50和微盘股分析数据"""
        try:
            from utils.data_fetcher import DataFetcher
            data_fetcher = DataFetcher()

            # 获取北证50数据 (代码: 899050)
            beijing_df = data_fetcher.index_metadata_manager.get_index_data(
                '899050',
                start_date=date_str,
                end_date=date_str
            )

            # 获取微盘股数据 (代码: 800007)
            microcap_df = data_fetcher.index_metadata_manager.get_index_data(
                '800007',
                start_date=date_str,
                end_date=date_str
            )

            # 提取涨跌幅数据
            beijing_change = None
            microcap_change = None

            if beijing_df is not None and not beijing_df.is_empty():
                beijing_change = float(beijing_df['涨跌幅'][0])

            if microcap_df is not None and not microcap_df.is_empty():
                microcap_change = float(microcap_df['涨跌幅'][0])

            return {
                'beijing_50': {
                    'change_pct': beijing_change,
                    'status': MarketAnalyzer._get_sentiment_status(beijing_change) if beijing_change is not None else '无数据'
                },
                'microcap': {
                    'change_pct': microcap_change,
                    'status': MarketAnalyzer._get_sentiment_status(microcap_change, is_microcap=True) if microcap_change is not None else '无数据'
                }
            }

        except Exception as e:
            print(f"获取北证50和微盘股数据时出错: {str(e)}")
            return {
                'beijing_50': {'change_pct': None, 'status': '数据获取失败'},
                'microcap': {'change_pct': None, 'status': '数据获取失败'}
            }

    @staticmethod
    def _get_sentiment_status(change_pct: float, is_microcap: bool = False) -> str:
        """根据涨跌幅获取情绪状态"""
        if is_microcap:
            # 微盘股的阈值稍有不同
            if change_pct >= 2:
                return "🔥 情绪火热"
            elif change_pct >= 1:
                return "📈 情绪积极"
            elif change_pct >= -1:
                return "😐 情绪平稳"
            elif change_pct >= -3:
                return "📉 情绪谨慎"
            else:
                return "❄️ 情绪低迷"
        else:
            # 北证50的阈值
            if change_pct >= 2:
                return "🔥 情绪火热"
            elif change_pct >= 1:
                return "📈 情绪积极"
            elif change_pct >= -1:
                return "😐 情绪平稳"
            elif change_pct >= -2:
                return "📉 情绪谨慎"
            else:
                return "❄️ 情绪低迷"

    @staticmethod
    def get_trading_strategy(beijing_data: dict) -> dict:
        """根据北证50和微盘股数据生成交易策略建议"""
        try:
            beijing_change = beijing_data.get('beijing_50', {}).get('change_pct')
            microcap_change = beijing_data.get('microcap', {}).get('change_pct')

            if beijing_change is None or microcap_change is None:
                return {
                    'strategy': "暂无数据",
                    'emoji': "⚠️",
                    'risk_level': "未知",
                    'description': "数据不足，无法生成策略建议"
                }

            # 策略逻辑（基于app.py中的逻辑）
            if -1 < beijing_change <= 1 or -1 < microcap_change <= 1:
                return {
                    'strategy': "短线情绪无碍，可以继续参与。",
                    'emoji': "😐",
                    'risk_level': "中性",
                    'description': "市场情绪平稳，可维持现有仓位"
                }
            elif 1 <= beijing_change < 2 or 1 <= microcap_change < 2:
                return {
                    'strategy': "短线情绪积极，可适当上仓位。",
                    'emoji': "😊",
                    'risk_level': "积极",
                    'description': "市场情绪转好，建议适度增加仓位"
                }
            elif beijing_change >= 2 or microcap_change >= 2:
                return {
                    'strategy': "短线情绪高昂，可以重仓参与。",
                    'emoji': "🚀",
                    'risk_level': "激进",
                    'description': "市场情绪火热，可考虑重仓操作"
                }
            elif -2 < beijing_change <= -1 or -3 < microcap_change <= -1:
                return {
                    'strategy': "短线谨慎，可以轻仓抢反弹。",
                    'emoji': "😰",
                    'risk_level': "谨慎",
                    'description': "市场情绪转弱，建议轻仓操作"
                }
            elif beijing_change <= -2 or microcap_change <= -3:
                return {
                    'strategy': "短线情绪瓦解，A浪下跌开始。",
                    'emoji': "😱",
                    'risk_level': "保守",
                    'description': "市场情绪恶化，建议观望或减仓"
                }
            else:
                return {
                    'strategy': "情绪区间不明，建议谨慎。",
                    'emoji': "🤔",
                    'risk_level': "观望",
                    'description': "市场信号不明确，建议谨慎观望"
                }

        except Exception as e:
            print(f"生成交易策略时出错: {str(e)}")
            return {
                'strategy': "策略生成失败",
                'emoji': "❌",
                'risk_level': "未知",
                'description': f"策略生成出错: {str(e)}"
            }

    @staticmethod
    def get_multi_index_kline_data(selected_indices: List[str], date_str: str, days_range: int) -> dict:
        """获取多指数K线数据"""
        try:
            from utils.data_fetcher import DataFetcher
            from utils.visualizers.index_visualizer import IndexVisualizer
            from datetime import datetime, timedelta
            import os

            data_fetcher = DataFetcher()

            # 指数代码映射
            index_options = {
                '上证指数': 'sh000001',
                '深证成指': 'sz399001',
                '创业板指': 'sz399006',
                '上证50': 'sh000016',
                '沪深300': 'sh000300',
                '中证500': 'sh000905',
                '中证2000': 'sz932000',
                '科创50': 'sh000688',
                '中证1000': 'sh000852'
            }

            # 计算开始日期
            selected_date = datetime.strptime(date_str, '%Y%m%d').date()
            start_date = selected_date - timedelta(days=days_range)
            start_date_str = start_date.strftime('%Y%m%d')

            index_data_dict = {}

            for index_name in selected_indices:
                if index_name not in index_options:
                    continue

                index_code = index_options[index_name]

                try:
                    # 检查是否有指数日K元数据
                    if os.path.exists("data_cache/indices/index_daily_metadata.parquet"):
                        # 从元数据中获取指数数据
                        clean_code = index_code.replace('sh', '').replace('sz', '')

                        df = data_fetcher.index_metadata_manager.get_index_data(
                            clean_code,
                            start_date=start_date_str,
                            end_date=date_str
                        )

                        if df is not None and not df.is_empty():
                            print(f"从元数据中获取到 {index_name} 数据，共 {df.height} 行")
                            index_data_dict[index_name] = df
                            continue

                    # 如果没有元数据，尝试使用akshare获取
                    try:
                        import akshare as ak

                        # 尝试不同的代码格式
                        code_variations = [
                            index_code,
                            index_code.replace('sh', '').replace('sz', ''),
                        ]

                        for code in code_variations:
                            try:
                                print(f"尝试使用代码 {code} 获取 {index_name} 数据")
                                df = ak.stock_zh_index_daily(symbol=code)
                                if not df.empty:
                                    df = pl.from_pandas(df)

                                    # 确保日期列格式正确
                                    if df['date'].dtype == pl.Utf8:
                                        df = df.with_columns([
                                            pl.col('date').str.strptime(pl.Date, '%Y-%m-%d').alias('date')
                                        ])

                                    # 重命名列以匹配预期格式
                                    df = df.rename({
                                        'date': '日期',
                                        'open': '开盘',
                                        'close': '收盘',
                                        'high': '最高',
                                        'low': '最低',
                                        'volume': '成交量'
                                    })

                                    # 筛选日期范围
                                    df = df.filter(
                                        (pl.col('日期') >= pl.lit(start_date)) &
                                        (pl.col('日期') <= pl.lit(selected_date))
                                    )

                                    if not df.is_empty():
                                        print(f"使用代码 {code} 成功获取到 {index_name} 数据，共 {df.height} 行")
                                        index_data_dict[index_name] = df
                                        break
                            except Exception as e:
                                print(f"使用代码 {code} 获取 {index_name} 数据失败: {str(e)}")

                    except ImportError:
                        print("akshare未安装，无法获取在线数据")

                except Exception as e:
                    print(f"获取 {index_name} 数据时出错: {str(e)}")

            # 生成K线图HTML和ECharts配置
            if index_data_dict:
                chart_html = IndexVisualizer.plot_multi_index_kline(index_data_dict)
                chart_options = IndexVisualizer.get_multi_index_kline_options(index_data_dict)
                return {
                    'chart_html': chart_html,
                    'chart_options': chart_options,
                    'data_summary': {name: df.height for name, df in index_data_dict.items()},
                    'success_count': len(index_data_dict),
                    'total_requested': len(selected_indices)
                }
            else:
                return {
                    'chart_html': "<div>无法获取指数数据</div>",
                    'data_summary': {},
                    'success_count': 0,
                    'total_requested': len(selected_indices)
                }

        except Exception as e:
            print(f"获取多指数K线数据时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'chart_html': f"<div>数据获取失败: {str(e)}</div>",
                'data_summary': {},
                'success_count': 0,
                'total_requested': len(selected_indices) if selected_indices else 0
            }

    @staticmethod
    def get_available_indices() -> List[dict]:
        """获取可用的指数列表"""
        try:
            from utils.data_fetcher import DataFetcher
            import os

            # 默认指数选项
            default_indices = [
                {'name': '上证指数', 'code': 'sh000001', 'available': False},
                {'name': '深证成指', 'code': 'sz399001', 'available': False},
                {'name': '创业板指', 'code': 'sz399006', 'available': False},
                {'name': '上证50', 'code': 'sh000016', 'available': False},
                {'name': '沪深300', 'code': 'sh000300', 'available': False},
                {'name': '中证500', 'code': 'sh000905', 'available': False},
                {'name': '中证2000', 'code': 'sz932000', 'available': False},
                {'name': '科创50', 'code': 'sh000688', 'available': False},
                {'name': '中证1000', 'code': 'sh000852', 'available': False}
            ]

            # 检查是否有指数元数据
            if os.path.exists("data_cache/indices/index_daily_metadata.parquet"):
                try:
                    data_fetcher = DataFetcher()
                    index_metadata = data_fetcher.index_metadata_manager.load_metadata()

                    if index_metadata is not None and not index_metadata.is_empty():
                        available_codes = index_metadata['代码'].unique().to_list()

                        # 更新可用状态
                        for index_info in default_indices:
                            clean_code = index_info['code'].replace('sh', '').replace('sz', '')
                            if clean_code in available_codes or index_info['code'] in available_codes:
                                index_info['available'] = True

                        # 添加元数据中有但默认列表中没有的指数
                        if '名称' in index_metadata.columns:
                            unique_indices = index_metadata.select(['代码', '名称']).unique()

                            for row in unique_indices.iter_rows():
                                code, name = row
                                clean_code = code.replace('sh', '').replace('sz', '')

                                # 检查是否已在默认列表中
                                found = False
                                for existing in default_indices:
                                    existing_clean = existing['code'].replace('sh', '').replace('sz', '')
                                    if clean_code == existing_clean:
                                        found = True
                                        break

                                if not found:
                                    # 添加新指数
                                    full_code = f"sh{clean_code}" if len(clean_code) == 6 else f"sz{clean_code}"
                                    default_indices.append({
                                        'name': name,
                                        'code': full_code,
                                        'available': True
                                    })

                except Exception as e:
                    print(f"检查指数元数据时出错: {str(e)}")

            return default_indices

        except Exception as e:
            print(f"获取可用指数列表时出错: {str(e)}")
            return []

    @staticmethod
    def analyze_heima_stocks(market_states_data: pl.DataFrame, date=None, exclude_st=True, include_non_main_board=False) -> List[dict]:
        """黑马分析 - 分析涨停股票"""
        try:
            if date is None:
                selected_date = datetime.now().date()
            elif isinstance(date, str):
                selected_date = datetime.strptime(date, '%Y-%m-%d').date()
            else:
                selected_date = date

            print(f"🔧 DEBUG: 黑马分析日期: {selected_date}")
            print(f"🔧 DEBUG: 市场数据行数: {market_states_data.height}")
            print(f"🔧 DEBUG: 过滤参数 - exclude_st: {exclude_st}, include_non_main_board: {include_non_main_board}")

            # 筛选当天的涨停个股
            zt_df = market_states_data.filter(
                (pl.col('涨停') == True) &
                (pl.col('日期') == selected_date)
            )

            print(f"🔧 DEBUG: 筛选后涨停股票数: {zt_df.height}")

            if zt_df.is_empty():
                # 如果当天没有涨停股票，尝试最近几天的数据
                print("🔧 DEBUG: 当天无涨停股票，尝试最近3天")
                recent_dates = market_states_data['日期'].unique().sort(descending=True).head(3)
                for recent_date in recent_dates:
                    zt_df = market_states_data.filter(
                        (pl.col('涨停') == True) &
                        (pl.col('日期') == recent_date)
                    )
                    if not zt_df.is_empty():
                        print(f"🔧 DEBUG: 找到 {recent_date} 的涨停股票: {zt_df.height}只")
                        selected_date = recent_date
                        break

            if zt_df.is_empty():
                return []

            # 应用过滤条件
            original_count = zt_df.height

            # 过滤ST股票
            if exclude_st:
                zt_df = zt_df.filter(~pl.col('名称').str.contains('ST'))
                print(f"🔧 DEBUG: 去掉ST股票后: {zt_df.height}只 (原{original_count}只)")

            # 过滤非主板股票（根据涨跌幅限制判断）
            if not include_non_main_board:
                # 主板股票通常涨跌幅限制为10%（0.10）
                zt_df = zt_df.filter(pl.col('涨跌幅限制') == 0.10)
                print(f"🔧 DEBUG: 只保留主板股票后: {zt_df.height}只")

            if zt_df.is_empty():
                print("🔧 DEBUG: 应用过滤条件后无股票")
                return []

            # 检查可用列
            available_cols = zt_df.columns
            select_cols = ['代码', '名称']

            # 添加可用的列
            optional_cols = ['收盘', '涨跌幅', '成交额', '换手率', '连板天数', '5日涨跌幅', '10日涨跌幅']
            for col in optional_cols:
                if col in available_cols:
                    select_cols.append(col)

            # 按连板天数排序（连板数最多的在前面）
            if '连板数' in zt_df.columns:
                # 先按连板数排序（降序），再按成交额排序（降序）
                if '成交额' in select_cols:
                    zt_df = zt_df.sort(['连板数', '成交额'], descending=[True, True])
                else:
                    zt_df = zt_df.sort('连板数', descending=True)
            elif '成交额' in select_cols:
                # 如果没有连板数，按成交额排序
                zt_df = zt_df.sort('成交额', descending=True)

            # 转换为字典列表
            result = zt_df.select(select_cols).to_dicts()

            print(f"🔧 DEBUG: 返回黑马股票数: {len(result)}")
            print(f"🔧 DEBUG: 排序方式: 连板数降序 -> 成交额降序")
            return result

        except Exception as e:
            print(f"黑马分析失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    @staticmethod
    def analyze_baima_stocks(market_states_data: pl.DataFrame, intervals=None, min_market_cap: float = 100, exclude_st=True, include_non_main_board=False, include_main_board=True, include_kcb_cyb=True, include_bjs=False) -> dict:
        """增强的白马分析 - 支持多时间区间对比，包含行业和概念信息"""
        try:
            print(f"🔧 DEBUG: 开始增强白马分析")
            print(f"  - 数据行数: {market_states_data.height}")
            print(f"  - 时间区间数: {len(intervals) if intervals else 0}")

            # 使用传入的市场状态数据，它已经包含了股票的基本信息
            # market_states_data 包含了所有需要的股票数据
            print(f"  - 市场数据列: {market_states_data.columns[:10]}...")

            # 检查必要的列是否存在
            required_cols = ['代码', '名称', '日期', '收盘']
            missing_cols = [col for col in required_cols if col not in market_states_data.columns]
            if missing_cols:
                return {'error': f'缺少必要的列: {missing_cols}'}

            # 尝试加载和合并行业、概念数据
            enhanced_data = market_states_data
            try:
                import os
                import pandas as pd

                # 检查实际存在的文件
                ths_file = 'data_cache/sectors/同花顺板块成分股.xlsx'
                dc_file = 'data_cache/sectors/东财板块成分股.xlsx'

                if os.path.exists(ths_file):
                    print("🔧 DEBUG: 加载同花顺行业和概念数据...")

                    # 读取同花顺数据（包含行业和概念）
                    ths_df = pd.read_excel(ths_file)
                    
                    # 标准化列名
                    ths_df = ths_df.rename(columns={
                        '股票代码': '代码',
                        '股票名称': '名称',
                        '板块名称': '板块名称'
                    })
                    
                    # 确保代码为6位字符串
                    ths_df['代码'] = ths_df['代码'].astype(str).str.zfill(6)
                    ths_pl = pl.from_pandas(ths_df)

                    # 分别处理行业和概念数据
                    industry_data = ths_pl.filter(pl.col("板块类型") == "行业")
                    concept_data = ths_pl.filter(pl.col("板块类型") == "概念")

                    # 处理行业数据 - 每个股票只保留一个主要行业
                    industries_grouped = None
                    if industry_data.height > 0:
                        industries_grouped = (
                            industry_data
                            .group_by("代码")
                            .agg([
                                pl.col("板块名称").first().alias("行业"),
                            ])
                        )
                        print(f"🔧 DEBUG: 处理了 {industries_grouped.height} 只股票的行业信息")

                    # 处理概念数据 - 合并多个概念
                    concepts_grouped = None
                    if concept_data.height > 0:
                        concepts_grouped = (
                            concept_data
                            .group_by("代码")
                            .agg([
                                pl.col("板块名称").str.concat(",").alias("概念"),
                            ])
                        )
                        print(f"🔧 DEBUG: 处理了 {concepts_grouped.height} 只股票的概念信息")

                    # 合并行业和概念信息
                    sector_info = None
                    if industries_grouped is not None and concepts_grouped is not None:
                        sector_info = (
                            industries_grouped
                            .join(concepts_grouped, on="代码", how="outer")
                            .select(["代码", "行业", "概念"])
                        )
                    elif industries_grouped is not None:
                        sector_info = industries_grouped.select(["代码", "行业"])
                    elif concepts_grouped is not None:
                        sector_info = concepts_grouped.select(["代码", "概念"])

                    # 与市场数据合并
                    if sector_info is not None:
                        enhanced_data = market_states_data.join(sector_info, on="代码", how="left")
                        print(f"🔧 DEBUG: 成功合并行业概念数据，新列数: {len(enhanced_data.columns)}")
                        
                        # 验证合并结果
                        if "行业" in enhanced_data.columns:
                            non_null_industry_count = enhanced_data.filter(pl.col("行业").is_not_null()).height
                            print(f"🔧 DEBUG: 有行业信息的股票数量: {non_null_industry_count}")
                    else:
                        print("🔧 DEBUG: 没有找到有效的行业概念数据")

                else:
                    print("🔧 DEBUG: 同花顺板块文件不存在，使用原始数据")

            except Exception as e:
                print(f"⚠️ 加载行业概念数据失败: {str(e)}，使用原始数据")
                enhanced_data = market_states_data

            # 如果没有传入时间区间，使用默认时间区间
            if intervals is None or len(intervals) == 0:
                from datetime import datetime, timedelta
                end_date = datetime.now().date()
                intervals = [
                    {
                        'start_date': (end_date - timedelta(days=30)).strftime('%Y-%m-%d'),
                        'end_date': end_date.strftime('%Y-%m-%d'),
                        'name': '最近30天'
                    },
                    {
                        'start_date': (end_date - timedelta(days=90)).strftime('%Y-%m-%d'),
                        'end_date': end_date.strftime('%Y-%m-%d'),
                        'name': '最近90天'
                    },
                    {
                        'start_date': f'{end_date.year}-01-01',
                        'end_date': end_date.strftime('%Y-%m-%d'),
                        'name': '本年度'
                    }
                ]

            print(f"🔧 DEBUG: 使用的时间区间: {len(intervals)}个")

            # 开始筛选股票 - 使用增强的数据
            filtered_stocks = enhanced_data

            # 构建筛选条件
            filter_conditions = []

            # ST股票筛选
            if exclude_st:
                filter_conditions.append(~pl.col('名称').str.contains("ST"))

            # 板块筛选（基于股票代码）
            board_conditions = []

            # 主板股票筛选（沪深主板：000、001、002、600、601、603开头）
            if include_main_board:
                main_board_condition = (
                    pl.col("代码").str.starts_with("000") |
                    pl.col("代码").str.starts_with("001") |
                    pl.col("代码").str.starts_with("002") |
                    pl.col("代码").str.starts_with("600") |
                    pl.col("代码").str.starts_with("601") |
                    pl.col("代码").str.starts_with("603")
                )
                board_conditions.append(main_board_condition)

            # 科创板/创业板股票筛选（科创板：688开头，创业板：300、301开头）
            if include_kcb_cyb:
                kcb_cyb_condition = (
                    pl.col("代码").str.starts_with("688") |
                    pl.col("代码").str.starts_with("300") |
                    pl.col("代码").str.starts_with("301")
                )
                board_conditions.append(kcb_cyb_condition)

            # 北交所股票筛选（8、4开头）
            if include_bjs:
                bjs_condition = (
                    pl.col("代码").str.starts_with("8") |
                    pl.col("代码").str.starts_with("4")
                )
                board_conditions.append(bjs_condition)

            # 组合板块条件
            if board_conditions:
                board_condition = board_conditions[0]
                for condition in board_conditions[1:]:
                    board_condition = board_condition | condition
                filter_conditions.append(board_condition)

            # 市值筛选（如果有市值列）
            if '市值' in filtered_stocks.columns:
                filter_conditions.append(pl.col('市值') >= min_market_cap * 100000000)  # 转换为元

            # 应用筛选条件
            if filter_conditions:
                combined_condition = filter_conditions[0]
                for condition in filter_conditions[1:]:
                    combined_condition = combined_condition & condition
                filtered_stocks = filtered_stocks.filter(combined_condition)

            print(f"🔧 DEBUG: 过滤后股票数: {filtered_stocks.height}")

            # 降低市值要求，如果没有股票
            if filtered_stocks.is_empty() and min_market_cap > 10:
                print(f"🔧 DEBUG: 没有股票满足{min_market_cap}亿市值要求，降低到10亿")
                # 重新筛选，降低市值要求
                filter_conditions_relaxed = [cond for cond in filter_conditions if '市值' not in str(cond)]
                if '市值' in enhanced_data.columns:
                    filter_conditions_relaxed.append(pl.col('市值') >= 10 * 100000000)  # 10亿

                if filter_conditions_relaxed:
                    combined_condition = filter_conditions_relaxed[0]
                    for condition in filter_conditions_relaxed[1:]:
                        combined_condition = combined_condition & condition
                    filtered_stocks = enhanced_data.filter(combined_condition)
                    print(f"🔧 DEBUG: 降低市值要求后股票数: {filtered_stocks.height}")

            if filtered_stocks.is_empty():
                return {
                    'intervals': [
                        {
                            'name': interval.get('name', f'区间{i+1}'),
                            'start_date': interval['start_date'],
                            'end_date': interval['end_date'],
                            'column_name': f"{interval.get('name', f'区间{i+1}')}涨跌幅"
                        }
                        for i, interval in enumerate(intervals)
                    ],
                    'stocks': [],
                    'total_count': 0,
                    'change_columns': [],
                    'min_market_cap': min_market_cap
                }

            # 查找每只股票的首个交易日
            first_trading_days = filtered_stocks.group_by("代码").agg([
                pl.col("日期").min().alias("首个交易日")
            ])

            # 将首个交易日信息添加到filtered_stocks中
            filtered_stocks_with_first_day = filtered_stocks.join(first_trading_days, on="代码")

            # 标记新股的首日数据
            filtered_stocks_with_first_day = filtered_stocks_with_first_day.with_columns([
                (pl.col("日期") == pl.col("首个交易日")).alias("是首日交易")
            ])

            # 为每个区间计算涨跌幅
            interval_results = []
            change_columns = []

            for i, interval in enumerate(intervals):
                # 解析时间区间
                if isinstance(interval, dict):
                    start_date_str = interval['start_date']
                    end_date_str = interval['end_date']
                    interval_name = interval.get('name', f'区间{i+1}')
                else:
                    # 兼容旧格式 (start_date, end_date) tuple
                    start_date_str = interval[0].strftime('%Y-%m-%d') if hasattr(interval[0], 'strftime') else str(interval[0])
                    end_date_str = interval[1].strftime('%Y-%m-%d') if hasattr(interval[1], 'strftime') else str(interval[1])
                    interval_name = f'区间{i+1}'

                # 转换为日期对象
                from datetime import datetime
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

                print(f"🔧 DEBUG: 处理区间{i+1}: {start_date} 到 {end_date}")

                # 过滤日期范围内的数据
                interval_data = filtered_stocks_with_first_day.filter(
                    (pl.col("日期") >= start_date) &
                    (pl.col("日期") <= end_date)
                )

                if interval_data.is_empty():
                    print(f"⚠️ 区间{i+1}无数据")
                    continue

                # 计算每个股票的涨跌幅和最高涨跌幅（排除首日交易数据）
                # 构建聚合列表
                agg_columns = [
                    # 获取期间首个交易日的收盘价
                    pl.col("收盘").first().alias("期初收盘价"),
                    # 获取期间最后一个交易日的收盘价
                    pl.col("收盘").last().alias("期末收盘价"),
                    # 获取区间内最低价和最高价
                    pl.col("最低").min().alias("区间最低价"),
                    pl.col("最高").max().alias("区间最高价"),
                    pl.col("名称").last().alias("名称"),
                ]

                # 添加行业信息（如果存在）
                if "行业" in interval_data.columns:
                    agg_columns.append(pl.col("行业").last().alias("行业"))

                # 添加概念信息（如果存在）
                if "概念" in interval_data.columns:
                    agg_columns.append(pl.col("概念").last().alias("概念"))

                # 添加市值信息（如果存在）
                if "市值" in interval_data.columns:
                    agg_columns.append(pl.col("市值").last().alias("市值"))

                stock_changes = (
                    interval_data
                    .filter(~pl.col("是首日交易"))
                    .sort(["代码", "日期"])  # 确保按时间排序
                    .group_by("代码")
                    .agg(agg_columns)
                    .with_columns([
                        # 计算区间涨跌幅（期初到期末）
                        ((pl.col("期末收盘价") - pl.col("期初收盘价")) / pl.col("期初收盘价") * 100)
                        .alias(f"{interval_name}涨跌幅"),
                        # 计算区间内最高涨跌幅（期初价格到区间最高价）
                        ((pl.col("区间最高价") - pl.col("期初收盘价")) / pl.col("期初收盘价") * 100)
                        .alias(f"{interval_name}最高涨跌幅")
                    ])
                )

                change_col_name = f"{interval_name}涨跌幅"
                max_change_col_name = f"{interval_name}最高涨跌幅"
                change_columns.append(change_col_name)
                change_columns.append(max_change_col_name)

                # 只保留需要的列，避免重复
                if i == 0:
                    # 第一个区间保留所有基础信息和涨跌幅
                    select_columns = ["代码", "名称"]

                    # 添加行业信息（如果存在）
                    if "行业" in stock_changes.columns:
                        select_columns.append("行业")

                    # 添加概念信息（如果存在）
                    if "概念" in stock_changes.columns:
                        select_columns.append("概念")

                    # 添加市值信息（如果存在）
                    if "市值" in stock_changes.columns:
                        select_columns.append("市值")

                    # 添加涨跌幅列和最高涨跌幅列
                    select_columns.append(change_col_name)
                    select_columns.append(max_change_col_name)

                    stock_changes = stock_changes.select(select_columns)
                else:
                    # 后续区间只保留代码、涨跌幅和最高涨跌幅
                    stock_changes = stock_changes.select([
                        "代码", change_col_name, max_change_col_name
                    ])

                interval_results.append(stock_changes)

            # 合并所有区间结果
            if interval_results:
                # 从第一个区间开始
                result_df = interval_results[0]

                # 逐个合并其他区间
                for i in range(1, len(interval_results)):
                    result_df = result_df.join(
                        interval_results[i],
                        on="代码",
                        how="outer"
                    )

                # 处理可能的null值
                for col in change_columns:
                    if col in result_df.columns:
                        result_df = result_df.with_columns([
                            pl.col(col).fill_null(0)
                        ])

                # 按第一个区间的涨跌幅排序
                if change_columns:
                    first_change_col = change_columns[0]
                    result_df = result_df.sort(first_change_col, descending=True)

                # 转换为字典格式返回，确保数据类型正确
                try:
                    # 先处理可能的数据类型问题
                    for col in result_df.columns:
                        if col in ['行业', '概念']:
                            # 确保字符串类型的列正确处理
                            result_df = result_df.with_columns([
                                pl.col(col).cast(pl.Utf8).fill_null("")
                            ])

                    result_data = result_df.to_dicts()

                    # 验证数据完整性
                    if result_data:
                        sample_stock = result_data[0]
                        print(f"🔧 DEBUG: 示例股票数据字段: {list(sample_stock.keys())}")
                        if '行业' in sample_stock:
                            print(f"🔧 DEBUG: 示例行业数据: {sample_stock['行业']}")

                except Exception as e:
                    print(f"⚠️ 数据转换警告: {str(e)}")
                    # 如果转换失败，尝试简化处理
                    result_data = []
                    for row in result_df.iter_rows(named=True):
                        stock_dict = {}
                        for key, value in row.items():
                            # 处理特殊数据类型
                            if value is None:
                                stock_dict[key] = None
                            elif isinstance(value, (int, float, str)):
                                stock_dict[key] = value
                            else:
                                stock_dict[key] = str(value)
                        result_data.append(stock_dict)

                print(f"✅ 白马分析完成: {len(result_data)} 只股票, {len(change_columns)} 个区间")

                return {
                    'intervals': [
                        {
                            'name': interval.get('name', f'区间{i+1}'),
                            'start_date': interval['start_date'],
                            'end_date': interval['end_date'],
                            'column_name': f"{interval.get('name', f'区间{i+1}')}涨跌幅"
                        }
                        for i, interval in enumerate(intervals)
                    ],
                    'stocks': result_data,
                    'total_count': len(result_data),
                    'change_columns': change_columns,
                    'min_market_cap': min_market_cap
                }
            else:
                return {
                    'intervals': [],
                    'stocks': [],
                    'total_count': 0,
                    'change_columns': [],
                    'min_market_cap': min_market_cap
                }

        except Exception as e:
            print(f"❌ 增强白马分析失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return {'error': str(e)}

    @staticmethod
    def _calculate_change_distribution(market_df: pl.DataFrame, change_col: str) -> dict:
        """计算涨跌幅分布"""
        try:
            # 定义涨跌幅区间（使用数值标签）
            ranges = [
                ('≤-9.5%', -20, -9.5),
                ('-9.5%~-5%', -9.5, -5),
                ('-5%~-3%', -5, -3),
                ('-3%~-1%', -3, -1),
                ('-1%~0%', -1, 0),
                ('0%~1%', 0, 1),
                ('1%~3%', 1, 3),
                ('3%~5%', 3, 5),
                ('5%~9.5%', 5, 9.5),
                ('≥9.5%', 9.5, 20)
            ]

            distribution = []
            total_count = market_df.height

            for label, min_val, max_val in ranges:
                if min_val == -20:  # 跌停区间，包含下边界
                    count = market_df.filter(
                        (pl.col(change_col) >= min_val) & (pl.col(change_col) < max_val)
                    ).height
                elif max_val == 20:  # 涨停区间，包含上边界
                    count = market_df.filter(
                        (pl.col(change_col) > min_val) & (pl.col(change_col) <= max_val)
                    ).height
                else:  # 其他区间，不包含边界
                    count = market_df.filter(
                        (pl.col(change_col) > min_val) & (pl.col(change_col) <= max_val)
                    ).height

                percentage = round((count / total_count * 100), 2) if total_count > 0 else 0

                distribution.append({
                    'label': label,
                    'count': count,
                    'percentage': percentage,
                    'range': f"{min_val}% ~ {max_val}%"
                })

            return {
                'ranges': distribution,
                'total_count': total_count
            }

        except Exception as e:
            print(f"计算涨跌幅分布失败: {str(e)}")
            return {
                'ranges': [],
                'total_count': 0
            }

    @staticmethod
    def get_money_effect_analysis(date_str, analysis_type='all', market_states=None,
                                exclude_st=True, include_non_main_board=False):
        """
        获取赚钱效应分析数据
        分为两个部分：全部股票赚钱效应（前300）和近期强势股（曾3板以上）

        Args:
            date_str: 分析日期 (YYYYMMDD格式)
            analysis_type: 分析类型 'all'=全部股票前300, 'strong'=近期强势股(曾3板以上)
            market_states: 预加载的市场状态数据，避免重复加载
            exclude_st: 是否排除ST和退市股票，默认True
            include_non_main_board: 是否包含非主板股票，默认False

        Returns:
            dict: 包含股票列表和统计数据
        """
        try:
            print(f"🔍 开始赚钱效应分析: date={date_str}, type={analysis_type}")

            # 使用传入的市场状态数据，避免重复加载
            if market_states is None or market_states.is_empty():
                return {
                    'stocks': [],
                    'stats': {},
                    'message': '无法获取市场数据'
                }

            # 转换日期格式
            target_date = pd.to_datetime(date_str, format='%Y%m%d').date()

            if analysis_type == 'all':
                # 全部股票赚钱效应分析（前300）
                return MarketAnalyzer._analyze_all_stocks_money_effect(
                    target_date, market_states, exclude_st, include_non_main_board)
            elif analysis_type == 'strong':
                # 近期强势股分析（曾3板以上）
                return MarketAnalyzer._analyze_strong_stocks_money_effect(
                    target_date, market_states, exclude_st, include_non_main_board)
            else:
                return {
                    'stocks': [],
                    'stats': {},
                    'message': f'不支持的分析类型: {analysis_type}'
                }



        except Exception as e:
            print(f"❌ 赚钱效应分析失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'stocks': [],
                'stats': {},
                'message': str(e)
            }

    @staticmethod
    def _analyze_all_stocks_money_effect(target_date, market_states, exclude_st=True, include_non_main_board=False):
        """
        分析全部股票的赚钱效应
        计算当日最低点到收盘价的涨跌幅，按涨幅排序取前300

        Args:
            target_date: 目标日期
            market_states: 市场状态数据
            exclude_st: 是否排除ST和退市股票
            include_non_main_board: 是否包含非主板股票
        """
        try:
            print(f"📊 分析全部股票赚钱效应: {target_date}")

            # 获取目标日期的所有股票数据
            target_data = market_states.filter(pl.col('日期') == target_date)

            if target_data.is_empty():
                return {
                    'stocks': [],
                    'stats': {},
                    'message': f'日期 {target_date} 无数据'
                }

            # 应用股票筛选
            if exclude_st:
                # 排除ST和退市股票
                target_data = target_data.filter(
                    ~pl.col('名称').str.contains("ST", literal=False) &
                    ~pl.col('名称').str.contains("退", literal=False)
                )

            if not include_non_main_board:
                # 只包含主板股票（000、001、002、600、601、603、605开头）
                target_data = target_data.filter(
                    pl.col('代码').str.starts_with("00") |
                    pl.col('代码').str.starts_with("60")
                )

            print(f"📈 目标日期股票数量: {len(target_data)}")

            # 计算赚钱效应指标
            money_effect_stocks = []

            for row in target_data.to_dicts():
                # 计算最低到收盘的涨跌幅（赚钱效应）
                low_price = row.get('最低', 0)
                close_price = row.get('收盘', 0)
                low_to_close_change = ((close_price - low_price) / low_price * 100) if low_price > 0 else 0

                # 计算最高到收盘的涨跌幅（亏钱效应）
                high_price = row.get('最高', 0)
                high_to_close_change = ((close_price - high_price) / high_price * 100) if high_price > 0 else 0

                # 安全的四舍五入，处理None值
                def safe_round(value, digits=2):
                    return round(value, digits) if value is not None else 0.0

                money_effect_stocks.append({
                    '名称': row.get('名称', ''),
                    '代码': row.get('代码', ''),
                    '连板天数': row.get('连板数', 0),
                    '最低到收盘涨幅': round(low_to_close_change, 2),
                    '最高到收盘涨幅': round(high_to_close_change, 2),
                    '当日涨跌幅': safe_round(row.get('涨跌幅')),
                    '5日涨跌幅': safe_round(row.get('5日涨跌幅')),
                    '10日涨跌幅': safe_round(row.get('10日涨跌幅')),
                    '20日涨跌幅': safe_round(row.get('20日涨跌幅')),
                    '收盘价': safe_round(row.get('收盘')),
                    '成交额': safe_round(row.get('成交额'))
                })

            # 按最低到收盘涨幅排序，取前300
            money_effect_stocks.sort(key=lambda x: x['最低到收盘涨幅'], reverse=True)
       

            # 计算统计数据
            stats = MarketAnalyzer._calculate_money_effect_stats(money_effect_stocks)

            print(f"✅ 全部股票赚钱效应分析完成，前300只股票")

            return {
                'stocks': money_effect_stocks,
                'stats': stats,
                'message': f'全部股票赚钱效应分析完成，显示前300只股票'
            }

        except Exception as e:
            print(f"❌ 全部股票赚钱效应分析失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'stocks': [],
                'stats': {},
                'message': str(e)
            }

    @staticmethod
    def _analyze_strong_stocks_money_effect(target_date, market_states, exclude_st=True, include_non_main_board=False):
        """
        分析近期强势股的赚钱效应（曾3板以上）
        显示近十天内曾经达到3板以上的股票

        Args:
            target_date: 目标日期
            market_states: 市场状态数据
            exclude_st: 是否排除ST和退市股票
            include_non_main_board: 是否包含非主板股票
        """
        try:
            print(f"📊 分析近期强势股赚钱效应（曾3板以上）: {target_date}")

            # 获取近十天的日期范围
            end_date = target_date
            start_date = end_date - timedelta(days=10)

            print(f"📅 分析日期范围: {start_date} 到 {end_date}")

            # 筛选日期范围内的数据
            date_filtered = market_states.filter(
                (pl.col('日期') >= start_date) &
                (pl.col('日期') <= end_date)
            )

            if date_filtered.is_empty():
                return {
                    'stocks': [],
                    'stats': {},
                    'message': f'指定日期范围内无数据'
                }

            print(f"📊 日期范围内数据: {len(date_filtered)} 条记录")

            # 应用股票筛选
            filtered_data = date_filtered
            if exclude_st:
                # 排除ST和退市股票
                filtered_data = filtered_data.filter(
                    ~pl.col('名称').str.contains("ST", literal=False) &
                    ~pl.col('名称').str.contains("退", literal=False)
                )

            if not include_non_main_board:
                # 只包含主板股票（000、001、002、600、601、603、605开头）
                filtered_data = filtered_data.filter(
                    pl.col('代码').str.starts_with("00") |
                    pl.col('代码').str.starts_with("60")
                )

            # 找出曾经3板以上的股票
            strong_stocks_data = filtered_data.filter(pl.col('连板数') >= 3)

            if strong_stocks_data.is_empty():
                return {
                    'stocks': [],
                    'stats': {},
                    'message': f'近十天内无3连板以上股票'
                }

            print(f"🎯 找到 {len(strong_stocks_data)} 条强势股记录")

            # 获取这些股票在目标日期的数据
            strong_stock_names = strong_stocks_data['名称'].unique().to_list()
            target_data = market_states.filter(
                (pl.col('日期') == target_date) &
                (pl.col('名称').is_in(strong_stock_names))
            )

            if target_data.is_empty():
                return {
                    'stocks': [],
                    'stats': {},
                    'message': f'目标日期无强势股数据'
                }

            print(f"📈 目标日期强势股数量: {len(target_data)}")

            # 计算赚钱效应指标
            money_effect_stocks = []

            for row in target_data.to_dicts():
                # 计算最低到收盘的涨跌幅（赚钱效应）
                low_price = row.get('最低', 0)
                close_price = row.get('收盘', 0)
                low_to_close_change = ((close_price - low_price) / low_price * 100) if low_price > 0 else 0

                # 计算最高到收盘的涨跌幅（亏钱效应）
                high_price = row.get('最高', 0)
                high_to_close_change = ((close_price - high_price) / high_price * 100) if high_price > 0 else 0

                # 获取该股票的历史最高连板数
                stock_name = row.get('名称', '')
                max_board_days = strong_stocks_data.filter(
                    pl.col('名称') == stock_name
                )['连板数'].max()

                # 安全的四舍五入，处理None值
                def safe_round(value, digits=2):
                    return round(value, digits) if value is not None else 0.0

                money_effect_stocks.append({
                    '名称': stock_name,
                    '代码': row.get('代码', ''),
                    '连板天数': row.get('连板数', 0),  # 当前连板数
                    '历史最高连板': max_board_days,  # 历史最高连板数
                    '最低到收盘涨幅': round(low_to_close_change, 2),
                    '最高到收盘涨幅': round(high_to_close_change, 2),
                    '当日涨跌幅': safe_round(row.get('涨跌幅')),
                    '5日涨跌幅': safe_round(row.get('5日涨跌幅')),
                    '10日涨跌幅': safe_round(row.get('10日涨跌幅')),
                    '20日涨跌幅': safe_round(row.get('20日涨跌幅')),
                    '收盘价': safe_round(row.get('收盘')),
                    '成交额': safe_round(row.get('成交额'))
                })

            # 按最低到收盘涨幅排序
            money_effect_stocks.sort(key=lambda x: x['最低到收盘涨幅'], reverse=True)

            # 计算统计数据
            stats = MarketAnalyzer._calculate_money_effect_stats(money_effect_stocks)

            print(f"✅ 近期强势股赚钱效应分析完成，找到 {len(money_effect_stocks)} 只股票")

            return {
                'stocks': money_effect_stocks,
                'stats': stats,
                'message': f'近期强势股赚钱效应分析完成，找到 {len(money_effect_stocks)} 只曾3板以上股票'
            }

        except Exception as e:
            print(f"❌ 近期强势股赚钱效应分析失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'stocks': [],
                'stats': {},
                'message': str(e)
            }

    @staticmethod
    def _calculate_money_effect_stats(stocks_data):
        """
        计算赚钱效应统计数据

        Args:
            stocks_data: 股票数据列表

        Returns:
            dict: 统计数据
        """
        try:
            if not stocks_data:
                return {
                    'totalStocks': 0,
                    'avgLowToClose': 0,
                    'avgHighToClose': 0,
                    'avg5DayChange': 0,
                    'avg10DayChange': 0
                }

            total_stocks = len(stocks_data)

            # 计算平均值
            avg_low_to_close = sum(stock['最低到收盘涨幅'] for stock in stocks_data) / total_stocks
            avg_high_to_close = sum(stock['最高到收盘涨幅'] for stock in stocks_data) / total_stocks
            avg_5day = sum(stock['5日涨跌幅'] for stock in stocks_data) / total_stocks
            avg_10day = sum(stock['10日涨跌幅'] for stock in stocks_data) / total_stocks

            return {
                'totalStocks': total_stocks,
                'avgLowToClose': round(avg_low_to_close, 2),
                'avgHighToClose': round(avg_high_to_close, 2),
                'avg5DayChange': round(avg_5day, 2),
                'avg10DayChange': round(avg_10day, 2)
            }

        except Exception as e:
            print(f"计算统计数据失败: {str(e)}")
            return {
                'totalStocks': 0,
                'avgLowToClose': 0,
                'avgHighToClose': 0,
                'avg5DayChange': 0,
                'avg10DayChange': 0
            }

    @staticmethod
    def get_index_data(index_name: str, date_str: str, days_range: int = 180):
        """获取单个指数的数据"""
        try:
            print(f"🔧 获取指数数据: {index_name}, 日期: {date_str}, 天数: {days_range}")

            from utils.data_fetcher import DataFetcher
            data_fetcher = DataFetcher()

            # 指数代码映射
            index_code_map = {
                '上证指数': '000001',
                '深证成指': '399001',
                '创业板指': '399006',
                '中证2000': '932000',
                '科创50': '000688',
                '北证50': '899050',
                '中证500': '000905',
                '沪深300': '000300'
            }

            index_code = index_code_map.get(index_name)
            if not index_code:
                print(f"❌ 未知的指数名称: {index_name}")
                return None

            # 计算开始日期
            from datetime import datetime, timedelta
            end_date = datetime.strptime(date_str, '%Y%m%d').date()
            start_date = end_date - timedelta(days=days_range)
            start_date_str = start_date.strftime('%Y%m%d')

            print(f"📊 查询指数 {index_name}({index_code}) 从 {start_date_str} 到 {date_str}")

            # 从指数元数据中获取数据
            df = data_fetcher.index_metadata_manager.get_index_data(
                index_code,
                start_date=start_date_str,
                end_date=date_str
            )

            if df is not None and not df.is_empty():
                print(f"✅ 获取到 {index_name} 数据，共 {df.height} 行")
                return df
            else:
                print(f"❌ 未获取到 {index_name} 数据")
                return None

        except Exception as e:
            print(f"❌ 获取指数 {index_name} 数据失败: {e}")
            import traceback
            traceback.print_exc()
            return None