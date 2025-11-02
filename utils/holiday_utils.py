# -*- coding: utf-8 -*-
"""
中国节假日和交易日工具模块
使用holidays库获取中国法定节假日，结合周末判断非交易日
"""

import holidays
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Union, List, Set


class ChinaHolidayUtil:
    """中国节假日和交易日工具类"""
    
    def __init__(self):
        """初始化中国节假日对象"""
        # 创建中国节假日对象，支持2020-2030年
        self.china_holidays = holidays.China(years=range(2020, 2031))
        
        # 中国股市特殊非交易日（手动维护）
        # 这些是股市特有的非交易日，不在法定节假日范围内
        self.special_non_trading_days = {
            # 2024年特殊非交易日示例
            # date(2024, 1, 1),  # 元旦调休等
            # 可以根据实际情况添加
        }
    
    def is_weekend(self, date_input: Union[str, datetime, date]) -> bool:
        """
        判断是否为周末
        
        Args:
            date_input: 日期，支持字符串、datetime或date对象
            
        Returns:
            bool: 是否为周末
        """
        target_date = self._parse_date(date_input)
        return target_date.weekday() >= 5  # 5=Saturday, 6=Sunday
    
    def is_holiday(self, date_input: Union[str, datetime, date]) -> bool:
        """
        判断是否为法定节假日
        
        Args:
            date_input: 日期，支持字符串、datetime或date对象
            
        Returns:
            bool: 是否为法定节假日
        """
        target_date = self._parse_date(date_input)
        return target_date in self.china_holidays
    
    def is_special_non_trading_day(self, date_input: Union[str, datetime, date]) -> bool:
        """
        判断是否为股市特殊非交易日
        
        Args:
            date_input: 日期，支持字符串、datetime或date对象
            
        Returns:
            bool: 是否为特殊非交易日
        """
        target_date = self._parse_date(date_input)
        return target_date in self.special_non_trading_days
    
    def is_non_trading_day(self, date_input: Union[str, datetime, date]) -> bool:
        """
        判断是否为非交易日（节假日、周末或特殊非交易日的并集）
        
        Args:
            date_input: 日期，支持字符串、datetime或date对象
            
        Returns:
            bool: 是否为非交易日
        """
        return (self.is_weekend(date_input) or 
                self.is_holiday(date_input) or 
                self.is_special_non_trading_day(date_input))
    
    def is_trading_day(self, date_input: Union[str, datetime, date]) -> bool:
        """
        判断是否为交易日（非交易日的反面）
        
        Args:
            date_input: 日期，支持字符串、datetime或date对象
            
        Returns:
            bool: 是否为交易日
        """
        return not self.is_non_trading_day(date_input)
    
    def get_non_trading_days_in_range(self, start_date: Union[str, datetime, date], 
                                     end_date: Union[str, datetime, date]) -> List[date]:
        """
        获取指定日期范围内的所有非交易日
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            List[date]: 非交易日列表
        """
        start = self._parse_date(start_date)
        end = self._parse_date(end_date)
        
        non_trading_days = []
        current_date = start
        
        while current_date <= end:
            if self.is_non_trading_day(current_date):
                non_trading_days.append(current_date)
            current_date += timedelta(days=1)
        
        return non_trading_days
    
    def get_trading_days_in_range(self, start_date: Union[str, datetime, date], 
                                 end_date: Union[str, datetime, date]) -> List[date]:
        """
        获取指定日期范围内的所有交易日
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            List[date]: 交易日列表
        """
        start = self._parse_date(start_date)
        end = self._parse_date(end_date)
        
        trading_days = []
        current_date = start
        
        while current_date <= end:
            if self.is_trading_day(current_date):
                trading_days.append(current_date)
            current_date += timedelta(days=1)
        
        return trading_days
    
    def get_next_trading_day(self, date_input: Union[str, datetime, date]) -> date:
        """
        获取指定日期的下一个交易日
        
        Args:
            date_input: 基准日期
            
        Returns:
            date: 下一个交易日
        """
        current_date = self._parse_date(date_input) + timedelta(days=1)
        
        while self.is_non_trading_day(current_date):
            current_date += timedelta(days=1)
        
        return current_date
    
    def get_previous_trading_day(self, date_input: Union[str, datetime, date]) -> date:
        """
        获取指定日期的上一个交易日
        
        Args:
            date_input: 基准日期
            
        Returns:
            date: 上一个交易日
        """
        current_date = self._parse_date(date_input) - timedelta(days=1)
        
        while self.is_non_trading_day(current_date):
            current_date -= timedelta(days=1)
        
        return current_date
    
    def get_holiday_info(self, date_input: Union[str, datetime, date]) -> dict:
        """
        获取指定日期的详细信息
        
        Args:
            date_input: 日期
            
        Returns:
            dict: 包含日期详细信息的字典
        """
        target_date = self._parse_date(date_input)
        
        info = {
            'date': target_date,
            'date_str': target_date.strftime('%Y-%m-%d'),
            'weekday': target_date.weekday(),  # 0=Monday, 6=Sunday
            'weekday_name': ['周一', '周二', '周三', '周四', '周五', '周六', '周日'][target_date.weekday()],
            'is_weekend': self.is_weekend(target_date),
            'is_holiday': self.is_holiday(target_date),
            'is_special_non_trading': self.is_special_non_trading_day(target_date),
            'is_non_trading_day': self.is_non_trading_day(target_date),
            'is_trading_day': self.is_trading_day(target_date),
            'holiday_name': None
        }
        
        # 获取节假日名称
        if target_date in self.china_holidays:
            info['holiday_name'] = self.china_holidays[target_date]
        
        return info
    
    def _parse_date(self, date_input: Union[str, datetime, date]) -> date:
        """
        解析不同格式的日期输入为date对象
        
        Args:
            date_input: 日期输入，支持字符串、datetime或date对象
            
        Returns:
            date: 解析后的date对象
        """
        if isinstance(date_input, str):
            # 支持常见的日期字符串格式
            if len(date_input) == 10:  # YYYY-MM-DD
                return datetime.strptime(date_input, '%Y-%m-%d').date()
            elif len(date_input) == 8:  # YYYYMMDD
                return datetime.strptime(date_input, '%Y%m%d').date()
            else:
                # 尝试使用pandas解析
                return pd.to_datetime(date_input).date()
        elif isinstance(date_input, datetime):
            return date_input.date()
        elif isinstance(date_input, date):
            return date_input
        else:
            raise ValueError(f"不支持的日期格式: {type(date_input)}")


# 创建全局实例
china_holiday_util = ChinaHolidayUtil()


def is_non_trading_day(date_input: Union[str, datetime, date]) -> bool:
    """
    快捷函数：判断是否为非交易日
    
    Args:
        date_input: 日期
        
    Returns:
        bool: 是否为非交易日
    """
    return china_holiday_util.is_non_trading_day(date_input)


def is_trading_day(date_input: Union[str, datetime, date]) -> bool:
    """
    快捷函数：判断是否为交易日
    
    Args:
        date_input: 日期
        
    Returns:
        bool: 是否为交易日
    """
    return china_holiday_util.is_trading_day(date_input)


def get_holiday_info(date_input: Union[str, datetime, date]) -> dict:
    """
    快捷函数：获取日期详细信息
    
    Args:
        date_input: 日期
        
    Returns:
        dict: 日期详细信息
    """
    return china_holiday_util.get_holiday_info(date_input)


def get_non_trading_days_in_month(year: int, month: int) -> List[dict]:
    """
    获取指定月份的所有非交易日信息（为前端日历组件优化）
    
    Args:
        year: 年份
        month: 月份
        
    Returns:
        List[dict]: 非交易日信息列表
    """
    # 计算月份的第一天和最后一天
    start_date = date(year, month, 1)
    if month == 12:
        end_date = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = date(year, month + 1, 1) - timedelta(days=1)
    
    non_trading_days = china_holiday_util.get_non_trading_days_in_range(start_date, end_date)
    
    # 返回详细信息
    result = []
    for day in non_trading_days:
        info = china_holiday_util.get_holiday_info(day)
        result.append({
            'date': day.strftime('%Y-%m-%d'),
            'type': 'holiday' if info['is_holiday'] else 'weekend',
            'name': info['holiday_name'] or '周末',
            'weekday': info['weekday_name']
        })
    
    return result


# 屏蔽pandas警告
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')
