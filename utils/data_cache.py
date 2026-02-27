import os
import pickle
from datetime import datetime, timedelta, date
import json
import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Any

class DataCache:
    """数据缓存管理类"""

    def __init__(self, cache_dir: str = None):
        """初始化缓存管理器"""
        if cache_dir is None:
            # 确保使用项目根目录的data_cache
            import os
            current_file = os.path.abspath(__file__)
            project_root = os.path.dirname(os.path.dirname(current_file))
            cache_dir = os.path.join(project_root, "data_cache")
        self.cache_dir = cache_dir
        self.metadata_file = os.path.join(cache_dir, "metadata.json")
        
        # 定义子文件夹
        self.index_dir = os.path.join(cache_dir, "indices")
        self.stock_daily_dir = os.path.join(cache_dir, "stock_daily")
        self.stock_minute_dir = os.path.join(cache_dir, "stock_minute")
        self.other_dir = os.path.join(cache_dir, "other")
        
        # 确保所有目录存在
        self._ensure_dirs()
        
        self.metadata = self._load_metadata()
    
    def _ensure_dirs(self):
        """确保所有缓存目录存在"""
        for directory in [self.cache_dir, self.index_dir, self.stock_daily_dir, 
                         self.stock_minute_dir, self.other_dir]:
            os.makedirs(directory, exist_ok=True)
        
    def _load_metadata(self) -> dict:
        """加载元数据"""
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    def _save_metadata(self):
        """保存元数据"""
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, ensure_ascii=False, indent=2)
    
    def _get_data_type_dir(self, key: str) -> str:
        """根据键名确定使用哪个子文件夹"""
        if key.startswith("index_"):
            return self.index_dir
        elif key.startswith("stock_detail_"):
            return self.stock_daily_dir
        elif key.startswith("stock_minute_"):
            return self.stock_minute_dir
        else:
            return self.other_dir
    
    def _get_cache_path(self, key: str, date: str) -> str:
        """获取缓存文件路径，根据数据类型存放在不同的子文件夹中"""
        data_dir = self._get_data_type_dir(key)
        return os.path.join(data_dir, f"{key}_{date}.pkl")
    
    def needs_update(self, key: str, date: str) -> bool:
        """检查是否需要更新缓存"""
        cache_path = self._get_cache_path(key, date)
        
        # 如果文件不存在，需要更新
        if not os.path.exists(cache_path):
            return True
        
        # 如果是当天数据，检查文件修改时间，超过1小时则更新
        if date == datetime.now().strftime('%Y%m%d'):
            file_mtime = datetime.fromtimestamp(os.path.getmtime(cache_path))
            if datetime.now() - file_mtime > timedelta(hours=1):
                return True
        
        return False
    
    def save_data(self, key: str, date: str, data: pl.DataFrame) -> None:
        """保存数据到缓存"""
        cache_path = self._get_cache_path(key, date)
        with open(cache_path, 'wb') as f:
            pickle.dump(data, f)
        
        # 更新元数据
        self.metadata[key] = {
            'last_update': datetime.now().isoformat(),
            'rows': data.height,
            'columns': data.columns,
            'path': cache_path
        }
        self._save_metadata()
    
    def load_data(self, key: str, date: str) -> Optional[pl.DataFrame]:
        """从缓存加载数据"""
        cache_path = self._get_cache_path(key, date)
        if not os.path.exists(cache_path):
            return None
        
        try:
            with open(cache_path, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            print(f"加载缓存 {key}_{date} 失败: {str(e)}")
            return None
    
    def save_dict_data(self, key: str, date: str, data_dict: Dict[str, Any]) -> None:
        """保存字典数据到缓存"""
        cache_path = self._get_cache_path(key, date)
        with open(cache_path, 'wb') as f:
            pickle.dump(data_dict, f)
        
        # 更新元数据
        self.metadata[key] = {
            'last_update': datetime.now().isoformat(),
            'keys': list(data_dict.keys()),
            'path': cache_path
        }
        self._save_metadata()
    
    def load_dict_data(self, key: str, date: str, expected_keys: List[str] = None) -> Optional[Dict[str, Any]]:
        """从缓存加载字典数据"""
        cache_path = self._get_cache_path(key, date)
        if not os.path.exists(cache_path):
            return None
        
        try:
            with open(cache_path, 'rb') as f:
                data = pickle.load(f)
                
            # 检查是否包含所有期望的键
            if expected_keys and not all(k in data for k in expected_keys):
                print(f"缓存 {key}_{date} 缺少必要的键")
                return None
                
            return data
        except Exception as e:
            print(f"加载缓存 {key}_{date} 失败: {str(e)}")
            return None
    
    def get_available_dates(self, key: str) -> list:
        """获取指定数据类型的所有可用日期"""
        data_dir = self._get_data_type_dir(key)
        dates = []
        
        if not os.path.exists(data_dir):
            return dates
            
        for filename in os.listdir(data_dir):
            if filename.startswith(f"{key}_") and filename.endswith(".pkl"):
                date = filename.split('_')[-1].split('.')[0]
                dates.append(date)
        return sorted(dates)
    
    def clear_old_data(self, days_to_keep: int = 30) -> None:
        """清理旧数据"""
        today = datetime.now()
        cutoff_date = today - timedelta(days=days_to_keep)
        cleared_count = 0
        
        # 遍历所有子文件夹
        for directory in [self.index_dir, self.stock_daily_dir, self.stock_minute_dir, self.other_dir]:
            if not os.path.exists(directory):
                continue
                
            for filename in os.listdir(directory):
                if not filename.endswith('.pkl'):
                    continue
                    
                # 提取日期部分
                try:
                    date_str = filename.split('_')[-1].split('.')[0]
                    file_date = datetime.strptime(date_str, '%Y%m%d')
                    
                    if file_date < cutoff_date:
                        file_path = os.path.join(directory, filename)
                        os.remove(file_path)
                        cleared_count += 1
                        print(f"已删除旧缓存文件: {filename}")
                        
                        # 从元数据中移除
                        key = filename.replace(f"_{date_str}.pkl", "")
                        if key in self.metadata:
                            del self.metadata[key]
                except:
                    # 如果无法解析日期，跳过该文件
                    continue
        
        # 保存更新后的元数据
        self._save_metadata()
        print(f"共清理了 {cleared_count} 个过期缓存文件")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """获取缓存信息统计"""
        info = {
            "total_files": 0,
            "indices_count": len(os.listdir(self.index_dir)) if os.path.exists(self.index_dir) else 0,
            "stock_daily_count": len(os.listdir(self.stock_daily_dir)) if os.path.exists(self.stock_daily_dir) else 0,
            "stock_minute_count": len(os.listdir(self.stock_minute_dir)) if os.path.exists(self.stock_minute_dir) else 0,
            "other_count": len(os.listdir(self.other_dir)) if os.path.exists(self.other_dir) else 0,
            "last_update": datetime.now().isoformat()
        }
        
        info["total_files"] = (info["indices_count"] + info["stock_daily_count"] + 
                              info["stock_minute_count"] + info["other_count"])
        
        return info 