# 数据缓存布局

用于只读检查的路径地图。
路径相对仓库根目录。

顶层目录：

- `data_cache/indices/`
- `data_cache/sectors/`
- `data_cache/stock_daily/`
- `data_cache/stock_minute/`
- `data_cache/other/`
- `data_cache/strategy_watch/`
- `data_cache/metadata.json`
- `data_cache/stock_groups.json`

常见文件：

- `data_cache/indices/index_daily_metadata.parquet`
- `data_cache/indices/index_minute_metadata.parquet`
- `data_cache/sectors/sectors_ths.parquet`
- `data_cache/sectors/sectors_dc.parquet`
- `data_cache/stock_daily/stock_daily_metadata.parquet`
- `data_cache/other/market_metadata.parquet`
- `data_cache/other/market_states.parquet`

文件格式：

- `.parquet`：表格元数据与时间序列
- `.pkl`：缓存对象与中间结果
- `.json`：索引与分组
- `.xlsx`：板块成分表

策略追踪（strategy_watch）：

- `data_cache/strategy_watch/resources.json` 与 `conversations.json` 用于索引上传与爬取资源。
- 子目录通常包含原始上传、爬取文档与转换后的 markdown。

提示：

- parquet 与 json 建议用 polars 或 pandas。
- 大型 parquet 优先使用惰性读取。
- 若文件缺失，需提示缺失情况并确认是否执行过缓存生成流程。
