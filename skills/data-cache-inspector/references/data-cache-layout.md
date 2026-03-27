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

核心 metadata 文件：

- `data_cache/indices/index_daily_metadata.parquet`
- `data_cache/indices/index_minute_metadata.parquet`
- `data_cache/sectors/sectors_ths.parquet`
- `data_cache/sectors/sectors_dc.parquet`
- `data_cache/stock_daily/stock_daily_metadata.parquet`
- `data_cache/other/market_metadata.parquet`
- `data_cache/other/market_states.parquet`

常见字段（列名:类型，示例）：

- `index_daily_metadata.parquet`
  `日期:Date, 开盘:Float64, 收盘:Float64, 最高:Float64, 最低:Float64, 成交额:Float64, 代码:Utf8, 交易所:Utf8, 名称:Utf8, 5日涨跌幅:Float64, 10日涨跌幅:Float64, 20日涨跌幅:Float64, MA5:Float64, MA10:Float64, MA20:Float64`
- `index_minute_metadata.parquet`
  `时间:Datetime, 日期:Utf8, 沪交所成交额:Float64, 深交所成交额:Float64, 北交所成交额:Float64, 总成交额:Float64, 深交所累计成交额:Float64, 沪交所累计成交额:Float64, 北交所累计成交额:Float64, 总累计成交额:Float64`
- `stock_daily_metadata.parquet`
  `日期:Date, 开盘:Float64, 收盘:Float64, 最高:Float64, 最低:Float64, 成交量:Int64, 成交额:Float64, 换手率:Float64, 涨跌幅:Float64, 名称:Utf8, 代码:Utf8, 振幅:Float64, 涨跌额:Float64`
- `market_metadata.parquet`
  `日期:Date, 成交总额:Float64, 红盘率:Float64, 总股票数:Int64, 上涨股票数:Int64, 涨停数:Int64, 跌停数:Int64, 炸板数:Int64, 1连板数:Int64 ... 6连板数:Int64`
- `market_states.parquet`
  `日期:Date, 代码:Utf8, 名称:Utf8, 成交量:Int64, 成交额:Float64, 涨停:Boolean, 跌停:Boolean, 炸板:Boolean, 连板数:Int32, 连板天数:Utf8, MA5/MA10/MA20:Float64`
- `sectors_ths.parquet`
  `日期:Date, 板块代码:Utf8, 板块名称:Utf8, 板块类型:Utf8, 数据源:Utf8, 开盘/收盘/最高/最低:Float64, 成交量:Float64, 成交额:Float64, 成交额量比:Float64`
- `sectors_dc.parquet`
  `日期:Utf8, 板块代码:Utf8, 板块名称:Utf8, 板块类型:Utf8, 数据源:Utf8, 开盘/收盘/最高/最低:Float64, 成交量:Int64, 成交额:Float64, 涨跌幅:Float64, MA5/MA10/MA20:Float64`

策略追踪（strategy_watch，新布局）：

- `data_cache/strategy_watch/reference/resources.json`：资源索引
- `data_cache/strategy_watch/memory/conversations.json`：会话记录
- `data_cache/strategy_watch/memory/memory_profiles.json`：记忆画像档案
- `data_cache/strategy_watch/memory/memory_links.json`：资源-画像链接
- `data_cache/strategy_watch/memory/memory_portraits.json`：画像内容
- `data_cache/strategy_watch/strategies.json` 与 `strategies_index.json`：策略索引
- `data_cache/strategy_watch/logs/*.jsonl`：按对话名/策略名拆分的 Agent 日志

文件格式：

- `.parquet`：表格元数据与时间序列（优先输出列名和 dtype）
- `.pkl`：缓存对象与中间结果
- `.json`：索引与分组
- `.xlsx`：板块成分表

提示：

- parquet 与 json 建议用 polars 或 pandas。
- 大型 parquet 优先使用惰性读取，并只采样 schema 与必要列。
- 若文件缺失，需提示缺失情况并确认是否执行过缓存生成流程。
