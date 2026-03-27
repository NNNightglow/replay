---
name: data-cache-inspector
description: 查询并分析 `data_cache` 下的市场数据（股票、指数、板块、市场元数据、strategy_watch）。支持按日期/列/条件读取实际数据行，并进行对比分析（如两日成交额变化）。
---

# 数据缓存查询与分析器

在不修改文件的前提下查询并分析市场数据缓存。
优先采用只读流程，先确认数据可用性，再读取实际数据并输出结论。

# 工作流程

1. 确认缓存根目录为 `data_cache`，列出一级目录。
2. 使用 `references/data-cache-layout.md` 定位子目录与文件类型。
3. 先做可用性盘点：数量、体积、最新时间戳、Schema 与日期范围。
4. 根据问题调用查询工具读取实际数据行（日期过滤、列过滤、条件筛选）。
5. 计算并输出对比指标（差值、环比、占比、排序等）。
6. 明确指出缺失、空目录、字段缺失或超出缓存范围。

# 读取辅助

- 使用 `utils/data_cache.py` 获取规范路径与辅助方法。
- 使用 `utils/metadata/index_data_manager.py`、`utils/metadata/stock_data_manager.py`、`utils/metadata/sector_data_manager.py` 了解字段与结构。
- Skill 脚本入口：`scripts/data_cache_gateway.py`
- 在 Agent Tool-Calling 中优先使用：
  - `data_cache_query_table`：按表名、日期范围、列名、筛选条件读取实际数据行。
  - `compare_market_turnover_dates`：直接比较两日 `market_metadata` 的 `成交总额`。

# 安全预览规则

- 不修改、不覆盖缓存文件。
- 大型 parquet 仅使用 `head()` 或 `limit()` 采样。
- 读取 `.pkl` 或 `.xlsx` 时仅加载必要行/列。
- 列级 Schema 优先从 parquet 元数据读取（避免全量加载）。

# 输出要求

报告以下信息：

- 查询结果中的关键数值（差值、百分比等）
