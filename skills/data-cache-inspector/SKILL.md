---
name: data-cache-inspector
description: 检查并汇总 `data_cache` 下的市场数据缓存（股票、指数、板块、元数据、strategy_watch）。用于回答“缓存了什么数据、数量、最新日期”，或预览缓存文件（parquet、pkl、json、xlsx）。
---

# 数据缓存检查器

在不修改文件的前提下检查市场数据缓存。
优先采用只读流程，报告数据存在哪、包含什么、是否最新。

# 工作流程

1. 确认缓存根目录为 `data_cache`，列出一级目录。
2. 使用 `references/data-cache-layout.md` 定位子目录与文件类型。
3. 汇总可用性：数量、体积、最新时间戳。
4. 如需预览，仅读取少量样本，报告字段与日期范围。
5. 明确指出缺失、空目录或过期数据。

# 读取辅助

- 使用 `utils/data_cache.py` 获取规范路径与辅助方法。
- 使用 `utils/metadata/index_data_manager.py`、`utils/metadata/stock_data_manager.py`、`utils/metadata/sector_data_manager.py` 了解字段与结构。

# 安全预览规则

- 不修改、不覆盖缓存文件。
- 大型 parquet 仅使用 `head()` 或 `limit()` 采样。
- 读取 `.pkl` 或 `.xlsx` 时仅加载必要行/列。

# 输出要求

报告以下信息：

- 检查的具体缓存路径
- 文件数量与体积概览（必要时）
- 样本字段与行数
- 推断日期范围或最新日期（如存在）
- 造成限制的缺失或空缓存说明
