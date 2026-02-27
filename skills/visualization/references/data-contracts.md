<!-- 此段说明。 -->
<!-- 数据契约 -->
# 数据契约
<!-- 通用规则 -->
# # 通用规则

- 输入通常为 `polars.DataFrame`。
- 日期字段应为 `?
- 绘图前需按日期升序排序。

<!-- K 线族 -->
# # K 线族

必需字段：

- `?
- `?or `?
- `?or `?
- `?or `?
- `?or `?

<!-- 可选字段 -->
# # 可选字段

可选：

- `?(preferred for amount bar)
- `?(fallback)

适用方法：

- `UniversalKlineChart.plot_kline_with_volume`
- `StockVisualizer.plot_stock_kline`
- `IndexVisualizer.plot_index_kline`
- `SectorVisualizer.plot_single_sector_kline`

<!-- 股票换手率 -->
# # 股票换手率
<!-- 必填字段 -->
# # 必填字段

必需：

- `?
- `?

<!-- 推荐字段 -->
# # 推荐字段

推荐：

- `? `?(for color logic)

适用方法：

- `StockVisualizer.plot_turnover_chart`

<!-- 多股票对比 -->
# # 多股票对比
<!-- 输入 -->
# # 输入

输入：

- `Dict[str, pl.DataFrame]`

每个数据框需包含：

- `?
- `?

适用方法：

- `StockVisualizer.plot_stock_comparison`

<!-- 板块排行/分布 -->
# # 板块排行/分布

排行所需：

- `?
- `?or chosen value column

分布所需：

- `?

成交额榜所需：

- `?
- `?
- `?

适用方法：

- `SectorVisualizer.get_top_volume_sectors`
- `SectorVisualizer.plot_sector_ranking`
- `SectorVisualizer.plot_sector_distribution`

<!-- 市场元数据组 -->
# # 市场元数据组

`plot_market_metadata` 预期字段：

- `market_states`：包含炸板/梯队分析所需字段，通常为 `? `? `? 及涨跌停价格相关字段。
- `market_metadata`：包含红盘比与涨跌停统计所需字段，通常为 `? `? `, `? `?

<!-- 模型结果 -->
# # 模型结果

`plot_model_one_stocks` 期望列表项包含：

- `code`
- `name`
- `score`
