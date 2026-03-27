<!-- 此段说明。 -->
<!-- API 映射 -->
# API 映射
<!-- 优先直接使用可视化器 -->
# # 优先直接使用可视化器

常规图表优先调用 `utils/visualizers/` 下的方法。
若需要新增图表类型，可在 `skills/visualization/scripts/chart_*.py` 增加函数，
并通过 `VisualizerManager.render_skill_chart` 进行动态调用。

| 领域 | 主要可视化方法 |
|---|---|
| stock | `StockVisualizer.plot_stock_kline` |
| stock | `StockVisualizer.plot_turnover_chart` |
| stock | `StockVisualizer.plot_stock_comparison` |
| stock | `StockVisualizer.plot_new_high_stock_kline` |
| stock | `StockVisualizer.calculate_ma_lines` |
| index | `IndexVisualizer.plot_index_kline` |
| index | `IndexVisualizer.plot_multi_index_kline` |
| index | `IndexVisualizer.plot_index_comparison` |
| index | `IndexVisualizer.calculate_ma_lines` |
| index | `IndexVisualizer.get_multi_index_kline_options` |
| sector | `SectorVisualizer.plot_sector_kline` |
| sector | `SectorVisualizer.plot_single_sector_kline` |
| sector | `SectorVisualizer.get_top_volume_sectors` |
| market | `MarketVisualizer.plot_market_metadata` |
| market | `MarketVisualizer.plot_change_distribution` |
| model | `ModelVisualizer.plot_model_one_stocks` |

<!-- 直接使用可视化器的场景 -->
# # 直接使用可视化器的场景（无管理器）

当没有封装器可用时，直接调用以下方法：

- `IndexVisualizer.plot_market_volume_chart`
- `IndexVisualizer.get_market_volume_chart_options`
- `SectorVisualizer.plot_sector_ranking`
- `SectorVisualizer.plot_sector_distribution`
- `MarketVisualizer.plot_market_sentiment`
