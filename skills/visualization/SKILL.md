---

<!-- 名称 -->
# 名称

name: visualization

<!-- 描述 -->
# # 描述

description: 构建并路由 A 股可视化任务到 `utils/visualizers` 与 `utils/visualizer_manager.py` 的正确图表管线。用于用户请求个股、指数、板块、市场情绪或模型结果图表，且需要判断所需输入字段、方法选择与输出格式（嵌入式 HTML 或 ECharts 选项）。

---

<!-- A 股可视化 -->
# # A 股可视化

将可视化请求路由到稳定的图表 API。
在绘图前校验数据契约，并输出前端可直接使用的结果。

<!-- 工作流程 -->
# # 工作流程

1. 按领域分类请求。
2. 选择精确的图表 API。
3. 校验必需字段与数据质量。
4. 生成符合约定的输出格式。
5. 报告限制与降级行为。

<!-- 步骤 1：分类请求 -->
# # 步骤 1：分类请求

将请求映射到一个领域：

- `stock`：个股 K 线、换手率、多股对比、新高股视图
- `index`：指数 K 线、多指数对比、盘中成交量对比
- `sector`：板块 K 线、成交额榜、排行、分布
- `market`：情绪、涨跌停、炸板、连板梯队
- `model`：模型选股结果可视化

不确定时先查看 `references/api-map.md`。

<!-- 步骤 2：选择 API -->
# # 步骤 2：选择 API

优先使用 `VisualizerManager` 的门面方法。
仅在管理器未覆盖所需变体时直接调用可视化器方法。
关键路由规则：

- K 线相关优先使用 `UniversalKlineChart.plot_kline_with_volume` 系列。
- 前端需要 ECharts 选项时使用 `IndexVisualizer.get_multi_index_kline_options`。
- 前端需要嵌入式 HTML 时使用 `plot_*().render_embed()` 路径。

<!-- 步骤 3：校验数据契约 -->
# # 步骤 3：校验数据契约

绘图前执行以下检查：

- 数据框非空。
- 必需字段存在。
- 日期字段排序与字符串格式归一。

<!-- 使用方法 -->
# # 使用方法

使用：

- `ChartUtils.validate_data_columns`
- `ChartUtils.prepare_chart_data`

所需字段参考 `references/data-contracts.md`。

<!-- 步骤 4：生成输出 -->
# # 步骤 4：生成输出

返回以下之一：

- `embedded_html`：`render_embed()` 返回的字符串，用于直接页面渲染
- `echarts_option`：前端自定义渲染使用的 `dict`
- `chart_map`：多组件 HTML 字典（市场元数据组）

必须说明：

- 使用的方法
- 必需输入字段
- 已知限制或降级模式

<!-- 步骤 5：处理失败 -->
# # 步骤 5：处理失败

失败需要明确、可执行：

- 空输入：返回简洁的“无数据”提示。
- 缺字段：列出缺失字段。
- 不支持请求：给出最接近的可支持图表及所需补充数据。

避免静默失败。

<!-- 参考资料 -->
# # 参考资料

- `references/api-map.md`：领域到方法的路由表
- `references/data-contracts.md`：各图表所需字段契约
- `references/output-contract.md`：返回类型与前端集成约定

<!-- 资源文件 -->
# # 资源文件

- `assets/visualization-request-template.md`：可复用的请求模板
