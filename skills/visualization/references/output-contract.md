<!-- 此段说明。 -->
<!-- 输出契约 -->
# 输出契约
<!-- 输出类型 -->
# # 输出类型
<!-- 1）嵌入式 HTML -->
# # 1) 嵌入式 HTML

- 类型：`str`
- 来源：大多数使用 `render_embed()` 的 `plot_*` 方法
- 适用：前端直接注入图表 HTML

<!-- 2）ECharts 选项 JSON -->
# # 2) ECharts 选项 JSON

- 类型：`dict`
- 来源：
  - `IndexVisualizer.get_multi_index_kline_options`
  - `IndexVisualizer.get_market_volume_chart_options`
- 适用：前端渲染器直接处理 options

<!-- 3）多组件字典 -->
# # 3) 多组件字典

- 类型：`Dict[str, str]`
- 来源：`MarketVisualizer.plot_market_metadata`
- 适用：前端在一个页面渲染多个图表块

<!-- 错误与空数据状态 -->
# # 错误与空数据状态

- 对空输入或校验失败返回简洁的 HTML 提示块。
- 需要包含缺失字段或不支持请求的细节。

<!-- 推荐响应载荷（API 层） -->
# # 推荐响应载荷（API 层）

当通过 API 包装图表生成时，建议返回：

- `chart_type`：`embedded_html` / `echarts_option` / `chart_map`
- `method`：使用的方法名
- `required_columns_checked`：已校验的字段列表
- `content`：图表内容
- `warnings`：可选的注意事项列表
