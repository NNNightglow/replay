# utils/agents 说明

本目录是策略看盘多 Agent 能力层，负责：

- Agent 档案与系统提示词加载
- LLM 调用（AgentScope ReAct + OpenAI 兼容回退）
- 运行时事件回调（供上层记录日志/进度）
- 技能脚本调用（如数据缓存检查、文件转 Markdown、微信抓取）

## 目录结构

- `agentscope_runtime.py`：核心运行时，包含 Agent 配置、提示词加载、模型调用、流式输出、事件回调。
- `dataloaders.py`：数据读取辅助。
- `wx_crawler.py`：微信公众号文章抓取逻辑。
- `resource/`：各 Agent 独立提示词文件（`*_prompt.md`）。
- `__init__.py`：对外导出运行时能力（供 `utils/strategy_watch_api.py` 调用）。

## 运行链路（策略看盘）

上层入口在 `utils/strategy_watch_api.py`：

1. 根据会话模式选择 Agent（`dialog/crawler/strategy_edit/strategy_analysis`）。
2. 调用 `utils.agents` 导出的 `chat_with_agent` 或 `chat_with_agent_stream`。
3. `agentscope_runtime.py` 优先走 AgentScope ReAct；失败时按配置可回退到 OpenAI 兼容接口。
4. 运行事件通过 `set_runtime_event_hook(...)` 回传给上层，写入 `data_cache/strategy_watch/logs/*.jsonl`。

## 提示词加载规则

`agentscope_runtime.py` 会先加载内置默认提示词，再用 `resource/*_prompt.md` 覆盖。

- 例如：`resource/pm_prompt.md` 会覆盖 `pm_agent` 的系统提示词。
- 文件名支持别名映射（如 `architect_prompt.md` -> `architect_agent`）。

## 关键对外接口

由 `utils.agents` 暴露给上层：

- `list_agent_profiles()`：返回 Agent 元信息（阶段、上下游、挂载 skill）。
- `chat_with_agent(...)`：非流式对话。
- `chat_with_agent_stream(...)`：流式对话。
- `set_runtime_event_hook(hook)`：注册运行事件回调。
- `convert_file_to_markdown_via_skill(...)`：多格式转 Markdown。
- `crawl_wechat_articles_from_text(...)`：抓取微信文章并产出 Markdown。

## 环境变量

常用配置（项目根目录 `.env`）：

- `OPENAI_API_KEY` / `LLM_API_KEY`
- `OPENAI_BASE_URL`
- `OPENAI_MODEL`
- `OPENAI_TIMEOUT_SECONDS`
- `AGENTSCOPE_STRICT`：`1` 表示 AgentScope 失败直接报错；`0` 允许回退 OpenAI 兼容调用。

## 如何新增/修改 Agent

1. 在 `agentscope_runtime.py` 的 `AGENT_PROFILES` 增加或调整 Agent。
2. 在 `resource/` 新增对应 `*_prompt.md`（推荐）。
3. 如需新能力，在 `SKILL_REGISTRY` 注册 skill，并绑定到目标 Agent 的 `skill_ids`。
4. 若上层需要新会话模式，更新 `CONVERSATION_MODE_AGENT_MAP`，并在 `strategy_watch_api.py` 接入。

## 如何只改提示词

推荐只改 `resource/*.md`，无需改 Python 逻辑：

- `pm_prompt.md`
- `architect_prompt.md`
- `engineer_prompt.md`
- `analyst_prompt.md`
- `visualization_prompt.md`
- `dialog_prompt.md`

## 注意事项

- `agentscope_runtime.py` 里存在默认中文提示词常量，可能有历史编码问题；实际运行优先使用 `resource/` 下独立提示词。
- 运行日志按会话/策略名称分桶写入 `data_cache/strategy_watch/logs/`。
- 策略编辑编排（PM -> Architect -> 子任务执行）主逻辑在 `strategy_watch_api.py`，不在本目录内。
