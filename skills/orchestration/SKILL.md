---
name: orchestration
description: "跨角色协同编排与任务规划：先澄清需求，再拆解任务、定义依赖与里程碑，并形成可执行行动清单。"
---

# 编排与规划（Orchestration）

用于需要多角色协作（PM、Architect、Engineer、Analyst、Visualization）的策略任务。

## 适用场景

- 需求不完整，需要先澄清目标、约束和验收标准。
- 任务复杂，需要分阶段交接与依赖管理。
- 需要输出可执行 ToDo，而不是泛化建议。

## 标准流程

1. 明确目标、边界、约束、验收标准。
2. 判定当前阶段负责人和下一阶段负责人。
3. 将目标拆解为最小可执行任务。
4. 标注任务依赖、执行顺序与里程碑。
5. 为每个任务定义输入/输出与参数要求。
6. 记录风险、待确认项、检查点与回滚方案。
7. 输出下一步可直接执行的行动清单。

## 输出契约

- `objective`: 一句话目标
- `constraints`: 关键约束
- `scope`: in-scope / out-of-scope
- `stage_plan`: 分阶段计划（含 owner）
- `tasks`: 任务清单（最小可执行单元）
- `dependencies`: 任务依赖映射
- `milestones`: 里程碑与交付物
- `handoff_artifacts`: 阶段交接产物
- `acceptance`: 验收标准
- `risks_and_checks`: 风险 + 缓解 + 检查点
- `fallback`: 回滚/降级方案
- `next_actions`: 立即可执行行动

## 任务字段规范（建议）

每个 `task` 包含：

- `task_id`
- `title`
- `agent`
- `description`
- `params`
- `depends_on`
- `milestone`
- `acceptance`
- `risk`
- `rollback`

## 护栏

- 不要无节制增加角色，优先复用现有角色与技能。
- 先确认需求清晰，再进入实现与分析。
- 计划必须可执行、可验证，避免抽象口号。
- 优先走 MVP 路径，先交付主链路，再做扩展。
