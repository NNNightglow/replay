#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import inspect
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests


BASE_DIR = Path(__file__).resolve().parent.parent.parent
_AGENTSCOPE_READY = False

try:
    import agentscope  # type: ignore
    from agentscope.message import Msg  # type: ignore
    from agentscope.agents import ReActAgent  # type: ignore

    _AGENTSCOPE_AGENT_CLASS = ReActAgent
    _AGENTSCOPE_AVAILABLE = True
except Exception:
    agentscope = None
    Msg = None
    ReActAgent = None
    _AGENTSCOPE_AGENT_CLASS = None
    _AGENTSCOPE_AVAILABLE = False



AGENT_PROFILES: Dict[str, Dict[str, Any]] = {
    # Stage 1: 总控规划（入口）
    "planner_agent": {
        "display_name": "Planner Agent",
        "description": "策略规划与任务拆解，可在无上传文件时直接对话。",
        "sys_prompt": (
            "你是A股策略规划助手。先澄清目标，再给出可执行步骤、关键风险和检查点。"
        ),
        "stage": 1,
        "upstream_agents": [],
        "downstream_agents": ["plan_task_agent"],
        "skill_ids": ["goal_clarification", "risk_checkpointing"],
    },
    # Stage 2: 任务编排
    "plan_task_agent": {
        "display_name": "Plan & Task Agent",
        "description": "把目标拆解为分层任务清单与执行顺序。",
        "sys_prompt": (
            "你是任务编排助手。把用户目标拆成按优先级排序的任务，输出里程碑、依赖、产出物。"
        ),
        "stage": 2,
        "upstream_agents": ["planner_agent"],
        "downstream_agents": ["crawler_agent", "data_processing_agent"],
        "skill_ids": ["task_decomposition", "dependency_planning"],
    },
    # Stage 3: 数据获取
    "crawler_agent": {
        "display_name": "Crawler Agent",
        "description": "爬取微信公众号文章并保存，再做内容总结。",
        "sys_prompt": (
            "你是爬虫助手。识别并处理微信公众号文章链接，输出抓取结果、失败原因和后续建议。"
        ),
        "stage": 3,
        "upstream_agents": ["plan_task_agent"],
        "downstream_agents": ["data_processing_agent"],
        "skill_ids": ["wechat_article_crawl", "crawl_failure_diagnosis"],
    },
    # Stage 4: 数据处理
    "data_processing_agent": {
        "display_name": "Data Processing Agent",
        "description": "数据清洗、特征构造、统计分析建议。",
        "sys_prompt": (
            "你是数据处理助手。聚焦数据口径、清洗规则、缺失值策略、特征与验证步骤。"
        ),
        "stage": 4,
        "upstream_agents": ["plan_task_agent", "crawler_agent"],
        "downstream_agents": ["visualization_agent"],
        "skill_ids": ["data_cleaning", "feature_engineering", "stat_validation", "data-cache-inspector"],
    },
    # Stage 5: 结果表达
    "visualization_agent": {
        "display_name": "Visualization Agent",
        "description": "图表与看板设计，解释可视化编码选择。",
        "sys_prompt": (
            "你是可视化助手。为金融场景设计图表方案，说明图表类型、字段映射、交互和误读风险。"
        ),
        "stage": 5,
        "upstream_agents": ["data_processing_agent"],
        "downstream_agents": [],
        "skill_ids": ["chart_design", "dashboard_layout", "visual_risk_review", "visualization"],
    },
}

AGENT_ROOT = "planner_agent"
AGENT_PROFILE_FALLBACK = AGENT_ROOT

SKILL_REGISTRY: Dict[str, Dict[str, str]] = {
    "goal_clarification": {
        "display_name": "Goal Clarification",
        "description": "澄清目标、边界条件与验收标准。",
    },
    "risk_checkpointing": {
        "display_name": "Risk Checkpointing",
        "description": "识别关键风险并设置检查点。",
    },
    "task_decomposition": {
        "display_name": "Task Decomposition",
        "description": "把目标拆解为可执行任务。",
    },
    "dependency_planning": {
        "display_name": "Dependency Planning",
        "description": "梳理任务依赖与执行顺序。",
    },
    "wechat_article_crawl": {
        "display_name": "WeChat Article Crawl",
        "description": "提取并抓取微信公众号文章内容。",
    },
    "crawl_failure_diagnosis": {
        "display_name": "Crawl Failure Diagnosis",
        "description": "分析抓取失败原因并给出修复建议。",
    },
    "data_cleaning": {
        "display_name": "Data Cleaning",
        "description": "口径统一、异常值与缺失值处理。",
    },
    "feature_engineering": {
        "display_name": "Feature Engineering",
        "description": "构建特征并评估稳定性。",
    },
    "stat_validation": {
        "display_name": "Stat Validation",
        "description": "统计检验、样本偏差与结果验证。",
    },
    "chart_design": {
        "display_name": "Chart Design",
        "description": "选择图表类型并定义字段映射。",
    },
    "dashboard_layout": {
        "display_name": "Dashboard Layout",
        "description": "构建多图联动与看板布局。",
    },
    "visual_risk_review": {
        "display_name": "Visual Risk Review",
        "description": "识别可视化误读风险并补充说明。",
    },
    "visualization": {
        "display_name": "Visualization",
        "description": "A-share visualization routing and data contract checks for stock/index/sector/market charts.",
    },
    "data-cache-inspector": {
        "display_name": "Data Cache Inspector",
        "description": "Inspect cached market data in data_cache (stocks, indices, sectors, metadata) and summarize what is available.",
    },
}


def _assert_agent_graph_valid() -> None:
    known_agents = set(AGENT_PROFILES.keys())
    for agent_name, profile in AGENT_PROFILES.items():
        upstream = profile.get("upstream_agents", [])
        downstream = profile.get("downstream_agents", [])
        skill_ids = profile.get("skill_ids", [])

        if not isinstance(upstream, list) or not all(isinstance(item, str) for item in upstream):
            raise RuntimeError(f"Invalid upstream_agents in {agent_name}.")
        if not isinstance(downstream, list) or not all(isinstance(item, str) for item in downstream):
            raise RuntimeError(f"Invalid downstream_agents in {agent_name}.")
        if not isinstance(skill_ids, list) or not all(isinstance(item, str) for item in skill_ids):
            raise RuntimeError(f"Invalid skill_ids in {agent_name}.")

        unknown_upstream = [item for item in upstream if item not in known_agents]
        unknown_downstream = [item for item in downstream if item not in known_agents]
        if unknown_upstream:
            raise RuntimeError(f"Unknown upstream agent(s) for {agent_name}: {unknown_upstream}")
        if unknown_downstream:
            raise RuntimeError(f"Unknown downstream agent(s) for {agent_name}: {unknown_downstream}")

    visiting = set()
    visited = set()

    def walk(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            raise RuntimeError(f"Cycle detected in AGENT_PROFILES graph around: {node}")
        visiting.add(node)
        for child in AGENT_PROFILES[node].get("downstream_agents", []):
            walk(child)
        visiting.remove(node)
        visited.add(node)

    for agent_name in AGENT_PROFILES.keys():
        walk(agent_name)


def register_skill(skill_id: str, display_name: str = "", description: str = "") -> None:
    clean_id = (skill_id or "").strip()
    if not clean_id:
        raise ValueError("skill_id can not be empty.")
    SKILL_REGISTRY[clean_id] = {
        "display_name": (display_name or clean_id).strip(),
        "description": (description or "").strip(),
    }


def bind_skill_to_agent(agent_name: str, skill_id: str, auto_register: bool = True) -> None:
    profile = AGENT_PROFILES.get(agent_name)
    if not profile:
        raise ValueError(f"Unknown agent: {agent_name}")

    clean_id = (skill_id or "").strip()
    if not clean_id:
        raise ValueError("skill_id can not be empty.")

    if clean_id not in SKILL_REGISTRY and auto_register:
        register_skill(clean_id)

    skill_ids = profile.setdefault("skill_ids", [])
    if clean_id not in skill_ids:
        skill_ids.append(clean_id)


def get_agent_profile(agent_name: str) -> Dict[str, Any]:
    return AGENT_PROFILES.get(agent_name) or AGENT_PROFILES[AGENT_PROFILE_FALLBACK]


_assert_agent_graph_valid()


def _load_env_files() -> None:
    for filename in (".env", "env"):
        env_path = BASE_DIR / filename
        if not env_path.exists() or not env_path.is_file():
            continue
        try:
            lines = env_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            continue

        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if key.startswith("export "):
                key = key.replace("export ", "", 1).strip()
            if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
                value = value[1:-1]
            if key and key not in os.environ:
                os.environ[key] = value


def _build_history_block(history: List[Dict]) -> str:
    lines = []
    for item in history[-24:]:
        role = item.get("role", "")
        content = (item.get("content") or "").strip()
        if role in {"user", "assistant"} and content:
            lines.append(f"[{role}] {content}")
    return "\n".join(lines)


def _get_agent_prompt(agent_name: str) -> str:
    profile = get_agent_profile(agent_name)
    return profile["sys_prompt"]


def _compose_context_blocks(resource_context: str, extra_context: str) -> List[str]:
    blocks: List[str] = []
    if resource_context:
        blocks.append("Resource context (Markdown):\n\n" + resource_context)
    if extra_context:
        blocks.append("Extra context:\n\n" + extra_context)
    return blocks


def _env_flag(name: str, default: str = "0") -> bool:
    raw = (os.getenv(name) or default).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _normalize_history_messages(
    history_messages: List[Dict[str, Any]],
    user_content: str,
) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    for item in history_messages[-24:]:
        role = (item.get("role") or "").strip()
        content = (item.get("content") or "").strip()
        if role in {"user", "assistant"} and content:
            normalized.append({"role": role, "content": content})

    user_content = (user_content or "").strip()
    if user_content:
        if not normalized or normalized[-1].get("role") != "user" or normalized[-1].get("content") != user_content:
            normalized.append({"role": "user", "content": user_content})
    return normalized


def _build_openai_messages(
    sys_prompt: str,
    resource_context: str,
    extra_context: str,
    history_messages: List[Dict[str, Any]],
    user_content: str,
) -> List[Dict[str, Any]]:
    messages: List[Dict[str, Any]] = [{"role": "system", "content": sys_prompt}]
    for block in _compose_context_blocks(resource_context, extra_context):
        messages.append({"role": "system", "content": block})
    messages.extend(_normalize_history_messages(history_messages, user_content))
    return messages


def _ensure_agentscope_init(model_name: str, base_url: str, api_key: str, temperature: float) -> None:
    global _AGENTSCOPE_READY
    if not _AGENTSCOPE_AVAILABLE or _AGENTSCOPE_READY:
        return

    agentscope.init(
        model_configs=[
            {
                "config_name": "stock_llm",
                "model_type": "openai_chat",
                "model_name": model_name,
                "api_key": api_key,
                "client_args": {"base_url": base_url},
                "generate_args": {"temperature": temperature},
            }
        ],
        logger_level="WARNING",
    )
    _AGENTSCOPE_READY = True



def _build_agentscope_agent(agent_name: str, sys_prompt: str):
    if _AGENTSCOPE_AGENT_CLASS is None:
        raise RuntimeError("AgentScope ReActAgent is not available.")

    cls = _AGENTSCOPE_AGENT_CLASS
    signatures = inspect.signature(cls.__init__).parameters

    def _pick_kwargs(raw: Dict[str, Any]) -> Dict[str, Any]:
        if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in signatures.values()):
            return raw
        return {k: v for k, v in raw.items() if k in signatures}

    candidates = [
        {"name": agent_name, "sys_prompt": sys_prompt, "model_config_name": "stock_llm"},
        {"name": agent_name, "system_prompt": sys_prompt, "model_config_name": "stock_llm"},
        {"name": agent_name, "model_config_name": "stock_llm"},
        {"name": agent_name},
    ]
    last_error = None
    for item in candidates:
        kwargs = _pick_kwargs(item)
        try:
            return cls(**kwargs)
        except Exception as exc:  # pragma: no cover - runtime compatibility branch
            last_error = exc
            continue

    if last_error:
        raise last_error
    raise RuntimeError(f"Failed to build agentscope agent for {agent_name}.")


def _extract_reply_content(reply: Any) -> str:
    content = getattr(reply, "content", "")
    if content:
        return str(content).strip()
    return str(reply).strip()


def _call_openai_compatible(
    messages: List[Dict],
    model_name: str,
    base_url: str,
    api_key: str,
    temperature: float,
) -> Tuple[str, Dict[str, Any]]:
    response = requests.post(
        f"{base_url.rstrip('/')}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model_name,
            "messages": messages,
            "temperature": max(0.0, min(float(temperature), 2.0)),
        },
        timeout=int(os.getenv("OPENAI_TIMEOUT_SECONDS", "180")),
    )
    if response.status_code >= 400:
        raise RuntimeError(f"LLM interface error {response.status_code}: {response.text[:400]}")
    data = response.json()
    choices = data.get("choices") or []
    if not choices:
        raise RuntimeError("LLM returned empty choices.")
    message = (choices[0] or {}).get("message") or {}
    content = message.get("content", "")
    usage = data.get("usage") or {}
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                parts.append(item.get("text") or "")
            else:
                parts.append(str(item))
        content = "".join(parts)
    return (str(content) or "").strip() or "Model returned empty content.", usage





def _stream_openai_compatible(
    messages: List[Dict],
    model_name: str,
    base_url: str,
    api_key: str,
    temperature: float,
    usage_collector: Dict[str, Any] = None,
):
    include_usage = os.getenv("OPENAI_STREAM_INCLUDE_USAGE", "1").strip() != "0"
    payload = {
        "model": model_name,
        "messages": messages,
        "temperature": max(0.0, min(float(temperature), 2.0)),
        "stream": True,
    }
    if include_usage:
        payload["stream_options"] = {"include_usage": True}
    response = requests.post(
        f"{base_url.rstrip('/')}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        stream=True,
        timeout=int(os.getenv("OPENAI_TIMEOUT_SECONDS", "180")),
    )
    if response.status_code >= 400:
        raise RuntimeError(f"LLM interface error {response.status_code}: {response.text[:400]}")

    for raw_line in response.iter_lines(decode_unicode=True):
        if not raw_line:
            continue
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            break
        try:
            payload = json.loads(data)
        except Exception:
            continue
        usage = payload.get("usage")
        if usage_collector is not None and isinstance(usage, dict):
            usage_collector["usage"] = usage
        choices = payload.get("choices") or []
        if not choices:
            continue
        delta = (choices[0] or {}).get("delta") or {}
        chunk = delta.get("content") or ""
        if chunk:
            yield str(chunk)





def list_agent_profiles() -> List[Dict]:
    sorted_profiles = sorted(
        AGENT_PROFILES.items(),
        key=lambda item: (int(item[1].get("stage", 10_000)), item[0]),
    )
    result = []
    for name, profile in sorted_profiles:
        skill_ids = list(profile.get("skill_ids", []))
        skills = []
        for skill_id in skill_ids:
            skill_info = SKILL_REGISTRY.get(skill_id) or {}
            skills.append(
                {
                    "skill_id": skill_id,
                    "display_name": skill_info.get("display_name", skill_id),
                    "description": skill_info.get("description", ""),
                }
            )
        result.append(
            {
                "name": name,
                "display_name": profile["display_name"],
                "description": profile["description"],
                "stage": int(profile.get("stage", 10_000)),
                "upstream_agents": list(profile.get("upstream_agents", [])),
                "downstream_agents": list(profile.get("downstream_agents", [])),
                "skill_ids": skill_ids,
                "skills": skills,
            }
        )
    return result


def chat_with_agent(
    agent_name: str,
    user_content: str,
    history_messages: List[Dict],
    resource_context: str = "",
    extra_context: str = "",
    model_name: str = "",
    temperature: float = 0.2,
) -> Tuple[str, str, str, Dict[str, Any]]:
    _load_env_files()

    api_key = (os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY / LLM_API_KEY is not configured.")

    resolved_model = (model_name or os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()
    base_url = (os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").strip()

    sys_prompt = _get_agent_prompt(agent_name)
    history_block = _build_history_block(history_messages)
    composed_user = user_content
    if history_block:
        composed_user = (
            "History summary:\n"
            f"{history_block}\n\n"
            "User question:\n"
            f"{user_content}"
        )
    context_blocks = _compose_context_blocks(resource_context, extra_context)

    # 默认关闭 AgentScope 包装，优先保持上游请求前缀稳定，提升服务端缓存命中率。
    if _AGENTSCOPE_AVAILABLE and _env_flag("AGENTSCOPE_ENABLED", "0"):
        try:
            _ensure_agentscope_init(
                model_name=resolved_model,
                base_url=base_url,
                api_key=api_key,
                temperature=temperature,
            )
            agent = _build_agentscope_agent(agent_name=agent_name, sys_prompt=sys_prompt)
            composed_user_for_agent = composed_user
            if context_blocks:
                composed_user_for_agent = "\n\n".join(context_blocks) + "\n\nUser question:\n" + composed_user
            try:
                reply = agent(
                    Msg(
                        name="user",
                        role="user",
                        content=composed_user_for_agent,
                    )
                )
            except TypeError:
                reply = agent(composed_user_for_agent)

            content = _extract_reply_content(reply)
            return content.strip(), resolved_model, "agentscope", {}
        except Exception:
            pass

    messages = _build_openai_messages(
        sys_prompt=sys_prompt,
        resource_context=resource_context,
        extra_context=extra_context,
        history_messages=history_messages,
        user_content=user_content,
    )
    fallback_reply, usage = _call_openai_compatible(
        messages=messages,
        model_name=resolved_model,
        base_url=base_url,
        api_key=api_key,
        temperature=temperature,
    )
    return fallback_reply, resolved_model, "openai_compatible_fallback", usage









def chat_with_agent_stream(
    agent_name: str,
    user_content: str,
    history_messages: List[Dict],
    resource_context: str = "",
    extra_context: str = "",
    model_name: str = "",
    temperature: float = 0.2,
):
    _load_env_files()

    api_key = (os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY / LLM_API_KEY is not configured.")

    resolved_model = (model_name or os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()
    base_url = (os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").strip()

    sys_prompt = _get_agent_prompt(agent_name)
    history_block = _build_history_block(history_messages)
    composed_user = user_content
    if history_block:
        composed_user = (
            "History summary:\n"
            f"{history_block}\n\n"
            "User question:\n"
            f"{user_content}"
        )
    context_blocks = _compose_context_blocks(resource_context, extra_context)

    if _AGENTSCOPE_AVAILABLE and _env_flag("AGENTSCOPE_ENABLED", "0"):
        try:
            content, resolved_model, provider, _usage = chat_with_agent(
                agent_name=agent_name,
                user_content=user_content,
                history_messages=history_messages,
                resource_context=resource_context,
                extra_context=extra_context,
                model_name=model_name,
                temperature=temperature,
            )

            def _single():
                if content:
                    yield content

            return _single(), resolved_model, provider, {}
        except Exception:
            pass

    messages = _build_openai_messages(
        sys_prompt=sys_prompt,
        resource_context=resource_context,
        extra_context=extra_context,
        history_messages=history_messages,
        user_content=user_content,
    )
    usage_collector: Dict[str, Any] = {}
    stream = _stream_openai_compatible(
        messages=messages,
        model_name=resolved_model,
        base_url=base_url,
        api_key=api_key,
        temperature=temperature,
        usage_collector=usage_collector,
    )
    return stream, resolved_model, "openai_compatible_stream", usage_collector
