import json
import os
import time
import uuid
from typing import Any

import httpx


DEFAULT_BASE_URL = os.getenv("AGENT_OPENAI_BASE_URL", "https://api-inference.modelscope.cn/v1").rstrip("/")
DEFAULT_API_KEY = os.getenv("AGENT_OPENAI_API_KEY", "ms-7ae9b437-2d5d-47c9-b613-86e012766c2c").strip()
DEFAULT_MODEL = os.getenv("AGENT_OPENAI_MODEL", "MiniMax/MiniMax-M2.5").strip()
SUPPORTED_MODELS = {DEFAULT_MODEL}


def resolve_api_key(explicit_api_key: str | None = None) -> str:
    api_key = (explicit_api_key or DEFAULT_API_KEY or "").strip()
    return api_key


def normalize_model(model: str | None) -> str:
    raw_model = str(model or "").strip()
    if not raw_model:
        return DEFAULT_MODEL
    if raw_model in SUPPORTED_MODELS:
        return raw_model
    return DEFAULT_MODEL


def build_headers(api_key: str) -> dict[str, str]:
    return {
        "accept": "application/json",
        "authorization": f"Bearer {api_key}",
        "content-type": "application/json",
    }


def extract_error_detail(response: httpx.Response) -> str:
    try:
        raw_bytes = response.read()
    except Exception:
        raw_bytes = b""

    text = raw_bytes.decode("utf-8", errors="replace").strip()
    if text:
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return text

        if isinstance(data, dict):
            error_data = data.get("error")
            if isinstance(error_data, dict):
                message = str(
                    error_data.get("message")
                    or error_data.get("detail")
                    or error_data.get("type")
                    or ""
                ).strip()
                if message:
                    return message
            message = str(data.get("message") or data.get("detail") or "").strip()
            if message:
                return message
        return text

    return f"HTTP {response.status_code}"


def build_payload(
    messages: list[dict[str, str]],
    model: str,
    session_id: str,
    system_prompt: str,
    *,
    max_tokens: int = 1800,
) -> dict[str, Any]:
    openai_messages: list[dict[str, str]] = [{"role": "system", "content": system_prompt}]
    openai_messages.extend(
        {
            "role": str(item.get("role", "user")),
            "content": str(item.get("content", "")),
        }
        for item in messages
    )
    return {
        "model": normalize_model(model),
        "messages": openai_messages,
        "max_tokens": max_tokens,
        "temperature": 0.2,
        "stream": False,
        "user": session_id,
    }


def extract_text(response_json: dict[str, Any]) -> str:
    choices = response_json.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0] or {}
        message = first.get("message")
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, str):
                return content.strip()
            if isinstance(content, list):
                parts: list[str] = []
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        parts.append(str(item.get("text", "")))
                return "".join(parts).strip()
        text = first.get("text")
        if isinstance(text, str):
            return text.strip()
    return ""


def send_chat(
    messages: list[dict[str, str]],
    model: str,
    system_prompt: str,
    *,
    session_id: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    max_tokens: int = 1800,
) -> dict[str, Any]:
    resolved_api_key = resolve_api_key(api_key)
    if not resolved_api_key:
        raise RuntimeError("Missing model API key.")

    resolved_session_id = session_id or str(uuid.uuid4())
    payload = build_payload(messages, model, resolved_session_id, system_prompt, max_tokens=max_tokens)
    headers = build_headers(resolved_api_key)
    url = f"{(base_url or DEFAULT_BASE_URL).rstrip('/')}/chat/completions"
    timeout = httpx.Timeout(connect=30.0, read=300.0, write=300.0, pool=30.0)

    started_at = time.perf_counter()
    with httpx.Client(timeout=timeout) as client:
        response = client.post(url, headers=headers, json=payload)
        if response.is_error:
            detail = extract_error_detail(response)
            raise RuntimeError(f"Upstream HTTP {response.status_code}: {detail}")
        data = response.json()
        return {
            "session_id": resolved_session_id,
            "model": data.get("model", normalize_model(model)),
            "text": extract_text(data),
            "raw": data,
            "elapsed_ms": round((time.perf_counter() - started_at) * 1000, 2),
        }
