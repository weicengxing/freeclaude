import json
import importlib.util
import logging
import os
import time
import uuid
from typing import Any, Iterator

import httpx


logger = logging.getLogger(__name__)
HTTP2_AVAILABLE = importlib.util.find_spec("h2") is not None

DEFAULT_BASE_URL = (
    os.getenv("UUAPI_BASE_URL")
    or os.getenv("ANTHROPIC_BASE_URL")
    or "https://uuapi.net"
).rstrip("/")
DEFAULT_MODEL = "claude-opus-4-6"
OPUS_47_MODEL = "claude-opus-4-7"
SUPPORTED_MODELS = {"claude-opus-4-6", "claude-sonnet-4-6", OPUS_47_MODEL}
MODEL_ALIASES = {
    "opus4.7": OPUS_47_MODEL,
    "opus-4.7": OPUS_47_MODEL,
    "claude-opus-4.7": OPUS_47_MODEL,
}
SUPPORTED_IMAGE_MEDIA_TYPES = {
    "image/jpeg",
    "image/png",
    "image/webp",
    "image/gif",
}
DEFAULT_BETA = ",".join(
    [
        "claude-code-20250219",
        "context-1m-2025-08-07",
        "interleaved-thinking-2025-05-14",
        "redact-thinking-2026-02-12",
        "context-management-2025-06-27",
        "prompt-caching-scope-2026-01-05",
        "effort-2025-11-24",
    ]
)
DEFAULT_SYSTEM_PROMPT = [
    {
        "type": "text",
        "text": "x-anthropic-billing-header: cc_version=2.1.110.610; cc_entrypoint=cli; cch=00000;",
    },
    {
        "type": "text",
        "text": "You are Claude Code, Anthropic's official CLI for Claude.",
        "cache_control": {"type": "ephemeral"},
    },
]


def resolve_api_key(explicit_api_key: str | None = None) -> str:
    api_key = (
        explicit_api_key
        or os.getenv("UUAPI_API_KEY")
        or os.getenv("CLAUDE_PROXY_UPSTREAM_API_KEY")
        or os.getenv("ANTHROPIC_AUTH_TOKEN", "")
    )
    return api_key.strip()


def normalize_model(model: str | None) -> str:
    raw_model = str(model or "").strip()
    if not raw_model:
        return DEFAULT_MODEL

    normalized_map = {name.lower(): name for name in SUPPORTED_MODELS}
    normalized_map.update({alias.lower(): target for alias, target in MODEL_ALIASES.items()})
    resolved = normalized_map.get(raw_model.lower())
    if resolved in SUPPORTED_MODELS:
        return resolved
    return DEFAULT_MODEL


def build_headers(api_key: str, session_id: str) -> dict[str, str]:
    return {
        "accept": "application/json",
        "anthropic-dangerous-direct-browser-access": "true",
        "anthropic-version": "2023-06-01",
        "anthropic-beta": DEFAULT_BETA,
        "authorization": f"Bearer {api_key}",
        "content-type": "application/json",
        "user-agent": "claude-cli/2.1.110 (external, cli)",
        "x-app": "cli",
        "x-claude-code-session-id": session_id,
        "x-stainless-lang": "js",
        "x-stainless-os": "Windows",
        "x-stainless-arch": "x64",
        "x-stainless-package-version": "0.81.0",
        "x-stainless-runtime": "node",
        "x-stainless-runtime-version": "v25.9.0",
        "x-stainless-retry-count": "0",
        "x-stainless-timeout": "300",
        "accept-language": "*",
        "sec-fetch-mode": "cors",
        "accept-encoding": "gzip, deflate",
    }


def create_httpx_client(timeout: httpx.Timeout) -> httpx.Client:
    return httpx.Client(timeout=timeout, http2=HTTP2_AVAILABLE)


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


def to_claude_message(message: dict[str, Any]) -> dict[str, Any]:
    role = str(message.get("role", "user"))
    text = str(message.get("content", "") or "")
    images = message.get("images")
    content: list[dict[str, Any]] = []

    if role == "user":
        raw_images = images if isinstance(images, list) else []
        if not raw_images and isinstance(message.get("image"), dict):
            raw_images = [message["image"]]

        for image in raw_images:
            if not isinstance(image, dict):
                continue
            media_type = str(image.get("media_type", "")).strip().lower()
            data = str(image.get("data", "")).strip()
            if media_type in SUPPORTED_IMAGE_MEDIA_TYPES and data:
                content.append(
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": data,
                        },
                    }
                )

    if text or not content:
        content.append(
            {
                "type": "text",
                "text": text,
            }
        )

    return {
        "role": role,
        "content": content,
    }


def build_payload(messages: list[dict[str, Any]], model: str, session_id: str, stream: bool = False) -> dict[str, Any]:
    return {
        "model": normalize_model(model),
        "max_tokens": 2048,
        "system": DEFAULT_SYSTEM_PROMPT,
        "messages": [to_claude_message(item) for item in messages],
        "thinking": {"type": "adaptive"},
        "metadata": {
            "user_id": json.dumps(
                {
                    "device_id": "kg-rag-web-chat",
                    "account_uuid": "",
                    "session_id": session_id,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        },
        "output_config": {"effort": "medium"},
        "context_management": {
            "edits": [
                {
                    "type": "clear_thinking_20251015",
                    "keep": "all",
                }
            ]
        },
        "tools": [],
        "stream": stream,
    }


def extract_text(response_json: dict[str, Any]) -> str:
    chunks = []
    for item in response_json.get("content", []):
        if item.get("type") == "text":
            chunks.append(item.get("text", ""))
    return "".join(chunks).strip()


def send_chat(
    messages: list[dict[str, Any]],
    model: str,
    session_id: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
) -> dict[str, Any]:
    resolved_api_key = resolve_api_key(api_key)
    if not resolved_api_key:
        raise RuntimeError("Missing UUAPI API key. Set UUAPI_API_KEY or CLAUDE_PROXY_UPSTREAM_API_KEY.")

    resolved_session_id = session_id or str(uuid.uuid4())
    payload = build_payload(messages, model, resolved_session_id, stream=False)
    headers = build_headers(resolved_api_key, resolved_session_id)
    url = f"{(base_url or DEFAULT_BASE_URL).rstrip('/')}/v1/messages?beta=true"
    timeout = httpx.Timeout(connect=30.0, read=300.0, write=300.0, pool=30.0)

    with create_httpx_client(timeout) as client:
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
        }


def iter_stream_chat(
    messages: list[dict[str, Any]],
    model: str,
    session_id: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
) -> Iterator[dict[str, Any]]:
    resolved_api_key = resolve_api_key(api_key)
    if not resolved_api_key:
        raise RuntimeError("Missing UUAPI API key. Set UUAPI_API_KEY or CLAUDE_PROXY_UPSTREAM_API_KEY.")

    resolved_session_id = session_id or str(uuid.uuid4())
    payload = build_payload(messages, model, resolved_session_id, stream=True)
    headers = build_headers(resolved_api_key, resolved_session_id)
    headers["accept"] = "text/event-stream"
    # Streaming responses are latency-sensitive; compression often causes the
    # first SSE event to be buffered for too long by proxies/CDNs.
    headers["accept-encoding"] = "identity"
    headers["cache-control"] = "no-cache"
    url = f"{(base_url or DEFAULT_BASE_URL).rstrip('/')}/v1/messages?beta=true"
    timeout = httpx.Timeout(connect=30.0, read=300.0, write=300.0, pool=30.0)

    stream_started_at = time.perf_counter()
    with create_httpx_client(timeout) as client:
        with client.stream("POST", url, headers=headers, json=payload) as response:
            if response.is_error:
                detail = extract_error_detail(response)
                raise RuntimeError(f"Upstream HTTP {response.status_code}: {detail}")
            headers_ready_ms = (time.perf_counter() - stream_started_at) * 1000
            if headers_ready_ms >= 1500:
                logger.warning(
                    "UUAPI stream headers slow: %.0fms model=%s session_id=%s",
                    headers_ready_ms,
                    normalize_model(model),
                    resolved_session_id,
                )
            elif not HTTP2_AVAILABLE:
                logger.info(
                    "HTTP/2 unavailable, using HTTP/1.1 for stream model=%s session_id=%s",
                    normalize_model(model),
                    resolved_session_id,
                )
            current_event = "message"
            first_event_logged = False

            for raw_line in response.iter_lines():
                if not raw_line:
                    continue

                line = raw_line.strip()
                if not line:
                    continue

                if not first_event_logged:
                    first_event_logged = True
                    first_event_ms = (time.perf_counter() - stream_started_at) * 1000
                    if first_event_ms >= 1500:
                        logger.warning(
                            "UUAPI first SSE line slow: %.0fms model=%s session_id=%s",
                            first_event_ms,
                            normalize_model(model),
                            resolved_session_id,
                        )

                if line.startswith("event:"):
                    current_event = line.split(":", 1)[1].strip()
                    continue

                if not line.startswith("data:"):
                    continue

                data_str = line.split(":", 1)[1].strip()
                if not data_str or data_str == "[DONE]":
                    continue

                yield {
                    "event": current_event,
                    "data": json.loads(data_str),
                    "session_id": resolved_session_id,
                }
