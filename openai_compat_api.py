import json
import logging
import re
import time
import uuid
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from urllib.error import HTTPError, URLError

import send_chat_request as bridge


app = FastAPI(title="SharedChat OpenAI-Compatible Bridge")

logger = logging.getLogger("openai_compat")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.FileHandler("openai_compat.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)

SENSITIVE_KEYS = {
    "authorization",
    "api_key",
    "apikey",
    "token",
    "cookie",
    "bearer_token",
    "conduit_token",
}


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: ("<redacted>" if key.lower() in SENSITIVE_KEYS else redact(child))
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


def log_request_summary(body: dict[str, Any]) -> None:
    messages = body.get("messages") or []
    tool_names = available_tool_names(body)
    summary = {
        "model": body.get("model"),
        "stream": body.get("stream"),
        "message_roles": [msg.get("role") for msg in messages if isinstance(msg, dict)],
        "message_count": len(messages) if isinstance(messages, list) else None,
        "has_tools": bool(body.get("tools")),
        "tool_count": len(body.get("tools") or []),
        "tool_names": tool_names,
        "tool_choice": body.get("tool_choice"),
        "response_format": body.get("response_format"),
        "temperature": body.get("temperature"),
        "max_tokens": body.get("max_tokens") or body.get("max_completion_tokens"),
    }
    logger.info("request_summary %s", json.dumps(redact(summary), ensure_ascii=False))
    with open("openai_compat_last_request.json", "w", encoding="utf-8") as file:
        json.dump(redact(body), file, ensure_ascii=False, indent=2)


def extract_content_text(content: Any) -> str:
    if isinstance(content, str):
        return content

    if isinstance(content, list):
        parts = []
        for item in content:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "text" and isinstance(item.get("text"), str):
                parts.append(item["text"])
            elif item.get("type") == "input_text" and isinstance(item.get("text"), str):
                parts.append(item["text"])
        return "\n".join(parts)

    return ""


def messages_to_prompt(messages: list[dict[str, Any]]) -> str:
    if not messages:
        raise HTTPException(status_code=400, detail="messages must not be empty")

    rendered = []
    for message in messages:
        role = message.get("role", "user")
        content = extract_content_text(message.get("content"))
        tool_calls = message.get("tool_calls")
        tool_call_id = message.get("tool_call_id")
        if not content and not tool_calls:
            continue

        if role in {"system", "developer"}:
            rendered.append(f"System: {content}")
        elif role == "assistant":
            if tool_calls:
                rendered.append(
                    "Assistant tool calls: "
                    + json.dumps(tool_calls, ensure_ascii=False)
                )
            if content:
                rendered.append(f"Assistant: {content}")
        elif role == "tool":
            prefix = f"Tool result ({tool_call_id})" if tool_call_id else "Tool result"
            rendered.append(f"{prefix}: {content}")
        else:
            rendered.append(f"User: {content}")

    if not rendered:
        raise HTTPException(status_code=400, detail="messages did not contain text content")

    return "\n\n".join(rendered)


def tools_to_prompt(body: dict[str, Any]) -> str:
    tools = body.get("tools")
    if not isinstance(tools, list) or not tools:
        return ""

    lines = [
        "Tool instructions:",
        "You are connected through an OpenAI-compatible bridge. If tools are available, do not invent tool results.",
        "For tool-capable clients such as Cline, you must use tools instead of returning conversational prose.",
        "When you need to use a tool, output exactly one JSON object and no extra prose:",
        '{"tool_calls":[{"name":"tool_name","arguments":{}}]}',
        "If the task is complete, call attempt_completion with a result.",
        "If files must be created or edited, prefer apply_patch with the exact patch content.",
        "If you need to inspect files, call read_file/list_files/search_files before guessing.",
        "Available tools:",
    ]
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        function = tool.get("function") if isinstance(tool.get("function"), dict) else tool
        name = function.get("name")
        description = function.get("description", "")
        parameters = function.get("parameters", {})
        if not name:
            continue
        lines.append(f"- {name}: {description}")
        if parameters:
            lines.append(f"  parameters: {json.dumps(parameters, ensure_ascii=False)}")

    tool_choice = body.get("tool_choice")
    if tool_choice:
        lines.append(f"tool_choice: {json.dumps(tool_choice, ensure_ascii=False)}")

    return "\n".join(lines)


def build_prompt(body: dict[str, Any]) -> str:
    prompt = messages_to_prompt(body["messages"])
    tool_prompt = tools_to_prompt(body)
    response_format = body.get("response_format")

    extras = []
    if tool_prompt:
        extras.append(tool_prompt)
    if response_format:
        extras.append(f"Response format: {json.dumps(response_format, ensure_ascii=False)}")

    if extras:
        return prompt + "\n\n" + "\n\n".join(extras)
    return prompt


def iter_tool_functions(body: dict[str, Any]):
    tools = body.get("tools")
    if not isinstance(tools, list):
        return

    for tool in tools:
        if not isinstance(tool, dict):
            continue
        function = tool.get("function") if isinstance(tool.get("function"), dict) else tool
        if isinstance(function, dict) and function.get("name"):
            yield function


def available_tool_names(body: dict[str, Any]) -> list[str]:
    return [function["name"] for function in iter_tool_functions(body)]


def make_tool_call(name: str, arguments: dict[str, Any] | str | None = None) -> dict[str, Any]:
    if arguments is None:
        arguments = {}
    if not isinstance(arguments, str):
        arguments = json.dumps(arguments, ensure_ascii=False)

    return {
        "id": f"call_{uuid.uuid4().hex}",
        "type": "function",
        "function": {
            "name": name,
            "arguments": arguments,
        },
    }


def save_last_upstream_answer(answer: str, source: str) -> None:
    preview = {
        "source": source,
        "answer_chars": len(answer),
        "answer": answer,
    }
    with open("openai_compat_last_upstream_answer.json", "w", encoding="utf-8") as file:
        json.dump(preview, file, ensure_ascii=False, indent=2)


def make_attempt_completion_call(body: dict[str, Any], answer: str) -> dict[str, Any] | None:
    if "attempt_completion" not in available_tool_names(body):
        return None

    if ("tool_calls" in answer or "apply_patch" in answer) and "*** Begin Patch" in answer:
        result = (
            "The upstream model returned a malformed tool call that could not be "
            "converted into a valid client tool invocation. Please retry the request."
        )
    else:
        result = answer.strip() or "Task completed."
    arguments: dict[str, Any] = {"result": result}

    # Cline accepts optional task_progress on most tools. Supplying a compact,
    # completed checklist helps it understand that this is the terminal step.
    arguments["task_progress"] = "- [x] Responded to the user's request"
    return make_tool_call("attempt_completion", arguments)


def make_fallback_tool_call(body: dict[str, Any], answer: str) -> dict[str, Any] | None:
    code_patch = make_apply_patch_from_answer(body, answer)
    if code_patch:
        return code_patch
    return make_attempt_completion_call(body, answer)


def make_apply_patch_from_answer(body: dict[str, Any], answer: str) -> dict[str, Any] | None:
    if "apply_patch" not in available_tool_names(body):
        return None

    code = extract_best_code_block(answer)
    if not code:
        return None

    filename = infer_filename_from_answer(answer)
    if not filename:
        return None

    patch_lines = [
        '%%bash',
        'apply_patch <<"EOF"',
        '*** Begin Patch',
        f'*** Add File: {filename}',
    ]
    patch_lines.extend(f"+{line}" for line in code.splitlines())
    patch_lines.extend([
        '*** End Patch',
        'EOF',
    ])
    return make_tool_call(
        "apply_patch",
        {
            "input": "\n".join(patch_lines),
            "task_progress": "- [x] Create the requested file",
        },
    )


def extract_best_code_block(answer: str) -> str | None:
    blocks = re.findall(r"```([a-zA-Z0-9_+-]*)\s*\n(.*?)```", answer, flags=re.DOTALL)
    if not blocks:
        return None

    preferred_languages = {"html", "css", "javascript", "js", "typescript", "ts", "python", "py", "json"}
    for language, code in blocks:
        if language.lower() in preferred_languages:
            return code.strip("\n")

    return max((code.strip("\n") for _, code in blocks), key=len, default=None)


def infer_filename_from_answer(answer: str) -> str | None:
    filename_match = re.search(
        r"([A-Za-z0-9_.-]+\.(?:html|css|js|ts|tsx|jsx|py|json|md|txt))",
        answer,
    )
    if filename_match:
        return filename_match.group(1)

    lower_answer = answer.lower()
    if "<!doctype html" in lower_answer or "<html" in lower_answer:
        return "beautiful_landing_page.html"

    return None


def make_completion_response(request_body: dict[str, Any], answer: str) -> dict[str, Any]:
    model = request_body.get("model") or bridge.MODEL
    completion_id = f"chatcmpl-{uuid.uuid4().hex}"
    now = int(time.time())
    message: dict[str, Any] = {
        "role": "assistant",
        "content": answer,
    }
    finish_reason = "stop"

    tool_calls = parse_available_tool_calls(request_body, answer)
    fallback_tool_call = False
    if not tool_calls:
        fallback_call = make_fallback_tool_call(request_body, answer)
        if fallback_call:
            tool_calls = [fallback_call]
            fallback_tool_call = True

    if tool_calls:
        message = {
            "role": "assistant",
            "content": None,
            "tool_calls": tool_calls,
        }
        finish_reason = "tool_calls"

    response = {
        "id": completion_id,
        "object": "chat.completion",
        "created": now,
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": message,
                "finish_reason": finish_reason,
            }
        ],
        "usage": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
    }
    logger.info(
        "completion_packaged %s",
        json.dumps(
            {
                "finish_reason": finish_reason,
                "tool_calls": len(tool_calls),
                "fallback_tool_call": fallback_tool_call,
            },
            ensure_ascii=False,
        ),
    )
    with open("openai_compat_last_response.json", "w", encoding="utf-8") as file:
        json.dump(response, file, ensure_ascii=False, indent=2)
    return response


def parse_tool_calls(answer: str) -> list[dict[str, Any]]:
    repaired_calls = parse_broken_tool_calls(answer)
    if repaired_calls:
        return repaired_calls

    parsed = extract_tool_json(answer)
    if not parsed:
        return []

    if isinstance(parsed, list):
        raw_calls = parsed
    else:
        raw_calls = (
            parsed.get("tool_calls")
            or parsed.get("tools")
            or parsed.get("function_calls")
        )

    if not isinstance(raw_calls, list):
        if isinstance(parsed, dict) and (
            parsed.get("name") or isinstance(parsed.get("function"), dict)
        ):
            raw_calls = [parsed]
        else:
            return []

    calls = []
    for call in raw_calls:
        parsed_call = normalize_tool_call(call)
        if parsed_call:
            calls.append(parsed_call)

    return calls


def parse_broken_tool_calls(answer: str) -> list[dict[str, Any]]:
    if '"tool_calls"' not in answer and "'tool_calls'" not in answer:
        return []

    name_match = re.search(r'["\']name["\']\s*:\s*["\']([^"\']+)["\']', answer)
    if not name_match:
        return []

    name = name_match.group(1)
    if name == "apply_patch":
        input_text = extract_apply_patch_input(answer)
        if input_text:
            return [
                make_tool_call(
                    "apply_patch",
                    {
                        "input": input_text,
                        "task_progress": "- [x] Create or update the requested file",
                    },
                )
            ]

    # If the model tried to call attempt_completion but produced invalid JSON,
    # preserve the useful result text without leaking the raw tool wrapper.
    if name == "attempt_completion":
        result_match = re.search(r'["\']result["\']\s*:\s*["\'](.*?)["\']\s*(?:,|})', answer, flags=re.DOTALL)
        result = result_match.group(1) if result_match else answer
        return [make_tool_call("attempt_completion", {"result": result})]

    return []


def extract_apply_patch_input(answer: str) -> str | None:
    patch_text = extract_patch_text(answer)
    if patch_text:
        return patch_text

    input_match = re.search(
        r'["\']input["\']\s*:\s*["\'](.*?)(?:"\s*,\s*["\']task_progress|"\s*}\s*]\s*})',
        answer,
        flags=re.DOTALL,
    )
    if input_match:
        return decode_jsonish_string(input_match.group(1))

    patch_match = re.search(
        r'(%%bash\s+apply_patch\s+<<["\']EOF["\']\s+.*?\*\*\* End Patch\s+EOF)',
        answer,
        flags=re.DOTALL,
    )
    if patch_match:
        return decode_jsonish_string(patch_match.group(1))

    patch_body_match = re.search(
        r'(\*\*\* Begin Patch\s+.*?\*\*\* End Patch)',
        answer,
        flags=re.DOTALL,
    )
    if patch_body_match:
        return '%%bash\napply_patch <<"EOF"\n' + decode_jsonish_string(patch_body_match.group(1)) + '\nEOF'

    return None


def extract_patch_text(answer: str) -> str | None:
    begin = answer.find("*** Begin Patch")
    end = answer.find("*** End Patch", begin)
    if begin == -1 or end == -1:
        return None

    end += len("*** End Patch")
    prefix_start = answer.rfind("%%bash", 0, begin)
    if prefix_start == -1:
        prefix_start = begin

    suffix_end = end
    suffix = answer[end:]
    eof_match = re.search(r"(?:\\n|\r?\n)\s*EOF\b", suffix)
    if eof_match:
        suffix_end = end + eof_match.end()

    patch_text = answer[prefix_start:suffix_end]
    patch_text = decode_jsonish_string(patch_text)
    if patch_text.startswith("*** Begin Patch"):
        patch_text = '%%bash\napply_patch <<"EOF"\n' + patch_text + "\nEOF"
    return patch_text


def decode_jsonish_string(value: str) -> str:
    value = value.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\t", "\t")
    value = value.replace('\\"', '"').replace("\\'", "'").replace("\\\\", "\\")
    return value.strip()


def extract_tool_json(answer: str) -> Any:
    stripped = answer.strip()
    candidates = []
    if stripped:
        candidates.append(stripped)

    fenced_blocks = re.findall(r"```(?:json)?\s*(.*?)```", answer, flags=re.DOTALL | re.I)
    candidates.extend(block.strip() for block in fenced_blocks)

    first_brace = answer.find("{")
    last_brace = answer.rfind("}")
    if first_brace != -1 and last_brace > first_brace:
        candidates.append(answer[first_brace:last_brace + 1])

    first_bracket = answer.find("[")
    last_bracket = answer.rfind("]")
    if first_bracket != -1 and last_bracket > first_bracket:
        candidates.append(answer[first_bracket:last_bracket + 1])

    for candidate in candidates:
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue

    return None


def normalize_tool_call(call: Any) -> dict[str, Any] | None:
    if not isinstance(call, dict):
        return None

    function = call.get("function") if isinstance(call.get("function"), dict) else {}
    name = call.get("name") or function.get("name")
    arguments = call.get("arguments")
    if arguments is None:
        arguments = function.get("arguments", {})
    if not name:
        return None

    if isinstance(arguments, str):
        try:
            json.loads(arguments)
        except json.JSONDecodeError:
            arguments = {"input": arguments}
    elif arguments is None:
        arguments = {}
    elif not isinstance(arguments, dict):
        arguments = {"value": arguments}

    return make_tool_call(name, arguments)


def filter_available_tool_calls(
    body: dict[str, Any],
    tool_calls: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    names = set(available_tool_names(body))
    if not names:
        return tool_calls

    filtered = [
        tool_call
        for tool_call in tool_calls
        if tool_call.get("function", {}).get("name") in names
    ]
    dropped = len(tool_calls) - len(filtered)
    if dropped:
        logger.info("dropped_unavailable_tool_calls %s", dropped)
    return filtered


def parse_available_tool_calls(body: dict[str, Any], answer: str) -> list[dict[str, Any]]:
    return filter_available_tool_calls(body, parse_tool_calls(answer))


def stream_tool_call_chunks(
    completion_id: str,
    model: str,
    tool_calls: list[dict[str, Any]],
) -> list[str]:
    chunks = []
    for index, tool_call in enumerate(tool_calls):
        tool_chunk = {
            "id": completion_id,
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "tool_calls": [
                            {
                                "index": index,
                                "id": tool_call["id"],
                                "type": "function",
                                "function": tool_call["function"],
                            }
                        ]
                    },
                    "finish_reason": None,
                }
            ],
        }
        chunks.append(f"data: {json.dumps(tool_chunk, ensure_ascii=False)}\n\n")
    return chunks


def stream_completion_response(request_body: dict[str, Any], prompt: str):
    model = request_body.get("model") or bridge.MODEL
    completion_id = f"chatcmpl-{uuid.uuid4().hex}"
    now = int(time.time())
    has_tools = bool(request_body.get("tools"))

    role_chunk = {
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": now,
        "model": model,
        "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
    }
    yield f"data: {json.dumps(role_chunk, ensure_ascii=False)}\n\n"

    profile = bridge.select_profile()
    buffered_answer = ""
    stream_error = None
    try:
        for item in bridge.stream_ai(prompt, profile):
            if item.get("type") != "delta":
                continue

            delta_text = item.get("delta", "")
            buffered_answer += delta_text
            if has_tools:
                continue

            content_chunk = {
                "id": completion_id,
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "delta": {"content": delta_text},
                        "finish_reason": None,
                    }
                ],
            }
            yield f"data: {json.dumps(content_chunk, ensure_ascii=False)}\n\n"
    except Exception as exc:
        buffered_answer = f"[bridge error] {exc}"
        stream_error = str(exc)
        if not has_tools:
            error_chunk = {
                "id": completion_id,
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "delta": {"content": f"\n{buffered_answer}"},
                        "finish_reason": None,
                    }
                ],
            }
            yield f"data: {json.dumps(error_chunk, ensure_ascii=False)}\n\n"

    tool_calls = parse_available_tool_calls(request_body, buffered_answer) if has_tools else []
    save_last_upstream_answer(buffered_answer, "stream")
    fallback_tool_call = False
    if has_tools and not tool_calls:
        fallback_call = make_fallback_tool_call(request_body, buffered_answer)
        if fallback_call:
            tool_calls = [fallback_call]
            fallback_tool_call = True

    if tool_calls:
        for chunk in stream_tool_call_chunks(completion_id, model, tool_calls):
            yield chunk

    elif has_tools and buffered_answer:
        content_chunk = {
            "id": completion_id,
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "delta": {"content": buffered_answer},
                    "finish_reason": None,
                }
            ],
        }
        yield f"data: {json.dumps(content_chunk, ensure_ascii=False)}\n\n"

    done_chunk = {
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls" if tool_calls else "stop"}],
    }
    stream_summary = {
        "answer_chars": len(buffered_answer),
        "tool_calls": len(tool_calls),
        "fallback_tool_call": fallback_tool_call,
        "finish_reason": "tool_calls" if tool_calls else "stop",
        "error": stream_error,
    }
    logger.info("stream_done %s", json.dumps(stream_summary, ensure_ascii=False))
    with open("openai_compat_last_stream_response.json", "w", encoding="utf-8") as file:
        json.dump(stream_summary, file, ensure_ascii=False, indent=2)
    yield f"data: {json.dumps(done_chunk, ensure_ascii=False)}\n\n"
    yield "data: [DONE]\n\n"


def call_bridge(prompt: str) -> dict[str, Any]:
    profile = bridge.select_profile()
    return bridge.ask_ai(prompt, profile)


@app.on_event("startup")
def startup() -> None:
    bridge.start_ws_refresh_workers(None if bridge.START_NEW_CONVERSATION else bridge.CONVERSATION_ID)


@app.on_event("shutdown")
def shutdown() -> None:
    bridge._ws_refresh_stop.set()


@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "profiles": [bridge.profile_name(profile) for profile in bridge.PROFILES]}


@app.get("/v1/models")
def list_models() -> dict[str, Any]:
    model = bridge.MODEL
    return {
        "object": "list",
        "data": [
            {
                "id": model,
                "object": "model",
                "created": 0,
                "owned_by": "sharedchat-bridge",
            }
        ],
    }


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body = await request.json()
    log_request_summary(body)
    messages = body.get("messages")
    if not isinstance(messages, list):
        raise HTTPException(status_code=400, detail="messages must be a list")

    prompt = build_prompt(body)

    if body.get("stream") is True:
        logger.info("stream_start")
        return StreamingResponse(
            stream_completion_response(body, prompt),
            media_type="text/event-stream",
        )

    try:
        result = call_bridge(prompt)
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(status_code=exc.code, detail=error_body)
    except URLError as exc:
        raise HTTPException(status_code=502, detail=str(exc.reason))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    answer = result.get("answer") or ""
    save_last_upstream_answer(answer, "non_stream")
    logger.info(
        "response_summary %s",
        json.dumps(
            {
                "answer_chars": len(answer),
                "profile": result.get("profile"),
                "conversation_id": result.get("conversation_id"),
                "topic_id": result.get("topic_id"),
                "tool_calls": len(parse_available_tool_calls(body, answer)),
            },
            ensure_ascii=False,
        ),
    )

    return JSONResponse(make_completion_response(body, answer))
