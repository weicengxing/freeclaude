import base64
import binascii
import json
import logging
import os
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
DATA_URL_PREFIX = "data:"
IMAGE_INPUT_DIR = os.path.abspath(".bridge_images")
WEB_SEARCH_TOOL_TYPES = {"web_search", "web_search_preview", "web_search_preview_2025_03_11"}


def redact_large_or_sensitive_string(value: str) -> str:
    if value.startswith(DATA_URL_PREFIX):
        mime = value[5:].split(";", 1)[0] or "application/octet-stream"
        return f"<redacted data URL: {mime}, {len(value)} chars>"
    if len(value) > 20000:
        return f"<redacted large string: {len(value)} chars>"
    return value


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: ("<redacted>" if key.lower() in SENSITIVE_KEYS else redact(child))
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return redact_large_or_sensitive_string(value)
    return value


def image_extension_from_mime(mime_type: str) -> str:
    mapping = {
        "image/png": ".png",
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/webp": ".webp",
        "image/gif": ".gif",
        "image/bmp": ".bmp",
    }
    return mapping.get(mime_type.lower(), ".img")


def save_data_url_image(data_url: str) -> str | None:
    match = re.match(r"^data:(image/[a-zA-Z0-9.+-]+);base64,(.*)$", data_url, flags=re.DOTALL)
    if not match:
        return None

    mime_type = match.group(1)
    encoded = re.sub(r"\s+", "", match.group(2))
    try:
        image_bytes = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        logger.info("image_data_url_decode_failed %s", json.dumps({"mime": mime_type}, ensure_ascii=False))
        return None

    os.makedirs(IMAGE_INPUT_DIR, exist_ok=True)
    path = os.path.join(IMAGE_INPUT_DIR, f"input_{uuid.uuid4().hex}{image_extension_from_mime(mime_type)}")
    with open(path, "wb") as file:
        file.write(image_bytes)
    logger.info(
        "saved_input_image %s",
        json.dumps({"path": path, "mime": mime_type, "bytes": len(image_bytes)}, ensure_ascii=False),
    )
    return path


def image_prompt_from_url(image_url: str) -> str:
    if image_url.startswith(DATA_URL_PREFIX):
        path = save_data_url_image(image_url)
        if path:
            return (
                "[image input saved by bridge]\n"
                f"Local path: {path}\n"
                "If a view_image tool is available, call it with this exact path before answering."
            )
        return "[image input received but bridge could not decode the data URL]"
    return (
        "[image input URL received]\n"
        f"URL: {image_url}\n"
        "If you have an image or browser tool available, inspect it before answering."
    )


def extract_image_prompt(item: dict[str, Any]) -> str:
    saved_path = item.get("_bridge_saved_image_path")
    if isinstance(saved_path, str) and saved_path:
        return (
            "[image input saved by bridge]\n"
            f"Local path: {saved_path}\n"
            "If a view_image tool is available, call it with this exact path before answering."
        )

    image_url = item.get("image_url") or item.get("url")
    if isinstance(image_url, dict):
        image_url = image_url.get("url")
    if isinstance(image_url, str) and image_url.strip():
        prompt = image_prompt_from_url(image_url.strip())
        match = re.search(r"^Local path: (.+)$", prompt, flags=re.MULTILINE)
        if match:
            item["_bridge_saved_image_path"] = match.group(1).strip()
        return prompt

    file_id = item.get("file_id")
    if isinstance(file_id, str) and file_id.strip():
        return f"[image input references file_id: {file_id.strip()}]"

    return "[image input received but no supported image_url was found]"


def log_request_summary(body: dict[str, Any]) -> None:
    messages = body.get("messages") or []
    tool_names = available_tool_names(body)
    summary = {
        "model": body.get("model"),
        "upstream_model": bridge.resolve_model(body.get("model")),
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


def log_responses_request_summary(body: dict[str, Any]) -> None:
    input_value = body.get("input")
    summary = {
        "model": body.get("model"),
        "upstream_model": bridge.resolve_model(body.get("model")),
        "stream": body.get("stream"),
        "input_type": type(input_value).__name__,
        "input_count": len(input_value) if isinstance(input_value, list) else None,
        "has_instructions": bool(body.get("instructions")),
        "has_tools": bool(body.get("tools")),
        "tool_count": len(body.get("tools") or []),
        "tool_names": available_tool_names(body),
        "tool_choice": body.get("tool_choice"),
        "previous_response_id": body.get("previous_response_id"),
        "max_output_tokens": body.get("max_output_tokens"),
    }
    logger.info("responses_request_summary %s", json.dumps(redact(summary), ensure_ascii=False))
    with open("openai_compat_last_responses_request.json", "w", encoding="utf-8") as file:
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
            elif item.get("type") in {"input_image", "image_url"}:
                parts.append(extract_image_prompt(item))
        return "\n".join(parts)

    return ""


def extract_responses_content_text(content: Any) -> str:
    if isinstance(content, str):
        return content

    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
                continue
            if not isinstance(item, dict):
                continue
            item_type = item.get("type")
            if item_type in {"input_text", "output_text", "text"} and isinstance(item.get("text"), str):
                parts.append(item["text"])
            elif item_type in {"input_image", "image_url"}:
                parts.append(extract_image_prompt(item))
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


def responses_input_to_messages(body: dict[str, Any]) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    instructions = body.get("instructions")
    if isinstance(instructions, str) and instructions.strip():
        messages.append({"role": "system", "content": instructions})

    input_value = body.get("input")
    if isinstance(input_value, str):
        messages.append({"role": "user", "content": input_value})
        return messages

    if not isinstance(input_value, list):
        return messages

    for item in input_value:
        if isinstance(item, str):
            messages.append({"role": "user", "content": item})
            continue
        if not isinstance(item, dict):
            continue

        item_type = item.get("type")
        if item_type == "message" or item.get("role"):
            content = extract_responses_content_text(item.get("content"))
            if content:
                messages.append({"role": item.get("role", "user"), "content": content})
        elif item_type == "function_call_output":
            call_id = item.get("call_id") or item.get("id")
            output = item.get("output")
            messages.append({
                "role": "tool",
                "tool_call_id": call_id,
                "content": output if isinstance(output, str) else json.dumps(output, ensure_ascii=False),
            })
        elif item_type == "function_call":
            messages.append({
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": item.get("call_id") or item.get("id") or f"call_{uuid.uuid4().hex}",
                        "type": "function",
                        "function": {
                            "name": item.get("name"),
                            "arguments": item.get("arguments") or "{}",
                        },
                    }
                ],
            })

    return messages


def latest_responses_user_text(body: dict[str, Any]) -> str:
    input_value = body.get("input")
    if isinstance(input_value, str):
        return input_value
    if not isinstance(input_value, list):
        return ""

    for item in reversed(input_value):
        if isinstance(item, str):
            return item
        if not isinstance(item, dict):
            continue
        if item.get("type") == "message" and item.get("role") == "user":
            return extract_responses_content_text(item.get("content"))
    return ""


def latest_responses_input_type(body: dict[str, Any]) -> str:
    input_value = body.get("input")
    if isinstance(input_value, str):
        return "message"
    if not isinstance(input_value, list) or not input_value:
        return ""
    item = input_value[-1]
    if isinstance(item, str):
        return "message"
    if isinstance(item, dict):
        return item.get("type") or ""
    return ""


def latest_chat_role(body: dict[str, Any]) -> str:
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        return ""
    latest = messages[-1]
    if isinstance(latest, dict):
        return latest.get("role") or ""
    return ""


def latest_chat_user_text(body: dict[str, Any]) -> str:
    messages = body.get("messages")
    if not isinstance(messages, list):
        return ""

    for message in reversed(messages):
        if not isinstance(message, dict) or message.get("role") != "user":
            continue
        return extract_content_text(message.get("content"))
    return ""


def all_request_text(body: dict[str, Any]) -> str:
    parts = []
    messages = body.get("messages")
    if isinstance(messages, list):
        for message in messages:
            if isinstance(message, dict):
                parts.append(extract_content_text(message.get("content")))

    input_value = body.get("input")
    if isinstance(input_value, str):
        parts.append(input_value)
    elif isinstance(input_value, list):
        for item in input_value:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                parts.append(extract_responses_content_text(item.get("content")))
    return "\n".join(part for part in parts if part)


def request_has_image_input(body: dict[str, Any]) -> bool:
    def content_has_image(content: Any) -> bool:
        if not isinstance(content, list):
            return False
        return any(
            isinstance(item, dict) and item.get("type") in {"input_image", "image_url"}
            for item in content
        )

    messages = body.get("messages")
    if isinstance(messages, list):
        for message in messages:
            if isinstance(message, dict) and content_has_image(message.get("content")):
                return True

    input_value = body.get("input")
    if isinstance(input_value, list):
        for item in input_value:
            if not isinstance(item, dict):
                continue
            if item.get("type") in {"input_image", "image_url"}:
                return True
            if content_has_image(item.get("content")):
                return True

    return False


def request_has_web_search_tool(body: dict[str, Any]) -> bool:
    tools = body.get("tools")
    if not isinstance(tools, list):
        return False

    for tool in tools:
        if not isinstance(tool, dict):
            continue
        if tool.get("type") in WEB_SEARCH_TOOL_TYPES:
            return True
        function = tool.get("function") if isinstance(tool.get("function"), dict) else tool
        name = function.get("name")
        if isinstance(name, str) and name in WEB_SEARCH_TOOL_TYPES:
            return True
    return False


def request_needs_web_search(body: dict[str, Any]) -> bool:
    if not request_has_web_search_tool(body):
        return False

    text = all_request_text(body).lower()
    if not text.strip():
        return False

    explicit_web = re.search(
        r"("
        r"\bweb\b|\binternet\b|\bsearch\b|\bgoogle\b|\bbrowse\b|\blook\s+up\b|"
        r"\bsource\b|\bsources\b|\bcite\b|\bcitation\b|\blink\b|\blinks\b|"
        r"联网|上网|搜索|搜一下|查一下|查找|网页|网上|来源|引用|链接"
        r")",
        text,
    )
    temporal_or_unstable = re.search(
        r"("
        r"\blatest\b|\bcurrent\b|\brecent\b|\btoday\b|\byesterday\b|\btomorrow\b|"
        r"\bnow\b|\bnews\b|\bprice\b|\bprices\b|\bschedule\b|\bversion\b|\brelease\b|"
        r"\bweather\b|\bscore\b|\bstandings\b|\bwho is\b|\bceo\b|\bpresident\b|"
        r"最新|最近|当前|现在|今天|昨日|昨天|明天|新闻|价格|股价|汇率|天气|赛程|比分|版本|发布"
        r")",
        text,
    )
    return bool(explicit_web or temporal_or_unstable)


def request_cwd(body: dict[str, Any]) -> str:
    text = all_request_text(body)
    match = re.search(r"<cwd>(.*?)</cwd>", text, flags=re.DOTALL)
    if match:
        return match.group(1).strip()
    return ""


def tool_supports_argument(body: dict[str, Any], tool_name: str, argument_name: str) -> bool:
    tools = body.get("tools")
    if not isinstance(tools, list):
        return False

    for tool in tools:
        if not isinstance(tool, dict):
            continue
        function = tool.get("function") if isinstance(tool.get("function"), dict) else tool
        if function.get("name") != tool_name:
            continue
        parameters = function.get("parameters")
        if not isinstance(parameters, dict):
            return False
        properties = parameters.get("properties")
        return isinstance(properties, dict) and argument_name in properties
    return False


def enrich_tool_calls(body: dict[str, Any], tool_calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cwd = request_cwd(body)
    if not cwd:
        return tool_calls

    enriched = []
    for tool_call in tool_calls:
        function = tool_call.get("function", {})
        name = function.get("name")
        if name not in {"shell", "shell_command", "execute_command"}:
            enriched.append(tool_call)
            continue
        if not tool_supports_argument(body, name, "workdir"):
            enriched.append(tool_call)
            continue

        try:
            arguments = json.loads(function.get("arguments") or "{}")
        except json.JSONDecodeError:
            enriched.append(tool_call)
            continue

        if isinstance(arguments, dict) and not arguments.get("workdir"):
            arguments["workdir"] = cwd
            tool_call = {
                **tool_call,
                "function": {
                    **function,
                    "arguments": json.dumps(arguments, ensure_ascii=False),
                },
            }
        enriched.append(tool_call)
    return enriched


def request_requires_tool_execution(body: dict[str, Any], latest_user_text: str = "") -> bool:
    if not body.get("tools"):
        return False

    latest_input_type = latest_responses_input_type(body)
    if latest_input_type and latest_input_type != "message":
        return False

    latest_role = latest_chat_role(body)
    if latest_role and latest_role != "user":
        return False

    tool_choice = body.get("tool_choice")
    if tool_choice == "required":
        return True
    if isinstance(tool_choice, dict):
        return True

    tool_names = set(available_tool_names(body))
    if request_has_image_input(body) and "view_image" in tool_names:
        return True
    if request_needs_web_search(body):
        return False

    text = (latest_user_text or latest_chat_user_text(body) or latest_responses_user_text(body)).lower()
    if not text.strip():
        return False

    can_touch_local = bool(
        tool_names
        & {
            "shell",
            "shell_command",
            "execute_command",
            "apply_patch",
            "read_file",
            "list_files",
            "search_files",
            "view_image",
        }
    )
    if not can_touch_local:
        return False

    local_object = re.search(
        r"("
        r"\bfile\b|\bfiles\b|\bfolder\b|\bdirectory\b|\brepo\b|\bworkspace\b|"
        r"\bterminal\b|\bshell\b|\bcommand\b|\bhtml\b|\bcss\b|\bjs\b|\bpython\b|"
        r"[a-z0-9_.-]+\.(?:html|css|js|ts|tsx|jsx|py|json|md|txt|csv|xml|yaml|yml)|"
        r"文件|目录|文件夹|终端|命令|脚本|仓库|项目|代码|本地|html|网页"
        r")",
        text,
    )
    local_action = re.search(
        r"("
        r"\bcreate\b|\bmake\b|\bwrite\b|\bsave\b|\bedit\b|\bmodify\b|\bpatch\b|"
        r"\bdelete\b|\bremove\b|\brename\b|\bmove\b|\bread\b|\bopen\b|\binspect\b|"
        r"\blist\b|\bsearch\b|\bfind\b|\brun\b|\bexecute\b|"
        r"新建|创建|生成|写入|保存|修改|编辑|删除|移除|重命名|移动|读取|查看|"
        r"打开|列出|搜索|查找|运行|执行"
        r")",
        text,
    )
    return bool(local_object and local_action)


def tool_requirement_prompt(body: dict[str, Any], latest_user_text: str = "") -> str:
    if not request_requires_tool_execution(body, latest_user_text):
        return ""

    tool_names = set(available_tool_names(body))
    examples = []
    if "shell" in tool_names:
        examples.append(
            '{"tool_calls":[{"name":"shell","arguments":{"command":["powershell.exe","-Command","Set-Content -LiteralPath example.txt -Value \'content\' -Encoding UTF8"],"workdir":"<current workspace cwd>"}}]}'
        )
    if "shell_command" in tool_names:
        examples.append(
            '{"tool_calls":[{"name":"shell_command","arguments":{"command":"Set-Content -LiteralPath example.txt -Value \'content\' -Encoding UTF8"}}]}'
        )
    if "execute_command" in tool_names:
        examples.append(
            '{"tool_calls":[{"name":"execute_command","arguments":{"command":"Set-Content -LiteralPath example.txt -Value \'content\' -Encoding UTF8"}}]}'
        )
    if "apply_patch" in tool_names:
        examples.append(
            '{"tool_calls":[{"name":"apply_patch","arguments":{"input":"*** Begin Patch\\n*** Add File: example.txt\\n+content\\n*** End Patch"}}]}'
        )
    if "view_image" in tool_names:
        examples.append(
            '{"tool_calls":[{"name":"view_image","arguments":{"path":"<absolute image path from the latest user message>"}}]}'
        )

    lines = [
        "MANDATORY TOOL-CALL MODE FOR THE LATEST USER REQUEST:",
        "The latest user request requires local client-side tool execution.",
        "Your entire response must be exactly one JSON object with a tool_calls array and must start with `{`.",
        "Do not stream or prepend any natural language before the JSON object.",
        "Do not answer with raw source code or file contents; place file contents inside the tool arguments.",
        "For multi-line files, use a Powershell here-string inside the tool command instead of printing the file content to the user.",
        "If the command tool has a workdir argument, set it to the current workspace cwd shown in environment_context.",
        "Do not use browser/chat sandbox artifacts, /mnt/data paths, or sandbox:/ links.",
        "Do not claim success until a client tool has actually been called.",
    ]
    if request_has_image_input(body) and "view_image" in tool_names:
        lines.append("The latest user request includes an image saved to a local path; call view_image with that exact path before describing it.")
    if examples:
        lines.append("Valid examples for this client:")
        lines.extend(examples)
    return "\n".join(lines)


def web_search_prompt(body: dict[str, Any]) -> str:
    if not request_needs_web_search(body):
        return ""

    return "\n".join(
        [
            "WEB SEARCH REQUIRED FOR THE LATEST USER REQUEST:",
            "The client requested web_search capability and the latest request asks for current, online, or source-backed information.",
            "Use the upstream ChatGPT browsing/search capability before answering.",
            "Base the answer on search results, include source URLs, and avoid relying only on memory.",
            "Do not emit a web_search JSON tool call; hosted web_search is not a local function tool in this bridge.",
        ]
    )


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
        "Tool execution is strict: only that JSON tool_calls object is executable.",
        "Do not wrap tool_calls JSON in markdown fences.",
        "If the task is complete, call attempt_completion with a result.",
        "If files must be created or edited, prefer apply_patch with the exact patch content.",
        "If apply_patch is not available but shell is available, use exactly one shell call with a Powershell command array.",
        "If the available command tool is shell_command or execute_command, put the Powershell command string in the command field.",
        "For multi-line file creation through a Powershell command, put the file contents in a single-quoted here-string and write it with Set-Content -Encoding UTF8.",
        "When a command tool supports workdir, set workdir to the current workspace cwd from environment_context.",
        "Never claim that you created a file in /mnt/data or provide a sandbox download link; this bridge can only execute explicit client tool calls.",
        "This bridge executes only tool calls represented in the JSON tool_calls array; prose, links, and claimed artifacts are not executable.",
        "After receiving a tool result, summarize or continue based on the result; do not repeat the same tool call.",
        "If you need to inspect files, call read_file/list_files/search_files before guessing.",
    ]
    if request_has_web_search_tool(body):
        lines.extend(
            [
                "Web search compatibility:",
                "The client provided a web_search tool, but this bridge cannot execute hosted OpenAI web_search calls locally.",
                "For requests that need current or online information, use the upstream ChatGPT web browsing/search capability before answering.",
                "Cite sources with URLs when web search is used.",
                "Do not emit web_search as JSON tool_calls; answer with the searched result after browsing upstream.",
            ]
        )
    lines.append("Available tools:")
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        if tool.get("type") in WEB_SEARCH_TOOL_TYPES:
            external = tool.get("external_web_access")
            lines.append(f"- {tool.get('type')}: hosted web search requested by client; external_web_access={external}")
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
    latest_user_text = latest_chat_user_text(body)

    extras = []
    search_prompt = web_search_prompt(body)
    if search_prompt:
        extras.append(search_prompt)
    if tool_prompt:
        extras.append(tool_prompt)
    requirement = tool_requirement_prompt(body, latest_user_text)
    if requirement:
        extras.append(requirement)
    if response_format:
        extras.append(f"Response format: {json.dumps(response_format, ensure_ascii=False)}")

    if extras:
        return prompt + "\n\n" + "\n\n".join(extras)
    return prompt


def build_responses_prompt(body: dict[str, Any]) -> str:
    messages = responses_input_to_messages(body)
    if not messages:
        raise HTTPException(status_code=400, detail="input must contain text content")

    bridge_messages = [
        {
            "role": "system",
            "content": (
                "Bridge compatibility instruction: you are running inside the Codex coding agent "
                "interface. Honor the user's Codex/Codex CLI instructions above the upstream model "
                "identity. If asked who you are, answer that you are Codex, a coding agent. When "
                "asked to create or edit local files, emit explicit JSON tool_calls for the "
                "available client tools. The bridge executes only JSON tool_calls; prose, links, "
                "and /mnt/data artifacts are ignored. Do not create files in /mnt/data, do not "
                "provide download links, and do not claim a file exists unless it was written "
                "through a client tool."
            ),
        },
        *messages,
    ]
    prompt = messages_to_prompt(bridge_messages)
    extras = []
    search_prompt = web_search_prompt(body)
    if search_prompt:
        extras.append(search_prompt)
    tool_prompt = tools_to_prompt(body)
    if tool_prompt:
        extras.append(tool_prompt)
    requirement = tool_requirement_prompt(body, latest_responses_user_text(body))
    if requirement:
        extras.append(requirement)

    text_options = body.get("text")
    if text_options:
        extras.append(f"Text options: {json.dumps(text_options, ensure_ascii=False)}")

    if extras:
        return prompt + "\n\n" + "\n\n".join(extras)
    return prompt


def build_tool_repair_prompt(body: dict[str, Any], invalid_answer: str) -> str:
    latest_user_text = latest_chat_user_text(body) or latest_responses_user_text(body)
    cwd = request_cwd(body)
    invalid_preview = invalid_answer.strip()
    if len(invalid_preview) > 6000:
        invalid_preview = invalid_preview[:6000].rstrip() + "\n...[truncated]"

    lines = [
        "You are repairing an invalid tool response for an OpenAI-compatible bridge.",
        "The previous answer claimed a local action was completed, but it did not provide executable tool_calls JSON.",
        "Convert the intent into exactly one JSON object and no prose. The response must start with `{`.",
        '{"tool_calls":[{"name":"tool_name","arguments":{}}]}',
        "Use only the available tools below. Do not use /mnt/data, sandbox:/ links, markdown, or natural language.",
        "If a file must be created, put the complete file contents inside a client tool argument.",
        "If using Powershell for a multi-line file, use a single-quoted here-string and Set-Content -Encoding UTF8.",
    ]
    if cwd:
        lines.append(f"Current workspace cwd: {cwd}")
    lines.extend(
        [
            "",
            "Latest user request:",
            latest_user_text,
            "",
            "Invalid previous answer:",
            invalid_preview,
            "",
            repair_tools_to_prompt(body),
            "",
            tool_requirement_prompt(body, latest_user_text),
        ]
    )
    return "\n".join(line for line in lines if line is not None)


def repair_tools_to_prompt(body: dict[str, Any]) -> str:
    preferred_tools = {"apply_patch", "shell", "shell_command", "execute_command"}
    lines = [
        "Available executable tools for repair:",
    ]
    for function in iter_tool_functions(body):
        name = function.get("name")
        if name not in preferred_tools:
            continue
        lines.append(f"- {name}")
        parameters = function.get("parameters")
        if isinstance(parameters, dict):
            properties = parameters.get("properties")
            required = parameters.get("required")
            compact_parameters = {
                "type": parameters.get("type", "object"),
                "properties": properties if isinstance(properties, dict) else {},
            }
            if isinstance(required, list):
                compact_parameters["required"] = required
            lines.append(f"  parameters: {json.dumps(compact_parameters, ensure_ascii=False)}")
    return "\n".join(lines)


def repair_tool_calls(
    body: dict[str, Any],
    invalid_answer: str,
    profile: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], str]:
    if not body.get("tools"):
        return [], ""

    repair_prompt = build_tool_repair_prompt(body, invalid_answer)
    try:
        result = bridge.ask_ai(repair_prompt, profile or bridge.select_profile(), body.get("model"))
    except Exception as exc:
        logger.info("tool_repair_error %s", json.dumps({"error": str(exc)}, ensure_ascii=False))
        return [], ""

    repaired_answer = result.get("answer") or ""
    tool_calls = enrich_tool_calls(body, parse_available_tool_calls(body, repaired_answer))
    logger.info(
        "tool_repair_done %s",
        json.dumps(
            {
                "answer_chars": len(repaired_answer),
                "tool_calls": len(tool_calls),
                "profile": result.get("profile"),
                "conversation_id": result.get("conversation_id"),
            },
            ensure_ascii=False,
        ),
    )
    return tool_calls, repaired_answer


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
    names = [function["name"] for function in iter_tool_functions(body)]
    tools = body.get("tools")
    if isinstance(tools, list):
        for tool in tools:
            if isinstance(tool, dict) and tool.get("type") in WEB_SEARCH_TOOL_TYPES:
                names.append(tool["type"])
    return names


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


def make_fallback_tool_call(
    body: dict[str, Any],
    answer: str,
    latest_user_text: str = "",
) -> dict[str, Any] | None:
    return make_attempt_completion_call(body, answer)


def bridge_tool_failure_message(answer: str) -> str:
    preview = answer.strip()
    if len(preview) > 500:
        preview = preview[:500].rstrip() + "..."
    detail = f"\n\nUpstream text was:\n{preview}" if preview else ""
    return (
        "Bridge error: this request required a real client tool call, but the upstream "
        "model returned prose or a sandbox artifact instead of executable tool_calls JSON. "
        "No local file or command was executed. Please retry; the upstream response must "
        "start with a JSON object like {\"tool_calls\":[...]}."
        f"{detail}"
    )


def make_tool_failure_call(body: dict[str, Any], answer: str) -> dict[str, Any] | None:
    return make_attempt_completion_call(body, bridge_tool_failure_message(answer))


def make_completion_response(request_body: dict[str, Any], answer: str) -> dict[str, Any]:
    model = request_body.get("model") or bridge.MODEL
    completion_id = f"chatcmpl-{uuid.uuid4().hex}"
    now = int(time.time())
    message: dict[str, Any] = {
        "role": "assistant",
        "content": answer,
    }
    finish_reason = "stop"

    tool_calls = enrich_tool_calls(request_body, parse_available_tool_calls(request_body, answer))
    fallback_tool_call = False
    must_use_tools = request_requires_tool_execution(request_body)
    if must_use_tools and not tool_calls:
        repaired_calls, repaired_answer = repair_tool_calls(request_body, answer)
        if repaired_calls:
            tool_calls = repaired_calls
        else:
            failure_call = make_tool_failure_call(request_body, repaired_answer or answer)
            if failure_call:
                tool_calls = [failure_call]
                fallback_tool_call = True
            else:
                message["content"] = bridge_tool_failure_message(repaired_answer or answer)
    elif not tool_calls:
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

    if name == "view_image" and isinstance(arguments, dict) and "path" not in arguments:
        for alias in ("file_path", "filepath", "image_path", "local_path"):
            if isinstance(arguments.get(alias), str):
                arguments["path"] = arguments[alias]
                break

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


def looks_like_tool_json(text: str) -> bool | None:
    stripped = text.lstrip()
    if not stripped:
        return None
    return stripped[0] in "{["


def chat_content_chunk(completion_id: str, model: str, delta_text: str) -> dict[str, Any]:
    return {
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
    must_use_tools = request_requires_tool_execution(request_body)

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
    tool_json_mode: bool | None = None
    streamed_content = False
    stream_error = None
    try:
        for item in bridge.stream_ai(prompt, profile, request_body.get("model")):
            if item.get("type") != "delta":
                continue

            delta_text = item.get("delta", "")
            buffered_answer += delta_text
            if has_tools:
                if tool_json_mode is None:
                    tool_json_mode = looks_like_tool_json(buffered_answer)
                    if tool_json_mode is None:
                        continue
                    if tool_json_mode is False:
                        if must_use_tools:
                            continue
                        streamed_content = True
                        yield f"data: {json.dumps(chat_content_chunk(completion_id, model, buffered_answer), ensure_ascii=False)}\n\n"
                    continue
                if tool_json_mode:
                    continue
                if must_use_tools:
                    continue

                streamed_content = True
                yield f"data: {json.dumps(chat_content_chunk(completion_id, model, delta_text), ensure_ascii=False)}\n\n"
                continue

            content_chunk = chat_content_chunk(completion_id, model, delta_text)
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

    tool_calls = (
        enrich_tool_calls(request_body, parse_available_tool_calls(request_body, buffered_answer))
        if has_tools and not streamed_content
        else []
    )
    save_last_upstream_answer(buffered_answer, "stream")
    fallback_tool_call = False
    if has_tools and not streamed_content and not tool_calls:
        if must_use_tools:
            repaired_calls, repaired_answer = repair_tool_calls(request_body, buffered_answer, profile)
            if repaired_calls:
                tool_calls = repaired_calls
                fallback_call = None
            else:
                fallback_call = make_tool_failure_call(request_body, repaired_answer or buffered_answer)
        else:
            fallback_call = make_fallback_tool_call(request_body, buffered_answer)
        if fallback_call:
            tool_calls = [fallback_call]
            fallback_tool_call = True

    if tool_calls:
        for chunk in stream_tool_call_chunks(completion_id, model, tool_calls):
            yield chunk

    elif has_tools and buffered_answer and not streamed_content:
        content = bridge_tool_failure_message(buffered_answer) if must_use_tools else buffered_answer
        content_chunk = {
            "id": completion_id,
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "delta": {"content": content},
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
        "streamed_content": streamed_content,
        "tool_json_mode": tool_json_mode,
        "must_use_tools": must_use_tools,
        "finish_reason": "tool_calls" if tool_calls else "stop",
        "error": stream_error,
    }
    logger.info("stream_done %s", json.dumps(stream_summary, ensure_ascii=False))
    with open("openai_compat_last_stream_response.json", "w", encoding="utf-8") as file:
        json.dump(stream_summary, file, ensure_ascii=False, indent=2)
    yield f"data: {json.dumps(done_chunk, ensure_ascii=False)}\n\n"
    yield "data: [DONE]\n\n"


RESPONSES_STORE: dict[str, dict[str, Any]] = {}


def make_response_base(request_body: dict[str, Any], response_id: str | None = None) -> dict[str, Any]:
    return {
        "id": response_id or f"resp_{uuid.uuid4().hex}",
        "object": "response",
        "created_at": int(time.time()),
        "status": "completed",
        "error": None,
        "incomplete_details": None,
        "instructions": request_body.get("instructions"),
        "max_output_tokens": request_body.get("max_output_tokens"),
        "model": request_body.get("model") or bridge.MODEL,
        "output": [],
        "parallel_tool_calls": request_body.get("parallel_tool_calls", False),
        "previous_response_id": request_body.get("previous_response_id"),
        "reasoning": request_body.get("reasoning"),
        "store": request_body.get("store", True),
        "temperature": request_body.get("temperature"),
        "text": request_body.get("text") or {"format": {"type": "text"}},
        "tool_choice": request_body.get("tool_choice", "auto"),
        "tools": request_body.get("tools") or [],
        "top_p": request_body.get("top_p"),
        "truncation": request_body.get("truncation", "disabled"),
        "usage": {
            "input_tokens": 0,
            "input_tokens_details": {"cached_tokens": 0},
            "output_tokens": 0,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": 0,
        },
        "user": request_body.get("user"),
        "metadata": request_body.get("metadata") or {},
    }


def make_response_message_item(answer: str) -> dict[str, Any]:
    return {
        "id": f"msg_{uuid.uuid4().hex}",
        "type": "message",
        "status": "completed",
        "role": "assistant",
        "content": [
            {
                "type": "output_text",
                "text": answer,
                "annotations": [],
            }
        ],
    }


def response_message_text(item: dict[str, Any]) -> str:
    content = item.get("content")
    if not isinstance(content, list):
        return ""

    parts = []
    for part in content:
        if isinstance(part, dict) and isinstance(part.get("text"), str):
            parts.append(part["text"])
    return "".join(parts)


def chat_tool_call_to_response_item(tool_call: dict[str, Any]) -> dict[str, Any]:
    function = tool_call.get("function", {})
    arguments = function.get("arguments")
    if not isinstance(arguments, str):
        arguments = json.dumps(arguments or {}, ensure_ascii=False)
    return {
        "id": f"fc_{uuid.uuid4().hex}",
        "type": "function_call",
        "status": "completed",
        "call_id": tool_call.get("id") or f"call_{uuid.uuid4().hex}",
        "name": function.get("name"),
        "arguments": arguments,
    }


def response_items_from_answer(request_body: dict[str, Any], answer: str) -> list[dict[str, Any]]:
    tool_calls = (
        enrich_tool_calls(request_body, parse_available_tool_calls(request_body, answer))
        if request_body.get("tools")
        else []
    )
    if tool_calls:
        return [chat_tool_call_to_response_item(tool_call) for tool_call in tool_calls]
    latest_input_type = latest_responses_input_type(request_body)
    latest_user_text = latest_responses_user_text(request_body)
    if request_requires_tool_execution(request_body, latest_user_text):
        repaired_calls, repaired_answer = repair_tool_calls(request_body, answer)
        if repaired_calls:
            return [chat_tool_call_to_response_item(tool_call) for tool_call in repaired_calls]
        return [make_response_message_item(bridge_tool_failure_message(repaired_answer or answer))]
    if request_body.get("tools") and latest_input_type in {"message", ""}:
        fallback_call = make_fallback_tool_call(request_body, answer, latest_user_text)
        if fallback_call:
            return [chat_tool_call_to_response_item(fallback_call)]
    return [make_response_message_item(answer)]


def make_responses_response(request_body: dict[str, Any], answer: str) -> dict[str, Any]:
    response = make_response_base(request_body)
    response["output"] = response_items_from_answer(request_body, answer)
    if len(response["output"]) > 1:
        response["parallel_tool_calls"] = True
    RESPONSES_STORE[response["id"]] = response
    with open("openai_compat_last_responses_response.json", "w", encoding="utf-8") as file:
        json.dump(response, file, ensure_ascii=False, indent=2)
    return response


def responses_sse(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def stream_response_text_events(
    response: dict[str, Any],
    item: dict[str, Any],
    text_iter,
):
    output_index = 0
    content_index = 0
    item_id = item["id"]
    response_in_progress = {**response, "status": "in_progress"}

    yield responses_sse("response.created", {"type": "response.created", "response": response_in_progress})
    yield responses_sse("response.in_progress", {"type": "response.in_progress", "response": response_in_progress})
    yield responses_sse(
        "response.output_item.added",
        {"type": "response.output_item.added", "output_index": output_index, "item": item},
    )
    part = {"type": "output_text", "text": "", "annotations": []}
    yield responses_sse(
        "response.content_part.added",
        {
            "type": "response.content_part.added",
            "item_id": item_id,
            "output_index": output_index,
            "content_index": content_index,
            "part": part,
        },
    )

    answer_parts = []
    for delta_text in text_iter:
        if not delta_text:
            continue
        answer_parts.append(delta_text)
        yield responses_sse(
            "response.output_text.delta",
            {
                "type": "response.output_text.delta",
                "item_id": item_id,
                "output_index": output_index,
                "content_index": content_index,
                "delta": delta_text,
            },
        )

    answer = "".join(answer_parts)
    item["content"][0]["text"] = answer
    response["output"] = [item]
    RESPONSES_STORE[response["id"]] = response

    yield responses_sse(
        "response.output_text.done",
        {
            "type": "response.output_text.done",
            "item_id": item_id,
            "output_index": output_index,
            "content_index": content_index,
            "text": answer,
        },
    )
    yield responses_sse(
        "response.content_part.done",
        {
            "type": "response.content_part.done",
            "item_id": item_id,
            "output_index": output_index,
            "content_index": content_index,
            "part": item["content"][0],
        },
    )
    yield responses_sse(
        "response.output_item.done",
        {"type": "response.output_item.done", "output_index": output_index, "item": item},
    )
    yield responses_sse("response.completed", {"type": "response.completed", "response": response})


def stream_responses_text_from_upstream(
    response: dict[str, Any],
    item: dict[str, Any],
    prompt: str,
    profile: dict[str, Any],
    request_body: dict[str, Any],
):
    buffered_answer = ""

    def deltas():
        nonlocal buffered_answer
        try:
            for chunk in bridge.stream_ai(prompt, profile, request_body.get("model")):
                if chunk.get("type") != "delta":
                    continue
                delta_text = chunk.get("delta", "")
                buffered_answer += delta_text
                yield delta_text
        except Exception as exc:
            buffered_answer = f"[bridge error] {exc}"
            yield buffered_answer

    yield from stream_response_text_events(response, item, deltas())
    save_last_upstream_answer(buffered_answer, "responses_stream")


def stream_response_function_call_events(response: dict[str, Any], items: list[dict[str, Any]]):
    if len(items) > 1:
        response["parallel_tool_calls"] = True
    response_in_progress = {**response, "status": "in_progress"}
    yield responses_sse("response.created", {"type": "response.created", "response": response_in_progress})
    yield responses_sse("response.in_progress", {"type": "response.in_progress", "response": response_in_progress})

    for output_index, item in enumerate(items):
        call_id = item["call_id"]
        arguments = item.get("arguments") or "{}"
        added_item = {**item, "arguments": ""}
        yield responses_sse(
            "response.output_item.added",
            {
                "type": "response.output_item.added",
                "response_id": response["id"],
                "output_index": output_index,
                "item": added_item,
            },
        )
        yield responses_sse(
            "response.function_call_arguments.delta",
            {
                "type": "response.function_call_arguments.delta",
                "response_id": response["id"],
                "item_id": item["id"],
                "output_index": output_index,
                "delta": arguments,
            },
        )
        yield responses_sse(
            "response.function_call_arguments.done",
            {
                "type": "response.function_call_arguments.done",
                "response_id": response["id"],
                "output_index": output_index,
                "item": item,
            },
        )
        yield responses_sse(
            "response.output_item.done",
            {
                "type": "response.output_item.done",
                "response_id": response["id"],
                "output_index": output_index,
                "item": item,
            },
        )
        logger.info("responses_function_call %s", json.dumps({"name": item.get("name"), "call_id": call_id}, ensure_ascii=False))

    response["output"] = items
    RESPONSES_STORE[response["id"]] = response
    yield responses_sse("response.completed", {"type": "response.completed", "response": response})


def stream_responses_response(request_body: dict[str, Any], prompt: str):
    response = make_response_base(request_body)
    has_tools = bool(request_body.get("tools"))
    latest_user_text = latest_responses_user_text(request_body)
    must_use_tools = request_requires_tool_execution(request_body, latest_user_text)
    profile = bridge.select_profile()
    buffered_answer = ""

    if not has_tools:
        item = make_response_message_item("")
        yield from stream_responses_text_from_upstream(response, item, prompt, profile, request_body)
        return

    tool_json_mode: bool | None = None
    streamed_content = False
    text_response_started = False
    text_item = make_response_message_item("")
    output_index = 0
    content_index = 0
    item_id = text_item["id"]

    def start_text_response():
        nonlocal text_response_started
        if text_response_started:
            return
        text_response_started = True
        response_in_progress = {**response, "status": "in_progress"}
        yield responses_sse("response.created", {"type": "response.created", "response": response_in_progress})
        yield responses_sse("response.in_progress", {"type": "response.in_progress", "response": response_in_progress})
        yield responses_sse(
            "response.output_item.added",
            {"type": "response.output_item.added", "output_index": output_index, "item": text_item},
        )
        part = {"type": "output_text", "text": "", "annotations": []}
        yield responses_sse(
            "response.content_part.added",
            {
                "type": "response.content_part.added",
                "item_id": item_id,
                "output_index": output_index,
                "content_index": content_index,
                "part": part,
            },
        )

    def text_delta(delta_text: str) -> str:
        return responses_sse(
            "response.output_text.delta",
            {
                "type": "response.output_text.delta",
                "item_id": item_id,
                "output_index": output_index,
                "content_index": content_index,
                "delta": delta_text,
            },
        )

    def finish_text_response():
        text_item["content"][0]["text"] = buffered_answer
        response["output"] = [text_item]
        RESPONSES_STORE[response["id"]] = response
        yield responses_sse(
            "response.output_text.done",
            {
                "type": "response.output_text.done",
                "item_id": item_id,
                "output_index": output_index,
                "content_index": content_index,
                "text": buffered_answer,
            },
        )
        yield responses_sse(
            "response.content_part.done",
            {
                "type": "response.content_part.done",
                "item_id": item_id,
                "output_index": output_index,
                "content_index": content_index,
                "part": text_item["content"][0],
            },
        )
        yield responses_sse(
            "response.output_item.done",
            {"type": "response.output_item.done", "output_index": output_index, "item": text_item},
        )
        yield responses_sse("response.completed", {"type": "response.completed", "response": response})

    try:
        for chunk in bridge.stream_ai(prompt, profile, request_body.get("model")):
            if chunk.get("type") != "delta":
                continue
            delta_text = chunk.get("delta", "")
            buffered_answer += delta_text
            if tool_json_mode is None:
                tool_json_mode = looks_like_tool_json(buffered_answer)
                if tool_json_mode is None:
                    continue
                if tool_json_mode is False:
                    if must_use_tools:
                        continue
                    streamed_content = True
                    yield from start_text_response()
                    yield text_delta(buffered_answer)
                continue
            if tool_json_mode:
                continue
            if must_use_tools:
                continue

            streamed_content = True
            yield text_delta(delta_text)
    except Exception as exc:
        buffered_answer = f"[bridge error] {exc}"
        streamed_content = True
        yield from start_text_response()
        yield text_delta(buffered_answer)

    save_last_upstream_answer(buffered_answer, "responses_stream")
    if streamed_content:
        yield from finish_text_response()
        logger.info(
            "responses_stream_done %s",
            json.dumps(
                {
                    "answer_chars": len(buffered_answer),
                    "streamed_content": True,
                    "tool_json_mode": tool_json_mode,
                    "must_use_tools": must_use_tools,
                },
                ensure_ascii=False,
            ),
        )
        return

    parsed_tool_calls = enrich_tool_calls(request_body, parse_available_tool_calls(request_body, buffered_answer))
    repaired_answer = ""
    if must_use_tools and not parsed_tool_calls:
        parsed_tool_calls, repaired_answer = repair_tool_calls(request_body, buffered_answer, profile)

    if parsed_tool_calls:
        items = [chat_tool_call_to_response_item(tool_call) for tool_call in parsed_tool_calls]
    elif must_use_tools:
        items = [make_response_message_item(bridge_tool_failure_message(repaired_answer or buffered_answer))]
    else:
        items = response_items_from_answer(request_body, buffered_answer)
    if must_use_tools and not items[0].get("type") == "function_call":
        logger.info(
            "responses_required_tool_call_missing %s",
            json.dumps(
                {
                    "answer_chars": len(buffered_answer),
                    "repair_answer_chars": len(repaired_answer),
                    "tool_json_mode": tool_json_mode,
                },
                ensure_ascii=False,
            ),
        )
    if items and items[0].get("type") == "function_call":
        yield from stream_response_function_call_events(response, items)
    else:
        yield from stream_response_text_events(response, items[0], [response_message_text(items[0])])


def call_bridge(prompt: str, model: str | None = None) -> dict[str, Any]:
    profile = bridge.select_profile()
    return bridge.ask_ai(prompt, profile, model)


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
    models = [bridge.MODEL, *bridge.MODEL_ALIASES.keys()]
    seen = set()
    unique_models = []
    for model in models:
        if model not in seen:
            seen.add(model)
            unique_models.append(model)
    return {
        "object": "list",
        "data": [
            {
                "id": model,
                "object": "model",
                "created": 0,
                "owned_by": "sharedchat-bridge",
            }
            for model in unique_models
        ],
    }


@app.get("/v1/responses/{response_id}")
def get_response(response_id: str) -> dict[str, Any]:
    response = RESPONSES_STORE.get(response_id)
    if not response:
        raise HTTPException(status_code=404, detail="response not found")
    return response


@app.post("/v1/responses/{response_id}/cancel")
def cancel_response(response_id: str) -> dict[str, Any]:
    response = RESPONSES_STORE.get(response_id)
    if not response:
        response = make_response_base({}, response_id)
    response["status"] = "cancelled"
    RESPONSES_STORE[response_id] = response
    return response


@app.post("/v1/responses")
async def responses(request: Request):
    body = await request.json()
    log_responses_request_summary(body)
    prompt = build_responses_prompt(body)

    if body.get("stream") is True:
        logger.info("responses_stream_start")
        return StreamingResponse(
            stream_responses_response(body, prompt),
            media_type="text/event-stream",
        )

    try:
        result = call_bridge(prompt, body.get("model"))
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(status_code=exc.code, detail=error_body)
    except URLError as exc:
        raise HTTPException(status_code=502, detail=str(exc.reason))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    answer = result.get("answer") or ""
    save_last_upstream_answer(answer, "responses_non_stream")
    response = make_responses_response(body, answer)
    logger.info(
        "responses_done %s",
        json.dumps(
            {
                "answer_chars": len(answer),
                "output_count": len(response.get("output") or []),
                "output_types": [item.get("type") for item in response.get("output") or []],
                "profile": result.get("profile"),
                "conversation_id": result.get("conversation_id"),
                "topic_id": result.get("topic_id"),
            },
            ensure_ascii=False,
        ),
    )
    return JSONResponse(response)


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
        result = call_bridge(prompt, body.get("model"))
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
