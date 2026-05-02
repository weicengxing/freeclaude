import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib import request
from urllib.error import HTTPError, URLError

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    tomllib = None


CODEX_INSTRUCTIONS = """You are a coding agent running in the Codex client. Be precise, safe, and helpful.

When the user asks for code, prefer concrete working changes or complete examples. Keep answers concise unless more detail is needed."""


def default_codex_home() -> Path:
    return Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex")


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def load_codex_config(path: Path | None = None) -> dict[str, Any]:
    config_path = path or default_codex_home() / "config.toml"
    if not config_path.exists() or tomllib is None:
        return {}

    with config_path.open("rb") as file:
        return tomllib.load(file)


def resolve_provider_base_url(config: dict[str, Any]) -> str:
    env_base_url = os.environ.get("CODEX_BASE_URL") or os.environ.get("OPENAI_BASE_URL")
    if env_base_url:
        return env_base_url.rstrip("/")

    provider_name = config.get("model_provider")
    providers = config.get("model_providers") or {}
    if isinstance(provider_name, str):
        provider = providers.get(provider_name) or {}
        base_url = provider.get("base_url")
        if isinstance(base_url, str) and base_url.strip():
            return base_url.rstrip("/")

    return "https://api.openai.com/v1"


def resolve_auth_path(value: str | None) -> Path:
    if value:
        return Path(value).expanduser()

    env_path = os.environ.get("CODEX_AUTH_JSON")
    if env_path:
        return Path(env_path).expanduser()

    codex_auth = default_codex_home() / "auth.json"
    if codex_auth.exists():
        return codex_auth

    return Path("auth.json")


def clean_bearer_token(token: str) -> str:
    token = token.strip()
    if token.lower().startswith("bearer "):
        return token[7:].strip()
    return token


def auth_headers_from_data(auth: dict[str, Any], source: str = "auth data") -> dict[str, str]:
    headers: dict[str, str] = {}

    tokens = auth.get("tokens") or {}
    access_token = tokens.get("access_token") or auth.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not isinstance(access_token, str) or not access_token.strip():
        raise RuntimeError(f"No access token or API key found in {source}")

    headers["authorization"] = f"Bearer {clean_bearer_token(access_token)}"

    account_id = tokens.get("account_id") or auth.get("account_id")
    if isinstance(account_id, str) and account_id.strip():
        headers["ChatGPT-Account-ID"] = account_id.strip()

    return headers


def load_auth_headers(auth_path: Path) -> dict[str, str]:
    return auth_headers_from_data(load_json(auth_path), str(auth_path))


def build_input(prompt: str) -> list[dict[str, Any]]:
    return [
        {
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": prompt,
                }
            ],
        }
    ]


def build_shell_tool() -> dict[str, Any]:
    return {
        "type": "function",
        "name": "shell",
        "description": "Runs a Powershell command on Windows. Arguments should be passed as a command array.",
        "parameters": {
            "type": "object",
            "properties": {
                "command": {
                    "type": "array",
                    "items": {"type": "string"},
                }
            },
            "required": ["command"],
            "additionalProperties": False,
        },
        "strict": True,
    }


def build_codex_body(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    model = args.model or os.environ.get("CODEX_MODEL") or config.get("model") or "gpt-5.5"
    reasoning_effort = args.reasoning_effort or os.environ.get("CODEX_REASONING_EFFORT") or config.get("model_reasoning_effort")
    store = args.store
    if store is None:
        store = not bool(config.get("disable_response_storage", True))

    body: dict[str, Any] = {
        "model": model,
        "instructions": args.instructions or CODEX_INSTRUCTIONS,
        "input": build_input(args.prompt),
        "store": bool(store),
        "stream": bool(args.stream),
        "parallel_tool_calls": True,
        "truncation": "auto",
    }

    if reasoning_effort:
        body["reasoning"] = {"effort": reasoning_effort}

    if args.shell_tool:
        body["tools"] = [build_shell_tool()]
        body["tool_choice"] = "auto"

    if args.include_encrypted_reasoning:
        body["include"] = ["reasoning.encrypted_content"]

    return body


def response_url(base_url: str) -> str:
    base_url = base_url.rstrip("/")
    if base_url.endswith("/responses"):
        return base_url
    return f"{base_url}/responses"


def make_request(url: str, headers: dict[str, str], body: dict[str, Any]) -> request.Request:
    encoded_body = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    req_headers = {
        "accept": "text/event-stream" if body.get("stream") else "application/json",
        "content-type": "application/json",
        "originator": "codex_cli_rs",
        "user-agent": "codex_cli_rs/0.126.0 (Windows 10; x86_64)",
        "version": "0.126.0",
        **headers,
    }
    return request.Request(url, data=encoded_body, headers=req_headers, method="POST")


def parse_sse_line(line: str) -> Any | None:
    if not line.startswith("data:"):
        return None

    data = line.removeprefix("data:").strip()
    if not data or data == "[DONE]":
        return None

    try:
        return json.loads(data)
    except json.JSONDecodeError:
        return data


def print_stream_response(req: request.Request, timeout: int) -> None:
    with request.urlopen(req, timeout=timeout) as res:
        print(f"HTTP {res.status} {res.reason}", file=sys.stderr)
        for raw_line in res:
            line = raw_line.decode("utf-8", errors="replace").rstrip()
            event = parse_sse_line(line)
            if not isinstance(event, dict):
                continue

            event_type = event.get("type")
            if event_type in {"response.output_text.delta", "response.refusal.delta"}:
                print(event.get("delta", ""), end="", flush=True)
            elif event_type == "response.function_call_arguments.delta":
                print(event.get("delta", ""), end="", flush=True)
            elif event_type == "response.completed":
                print()


def collect_text_from_response(data: dict[str, Any]) -> str:
    output_text = data.get("output_text")
    if isinstance(output_text, str):
        return output_text

    chunks: list[str] = []
    for item in data.get("output") or []:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "function_call":
            name = item.get("name")
            arguments = item.get("arguments")
            chunks.append(json.dumps({"function_call": name, "arguments": arguments}, ensure_ascii=False))
            continue
        for content in item.get("content") or []:
            if isinstance(content, dict) and content.get("type") in {"output_text", "text"}:
                text = content.get("text")
                if isinstance(text, str):
                    chunks.append(text)

    return "\n".join(chunk for chunk in chunks if chunk)


def print_json_response(req: request.Request, timeout: int) -> None:
    with request.urlopen(req, timeout=timeout) as res:
        body = res.read().decode("utf-8", errors="replace")
        data = json.loads(body)
        text = collect_text_from_response(data)
        print(text if text else json.dumps(data, ensure_ascii=False, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send a Codex-style Responses API request.")
    parser.add_argument("prompt", nargs="?", default="Hello from a Codex-style request.")
    parser.add_argument("--auth", help="Path to Codex auth.json. Defaults to CODEX_AUTH_JSON or ~/.codex/auth.json.")
    parser.add_argument("--config", help="Path to Codex config.toml. Defaults to ~/.codex/config.toml.")
    parser.add_argument("--base-url", help="Base URL ending in /v1, for example https://api.openai.com/v1.")
    parser.add_argument("--model", help="Model name. Defaults to Codex config model.")
    parser.add_argument("--reasoning-effort", choices=["minimal", "low", "medium", "high"], help="Responses reasoning effort.")
    parser.add_argument("--instructions", help="Override the default Codex-style instructions.")
    parser.add_argument("--stream", action=argparse.BooleanOptionalAction, default=True, help="Stream SSE output.")
    parser.add_argument("--store", action=argparse.BooleanOptionalAction, default=None, help="Set Responses store.")
    parser.add_argument("--shell-tool", action="store_true", help="Include a Codex-like shell function tool schema.")
    parser.add_argument("--include-encrypted-reasoning", action="store_true", help="Request reasoning.encrypted_content in include.")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--print-body", action="store_true", help="Print the JSON body without sending it.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_codex_config(Path(args.config).expanduser() if args.config else None)
    base_url = (args.base_url or resolve_provider_base_url(config)).rstrip("/")
    body = build_codex_body(args, config)

    if args.print_body:
        print(json.dumps(body, ensure_ascii=False, indent=2))
        return 0

    auth_headers = load_auth_headers(resolve_auth_path(args.auth))
    req = make_request(response_url(base_url), auth_headers, body)

    try:
        if body.get("stream"):
            print_stream_response(req, args.timeout)
        else:
            print_json_response(req, args.timeout)
        return 0
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        print(f"HTTP error {exc.code}: {details}", file=sys.stderr)
        return 1
    except URLError as exc:
        print(f"Network error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
