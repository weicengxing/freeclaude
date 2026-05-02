from __future__ import annotations

import json
import os
import re
import subprocess
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from .llm_client import DEFAULT_MODEL, SUPPORTED_MODELS, normalize_model, send_chat


BASE_DIR = Path(__file__).resolve().parent
MAX_TOOL_STEPS = 10
MAX_DIR_ENTRIES = 200
MAX_FILE_BYTES = 160_000
MAX_READ_LINES = 220
MAX_SEARCH_RESULTS = 60
MAX_HISTORY_MESSAGES = 18
MAX_COMMAND_TIMEOUT = 30
MAX_COMMAND_OUTPUT_CHARS = 16_000


app = FastAPI(title="Workspace Agent Prototype")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@dataclass
class SessionMessage:
    role: str
    content: str
    tool_steps: list[dict[str, Any]] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)


@dataclass
class SessionPermissions:
    read_enabled: bool = True
    write_enabled: bool = False
    shell_enabled: bool = False


@dataclass
class AgentSession:
    id: str
    workspace_root: str
    model: str
    permissions: SessionPermissions = field(default_factory=SessionPermissions)
    history: list[SessionMessage] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


SESSIONS: dict[str, AgentSession] = {}
SESSIONS_LOCK = threading.Lock()


class CreateSessionRequest(BaseModel):
    workspace_root: str = Field(min_length=1)
    model: str | None = None


class UpdateWorkspaceRequest(BaseModel):
    workspace_root: str = Field(min_length=1)


class UpdatePermissionsRequest(BaseModel):
    write_enabled: bool | None = None
    shell_enabled: bool | None = None


class ChatTurnRequest(BaseModel):
    message: str = Field(min_length=1, max_length=12000)
    model: str | None = None


def serialize_message(message: SessionMessage) -> dict[str, Any]:
    return {
        "role": message.role,
        "content": message.content,
        "tool_steps": message.tool_steps,
        "created_at": message.created_at,
    }


def serialize_permissions(permissions: SessionPermissions) -> dict[str, bool]:
    return {
        "read_enabled": permissions.read_enabled,
        "write_enabled": permissions.write_enabled,
        "shell_enabled": permissions.shell_enabled,
    }


def serialize_session(session: AgentSession) -> dict[str, Any]:
    return {
        "id": session.id,
        "workspace_root": session.workspace_root,
        "model": session.model,
        "permissions": serialize_permissions(session.permissions),
        "history": [serialize_message(item) for item in session.history],
        "created_at": session.created_at,
        "updated_at": session.updated_at,
    }


def sse_event(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def parse_env_flag(name: str, default: bool = False) -> bool:
    value = str(os.getenv(name, "") or "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def resolve_configured_roots() -> list[Path]:
    raw_value = str(os.getenv("AGENT_ALLOWED_WORKSPACE_ROOTS", "") or "").strip()
    if not raw_value:
        return []

    allowed_roots: list[Path] = []
    for raw_item in raw_value.split(os.pathsep):
        item = raw_item.strip().strip('"')
        if not item:
            continue
        candidate = Path(os.path.expandvars(os.path.expanduser(item)))
        try:
            resolved = candidate.resolve(strict=True)
        except FileNotFoundError as exc:
            raise RuntimeError(f"Configured workspace root does not exist: {candidate}") from exc
        if not resolved.is_dir():
            raise RuntimeError(f"Configured workspace root is not a directory: {candidate}")
        allowed_roots.append(resolved)
    return allowed_roots


def is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def get_shell_mode() -> str:
    mode = str(os.getenv("AGENT_SHELL_MODE", "disabled") or "disabled").strip().lower()
    if mode in {"host", "docker"}:
        return mode
    return "disabled"


def shell_execution_is_available() -> bool:
    mode = get_shell_mode()
    if mode == "host":
        return True
    if mode == "docker":
        return bool(str(os.getenv("AGENT_SHELL_DOCKER_IMAGE", "") or "").strip())
    return False


def ensure_shell_execution_available() -> None:
    mode = get_shell_mode()
    if mode == "host":
        return
    if mode == "docker":
        image = str(os.getenv("AGENT_SHELL_DOCKER_IMAGE", "") or "").strip()
        if image:
            return
        raise HTTPException(
            status_code=503,
            detail="Shell execution requires AGENT_SHELL_DOCKER_IMAGE when AGENT_SHELL_MODE=docker.",
        )
    raise HTTPException(
        status_code=403,
        detail=(
            "Shell execution is disabled. Configure AGENT_SHELL_MODE=docker for isolated execution "
            "or AGENT_SHELL_MODE=host for trusted local development only."
        ),
    )


def build_shell_invocation(root: Path, cwd: Path, command: str) -> tuple[list[str], str | None]:
    mode = get_shell_mode()
    if mode == "host":
        if os.name == "nt":
            return ["powershell", "-NoProfile", "-Command", command], str(cwd)
        return ["/bin/bash", "-lc", command], str(cwd)

    if mode == "docker":
        image = str(os.getenv("AGENT_SHELL_DOCKER_IMAGE", "") or "").strip()
        if not image:
            raise HTTPException(
                status_code=503,
                detail="Shell execution requires AGENT_SHELL_DOCKER_IMAGE when AGENT_SHELL_MODE=docker.",
            )
        docker_bin = str(os.getenv("AGENT_SHELL_DOCKER_BIN", "docker") or "docker").strip()
        shell_binary = str(os.getenv("AGENT_SHELL_DOCKER_SHELL", "/bin/sh") or "/bin/sh").strip()
        memory_limit = str(os.getenv("AGENT_SHELL_DOCKER_MEMORY", "512m") or "512m").strip()
        pids_limit = str(os.getenv("AGENT_SHELL_DOCKER_PIDS", "64") or "64").strip()
        workspace_mount = "/workspace"
        relative_workdir = cwd.relative_to(root).as_posix() if cwd != root else ""
        container_workdir = workspace_mount if not relative_workdir else f"{workspace_mount}/{relative_workdir}"
        invocation = [
            docker_bin,
            "run",
            "--rm",
            "--network",
            "none",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--pids-limit",
            pids_limit,
            "--memory",
            memory_limit,
            "--read-only",
            "--tmpfs",
            "/tmp:rw,noexec,nosuid,size=64m",
            "-v",
            f"{root}:{workspace_mount}",
            "-w",
            container_workdir,
            image,
            shell_binary,
            "-lc",
            command,
        ]
        return invocation, None

    raise HTTPException(
        status_code=403,
        detail=(
            "Shell execution is disabled. Configure AGENT_SHELL_MODE=docker for isolated execution "
            "or AGENT_SHELL_MODE=host for trusted local development only."
        ),
    )


def validate_workspace_root(raw_path: str) -> Path:
    text = str(raw_path or "").strip().strip('"')
    if not text:
        raise HTTPException(status_code=400, detail="Workspace root cannot be empty.")
    candidate = Path(os.path.expandvars(os.path.expanduser(text)))
    try:
        resolved = candidate.resolve(strict=True)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"Workspace root does not exist: {candidate}") from exc
    if not resolved.is_dir():
        raise HTTPException(status_code=400, detail="Workspace root must be a directory.")
    try:
        allowed_roots = resolve_configured_roots()
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if allowed_roots and not any(is_relative_to(resolved, allowed_root) for allowed_root in allowed_roots):
        allowed_text = ", ".join(str(item) for item in allowed_roots)
        raise HTTPException(
            status_code=403,
            detail=f"Workspace root is not inside the configured allowlist: {allowed_text}",
        )
    return resolved


def resolve_tool_path(root: Path, relative_path: str | None = None) -> Path:
    rel = str(relative_path or ".").strip() or "."
    if Path(rel).is_absolute():
        raise HTTPException(status_code=400, detail="Tool paths must be relative to the workspace root.")
    target = (root / rel).resolve(strict=False)
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Resolved path escaped the workspace root.") from exc
    return target


def truncate_text(text: str, limit: int) -> tuple[str, bool]:
    if len(text) <= limit:
        return text, False
    return text[:limit].rstrip() + "\n\n[truncated]", True


def sniff_text(path: Path, *, max_bytes: int = MAX_FILE_BYTES) -> str:
    data = path.read_bytes()[:max_bytes]
    if b"\x00" in data:
        raise HTTPException(status_code=400, detail=f"Binary file preview is not supported: {path.name}")
    return data.decode("utf-8", errors="replace")


def require_permission(session: AgentSession, permission_name: str, tool_name: str) -> None:
    if not getattr(session.permissions, permission_name):
        raise HTTPException(
            status_code=403,
            detail=f"{tool_name} is not allowed until the user grants {permission_name.replace('_', ' ')}.",
        )


def list_dir_tool(session: AgentSession, root: Path, args: dict[str, Any]) -> dict[str, Any]:
    require_permission(session, "read_enabled", "list_dir")
    relative_path = str(args.get("relative_path", ".") or ".")
    target = resolve_tool_path(root, relative_path)
    if not target.exists():
        raise HTTPException(status_code=404, detail=f"Path not found: {relative_path}")
    if not target.is_dir():
        raise HTTPException(status_code=400, detail=f"Not a directory: {relative_path}")

    entries = []
    children = sorted(target.iterdir(), key=lambda item: (not item.is_dir(), item.name.lower()))
    for child in children[:MAX_DIR_ENTRIES]:
        stat = child.stat()
        child_rel = child.relative_to(root).as_posix() if child != root else "."
        entries.append(
            {
                "name": child.name,
                "relative_path": child_rel,
                "type": "dir" if child.is_dir() else "file",
                "size": stat.st_size if child.is_file() else None,
                "modified_at": stat.st_mtime,
            }
        )

    return {
        "tool": "list_dir",
        "relative_path": target.relative_to(root).as_posix() if target != root else ".",
        "entries": entries,
        "truncated": len(children) > MAX_DIR_ENTRIES,
    }


def read_file_tool(session: AgentSession, root: Path, args: dict[str, Any]) -> dict[str, Any]:
    require_permission(session, "read_enabled", "read_file")
    relative_path = str(args.get("relative_path", "") or "").strip()
    if not relative_path:
        raise HTTPException(status_code=400, detail="read_file requires relative_path.")
    start_line = max(1, int(args.get("start_line", 1) or 1))
    max_lines = min(MAX_READ_LINES, max(1, int(args.get("max_lines", 160) or 160)))
    target = resolve_tool_path(root, relative_path)
    if not target.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {relative_path}")
    if not target.is_file():
        raise HTTPException(status_code=400, detail=f"Not a file: {relative_path}")

    text = sniff_text(target)
    lines = text.splitlines()
    start_index = start_line - 1
    excerpt_lines = lines[start_index:start_index + max_lines]
    numbered = [
        f"{index}: {line}"
        for index, line in enumerate(excerpt_lines, start=start_line)
    ]

    return {
        "tool": "read_file",
        "relative_path": target.relative_to(root).as_posix(),
        "start_line": start_line,
        "end_line": start_line + len(excerpt_lines) - 1 if excerpt_lines else start_line,
        "line_count": len(lines),
        "content": "\n".join(numbered),
        "truncated": start_index + max_lines < len(lines),
    }


def read_file_content(session: AgentSession, root: Path, relative_path: str) -> dict[str, Any]:
    require_permission(session, "read_enabled", "read_file")
    cleaned_path = str(relative_path or "").strip()
    if not cleaned_path:
        raise HTTPException(status_code=400, detail="relative_path is required.")

    target = resolve_tool_path(root, cleaned_path)
    if not target.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {cleaned_path}")
    if not target.is_file():
        raise HTTPException(status_code=400, detail=f"Not a file: {cleaned_path}")

    text = sniff_text(target)
    line_count = text.count("\n") + (0 if not text else 1)
    return {
        "relative_path": target.relative_to(root).as_posix(),
        "content": text,
        "line_count": line_count,
        "bytes": target.stat().st_size,
    }


def search_text_tool(session: AgentSession, root: Path, args: dict[str, Any]) -> dict[str, Any]:
    require_permission(session, "read_enabled", "search_text")
    query = str(args.get("query", "") or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="search_text requires query.")
    relative_path = str(args.get("relative_path", ".") or ".")
    target = resolve_tool_path(root, relative_path)
    if not target.exists():
        raise HTTPException(status_code=404, detail=f"Path not found: {relative_path}")

    base = target if target.is_dir() else target.parent
    candidates = [target] if target.is_file() else list(base.rglob("*"))
    matches: list[dict[str, Any]] = []
    lowered = query.lower()

    for candidate in candidates:
        if len(matches) >= MAX_SEARCH_RESULTS:
            break
        if not candidate.is_file():
            continue
        try:
            text = sniff_text(candidate, max_bytes=80_000)
        except HTTPException:
            continue
        for index, line in enumerate(text.splitlines(), start=1):
            if lowered in line.lower():
                matches.append(
                    {
                        "relative_path": candidate.relative_to(root).as_posix(),
                        "line": index,
                        "snippet": line.strip(),
                    }
                )
                if len(matches) >= MAX_SEARCH_RESULTS:
                    break

    return {
        "tool": "search_text",
        "query": query,
        "relative_path": base.relative_to(root).as_posix() if base != root else ".",
        "matches": matches,
        "truncated": len(matches) >= MAX_SEARCH_RESULTS,
    }


def write_file_tool(session: AgentSession, root: Path, args: dict[str, Any]) -> dict[str, Any]:
    require_permission(session, "write_enabled", "write_file")
    relative_path = str(args.get("relative_path", "") or "").strip()
    if not relative_path:
        raise HTTPException(status_code=400, detail="write_file requires relative_path.")
    content = str(args.get("content", ""))
    mode = str(args.get("mode", "overwrite") or "overwrite").strip().lower()
    if mode not in {"overwrite", "append"}:
        raise HTTPException(status_code=400, detail="write_file mode must be overwrite or append.")
    create_parents = bool(args.get("create_parents", True))

    target = resolve_tool_path(root, relative_path)
    if target.exists() and target.is_dir():
        raise HTTPException(status_code=400, detail="write_file target cannot be a directory.")
    if create_parents:
        target.parent.mkdir(parents=True, exist_ok=True)

    encoding = "utf-8"
    before_text = ""
    if target.exists() and target.is_file():
        try:
            before_text = sniff_text(target, max_bytes=MAX_FILE_BYTES)
        except HTTPException:
            before_text = ""

    if mode == "append":
        with target.open("a", encoding=encoding, newline="") as handle:
            handle.write(content)
    else:
        target.write_text(content, encoding=encoding)

    after_text = target.read_text(encoding=encoding, errors="replace")
    preview, preview_truncated = truncate_text(after_text, 3000)
    return {
        "tool": "write_file",
        "relative_path": target.relative_to(root).as_posix(),
        "mode": mode,
        "bytes_written": len(content.encode(encoding, errors="replace")),
        "previous_exists": target.exists() or bool(before_text),
        "preview": preview,
        "preview_truncated": preview_truncated,
    }


def run_command_tool(session: AgentSession, root: Path, args: dict[str, Any]) -> dict[str, Any]:
    require_permission(session, "shell_enabled", "run_command")
    ensure_shell_execution_available()
    command = str(args.get("command", "") or "").strip()
    if not command:
        raise HTTPException(status_code=400, detail="run_command requires command.")
    relative_cwd = str(args.get("relative_cwd", ".") or ".").strip() or "."
    timeout_seconds = min(MAX_COMMAND_TIMEOUT, max(1, int(args.get("timeout_seconds", 20) or 20)))
    cwd = resolve_tool_path(root, relative_cwd)
    if not cwd.exists() or not cwd.is_dir():
        raise HTTPException(status_code=400, detail=f"Invalid command cwd: {relative_cwd}")

    started_at = time.perf_counter()
    invocation, execution_cwd = build_shell_invocation(root, cwd, command)

    try:
        completed = subprocess.run(
            invocation,
            cwd=execution_cwd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            shell=False,
        )
        timed_out = False
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
        exit_code = completed.returncode
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        stdout = (exc.stdout or "") if isinstance(exc.stdout, str) else (exc.stdout or b"").decode("utf-8", errors="replace")
        stderr = (exc.stderr or "") if isinstance(exc.stderr, str) else (exc.stderr or b"").decode("utf-8", errors="replace")
        exit_code = None

    combined = []
    if stdout.strip():
        combined.append(f"[stdout]\n{stdout.strip()}")
    if stderr.strip():
        combined.append(f"[stderr]\n{stderr.strip()}")
    output_text = "\n\n".join(combined).strip() or "(no output)"
    output_preview, output_truncated = truncate_text(output_text, MAX_COMMAND_OUTPUT_CHARS)

    return {
        "tool": "run_command",
        "command": command,
        "relative_cwd": cwd.relative_to(root).as_posix() if cwd != root else ".",
        "exit_code": exit_code,
        "timed_out": timed_out,
        "timeout_seconds": timeout_seconds,
        "elapsed_ms": round((time.perf_counter() - started_at) * 1000, 2),
        "output": output_preview,
        "output_truncated": output_truncated,
    }


TOOLS: dict[str, Any] = {
    "list_dir": list_dir_tool,
    "read_file": read_file_tool,
    "search_text": search_text_tool,
    "write_file": write_file_tool,
    "run_command": run_command_tool,
}


def format_tool_result_for_model(result: dict[str, Any]) -> str:
    return json.dumps(result, ensure_ascii=False, indent=2)


GREETING_RE = re.compile(
    r"^\s*(hi|hello|hey|yo|hola|你好|您好|嗨|哈喽|在吗|在麼|在嘛|早|早上好|中午好|下午好|晚上好|thanks|thank you|谢谢|谢了|收到)\s*[!！,，。.\?？~～]*\s*$",
    flags=re.IGNORECASE,
)


def build_smalltalk_reply(user_message: str) -> str | None:
    text = str(user_message or "").strip()
    if not text:
        return None
    if GREETING_RE.fullmatch(text):
        return "你好，我在。你可以直接告诉我想看哪个目录、查什么问题，或者让我直接改代码。"
    return None


def build_agent_system_prompt(session: AgentSession) -> str:
    permissions = serialize_permissions(session.permissions)
    return (
        "You are Workspace Agent, a local coding assistant for a directory.\n"
        "You must inspect the workspace with tools before making claims about files.\n"
        "For greetings, pleasantries, or requests that do not require workspace facts, reply with a final answer immediately and do not use tools.\n"
        "Return JSON only, with no markdown fences.\n\n"
        "Valid response schemas:\n"
        '{"type":"tool","tool":"list_dir","args":{"relative_path":"."},"reason":"why"}\n'
        '{"type":"tool","tool":"read_file","args":{"relative_path":"src/app.py","start_line":1,"max_lines":160},"reason":"why"}\n'
        '{"type":"tool","tool":"search_text","args":{"query":"FastAPI","relative_path":"."},"reason":"why"}\n'
        '{"type":"tool","tool":"write_file","args":{"relative_path":"notes.txt","content":"hello","mode":"overwrite"},"reason":"why"}\n'
        '{"type":"tool","tool":"run_command","args":{"command":"Get-ChildItem","relative_cwd":".","timeout_seconds":20},"reason":"why"}\n'
        '{"type":"final","answer":"your answer to the user"}\n\n'
        "Tool rules:\n"
        "- Paths must always be relative to the workspace root.\n"
        "- Use list_dir to discover structure.\n"
        "- Use read_file for concrete evidence.\n"
        "- Use search_text to locate symbols or text quickly.\n"
        "- Only use write_file when the task needs an actual file change.\n"
        "- Only use run_command when command output is necessary.\n"
        "- Never invent file contents or command output.\n"
        "- If a permission is disabled, do not request that tool unless you explicitly explain the blocker.\n"
        "- Stop after enough evidence and answer clearly.\n\n"
        f"Workspace root: {session.workspace_root}\n"
        f"Permissions: {json.dumps(permissions, ensure_ascii=False)}"
    )


def coerce_tool_parameter_value(raw_value: str) -> Any:
    text = str(raw_value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None
    if re.fullmatch(r"-?\d+", text):
        try:
            return int(text)
        except ValueError:
            pass
    if re.fullmatch(r"-?\d+\.\d+", text):
        try:
            return float(text)
        except ValueError:
            pass
    if text[:1] in {'[', '{', '"'}:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
    return text


def parse_minimax_tool_call(raw: str) -> dict[str, Any] | None:
    invoke_match = re.search(
        r"<invoke\s+name=['\"](?P<tool>[^'\"]+)['\"]\s*>(?P<body>.*?)</invoke>",
        raw,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not invoke_match:
        return None

    args: dict[str, Any] = {}
    body = invoke_match.group("body")
    for match in re.finditer(
        r"<parameter\s+name=['\"](?P<name>[^'\"]+)['\"]\s*>(?P<value>.*?)</parameter>",
        body,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        args[str(match.group("name")).strip()] = coerce_tool_parameter_value(match.group("value"))

    return {
        "type": "tool",
        "tool": str(invoke_match.group("tool")).strip(),
        "args": args,
        "reason": "Parsed from MiniMax tool call markup.",
    }


def collect_action_dicts(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def parse_agent_actions(text: str) -> list[dict[str, Any]]:
    raw = str(text or "").strip()
    if not raw:
        return []
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

    minimax_action = parse_minimax_tool_call(raw)
    if minimax_action is not None:
        return [minimax_action]

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        decoder = json.JSONDecoder()
        actions: list[dict[str, Any]] = []
        index = 0
        while index < len(raw):
            match = re.search(r"[\{\[]", raw[index:])
            if not match:
                break
            start = index + match.start()
            try:
                payload, end = decoder.raw_decode(raw, start)
            except json.JSONDecodeError:
                index = start + 1
                continue
            actions.extend(collect_action_dicts(payload))
            index = end
        return actions
    return collect_action_dicts(data)


def get_session_or_404(session_id: str) -> AgentSession:
    with SESSIONS_LOCK:
        session = SESSIONS.get(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Agent session not found.")
    return session


def build_llm_messages(history: list[SessionMessage], user_message: str) -> list[dict[str, str]]:
    clipped = history[-MAX_HISTORY_MESSAGES:]
    messages = [{"role": item.role, "content": item.content} for item in clipped]
    messages.append({"role": "user", "content": user_message})
    return messages


def build_done_payload(session: AgentSession, assistant_entry: SessionMessage) -> dict[str, Any]:
    return {
        "session": serialize_session(session),
        "assistant": serialize_message(assistant_entry),
    }


def iter_agent_turn(session: AgentSession, user_message: str) -> Iterator[tuple[str, dict[str, Any]]]:
    smalltalk_reply = build_smalltalk_reply(user_message)
    if smalltalk_reply is not None:
        user_entry = SessionMessage(role="user", content=user_message)
        assistant_entry = SessionMessage(role="assistant", content=smalltalk_reply, tool_steps=[])
        session.history.extend([user_entry, assistant_entry])
        session.updated_at = time.time()
        yield "done", build_done_payload(session, assistant_entry)
        return

    root = validate_workspace_root(session.workspace_root)
    working_messages = build_llm_messages(session.history, user_message)
    tool_steps: list[dict[str, Any]] = []
    final_answer = ""

    yield "status", {"text": "Agent 正在分析请求..."}
    for _ in range(MAX_TOOL_STEPS):
        yield "status", {"text": "正在请求模型决策..."}
        try:
            completion = send_chat(
                working_messages,
                session.model,
                build_agent_system_prompt(session),
                session_id=session.id,
                max_tokens=1400,
            )
        except Exception as exc:
            final_answer = f"本轮请求失败：{exc}"
            break
        actions = parse_agent_actions(completion["text"])
        if not actions:
            final_answer = completion["text"].strip() or "Agent did not return a usable response."
            break

        remaining_steps = MAX_TOOL_STEPS - len(tool_steps)
        if remaining_steps <= 0:
            break

        completed_tool_round = False
        for action in actions[:remaining_steps]:
            if action.get("type") == "final":
                final_answer = str(action.get("answer", "") or "").strip()
                if not final_answer:
                    final_answer = "Agent finished without a final answer."
                break

            if action.get("type") != "tool":
                final_answer = completion["text"].strip() or "Agent returned an unsupported action."
                break

            tool_name = str(action.get("tool", "") or "").strip()
            tool_fn = TOOLS.get(tool_name)
            if tool_fn is None:
                final_answer = f"Agent requested an unknown tool: {tool_name}"
                break

            args = action.get("args") if isinstance(action.get("args"), dict) else {}
            reason = str(action.get("reason", "") or "").strip()
            step_index = len(tool_steps)
            yield "tool_call", {
                "index": step_index,
                "tool": tool_name,
                "reason": reason,
                "args": args,
            }
            yield "status", {"text": f"正在执行工具：{tool_name}"}
            try:
                result = tool_fn(session, root, args)
            except HTTPException as exc:
                result = {
                    "tool": tool_name,
                    "error": exc.detail,
                }
            tool_step = {
                "tool": tool_name,
                "reason": reason,
                "args": args,
                "result": result,
            }
            tool_steps.append(tool_step)
            yield "tool_result", {
                "index": step_index,
                **tool_step,
            }
            working_messages.append({"role": "assistant", "content": json.dumps(action, ensure_ascii=False)})
            working_messages.append(
                {
                    "role": "user",
                    "content": (
                        f"Tool result for {tool_name}:\n"
                        f"{format_tool_result_for_model(result)}\n\n"
                        "Continue using tools if needed, otherwise return a final answer JSON object."
                    ),
                }
            )
            completed_tool_round = True

        if final_answer:
            break
        if not completed_tool_round:
            final_answer = completion["text"].strip() or "Agent did not return a usable response."
            break

    if not final_answer:
        final_answer = (
            "I inspected the workspace but hit the tool-step limit before I could finish. "
            "Try narrowing the request or asking about a smaller area."
        )

    user_entry = SessionMessage(role="user", content=user_message)
    assistant_entry = SessionMessage(role="assistant", content=final_answer, tool_steps=tool_steps)
    session.history.extend([user_entry, assistant_entry])
    session.updated_at = time.time()
    yield "done", build_done_payload(session, assistant_entry)


def run_agent_turn(session: AgentSession, user_message: str) -> SessionMessage:
    assistant_payload: dict[str, Any] | None = None
    for event_name, payload in iter_agent_turn(session, user_message):
        if event_name == "done":
            assistant_payload = payload.get("assistant")

    if not assistant_payload:
        raise RuntimeError("Agent turn finished without an assistant payload.")

    return SessionMessage(
        role=str(assistant_payload.get("role", "assistant") or "assistant"),
        content=str(assistant_payload.get("content", "") or ""),
        tool_steps=list(assistant_payload.get("tool_steps") or []),
        created_at=float(assistant_payload.get("created_at", time.time()) or time.time()),
    )


def open_workspace_picker() -> str:
    if not parse_env_flag("AGENT_ENABLE_WORKSPACE_PICKER", default=os.name == "nt"):
        raise HTTPException(
            status_code=403,
            detail="Workspace picker is disabled. Enter a workspace path manually on the server.",
        )
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Folder picker is unavailable: {exc}") from exc

    root = tk.Tk()
    root.withdraw()
    try:
        root.attributes("-topmost", True)
    except Exception:
        pass
    try:
        selected = filedialog.askdirectory(mustexist=True)
    finally:
        root.destroy()
    if not selected:
        raise HTTPException(status_code=400, detail="Folder selection was cancelled.")
    return str(validate_workspace_root(selected))


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "default_model": DEFAULT_MODEL,
            "models": sorted(SUPPORTED_MODELS),
        },
    )


@app.get("/api/models")
def api_models() -> dict[str, Any]:
    return {"models": sorted(SUPPORTED_MODELS), "default_model": DEFAULT_MODEL}


@app.post("/api/pick-workspace")
def api_pick_workspace() -> dict[str, str]:
    return {"workspace_root": open_workspace_picker()}


@app.post("/api/sessions")
def api_create_session(payload: CreateSessionRequest) -> dict[str, Any]:
    root = validate_workspace_root(payload.workspace_root)
    session = AgentSession(
        id=str(uuid.uuid4()),
        workspace_root=str(root),
        model=normalize_model(payload.model),
    )
    with SESSIONS_LOCK:
        SESSIONS[session.id] = session
    return {
        "session": serialize_session(session),
        "workspace_preview": list_dir_tool(session, root, {"relative_path": "."}),
    }


@app.get("/api/sessions/{session_id}")
def api_get_session(session_id: str) -> dict[str, Any]:
    session = get_session_or_404(session_id)
    root = validate_workspace_root(session.workspace_root)
    return {
        "session": serialize_session(session),
        "workspace_preview": list_dir_tool(session, root, {"relative_path": "."}),
    }


@app.patch("/api/sessions/{session_id}/workspace")
def api_update_workspace(session_id: str, payload: UpdateWorkspaceRequest) -> dict[str, Any]:
    session = get_session_or_404(session_id)
    root = validate_workspace_root(payload.workspace_root)
    session.workspace_root = str(root)
    session.permissions.read_enabled = True
    session.updated_at = time.time()
    return {
        "session": serialize_session(session),
        "workspace_preview": list_dir_tool(session, root, {"relative_path": "."}),
    }


@app.patch("/api/sessions/{session_id}/permissions")
def api_update_permissions(session_id: str, payload: UpdatePermissionsRequest) -> dict[str, Any]:
    session = get_session_or_404(session_id)
    if payload.write_enabled is not None:
        session.permissions.write_enabled = bool(payload.write_enabled)
    if payload.shell_enabled is not None:
        if payload.shell_enabled:
            ensure_shell_execution_available()
        session.permissions.shell_enabled = bool(payload.shell_enabled)
    session.updated_at = time.time()
    return {"session": serialize_session(session)}


@app.delete("/api/sessions/{session_id}")
def api_delete_session(session_id: str) -> dict[str, bool]:
    with SESSIONS_LOCK:
        session = SESSIONS.pop(session_id, None)
    if session is None:
        raise HTTPException(status_code=404, detail="Agent session not found.")
    return {"ok": True}


@app.post("/api/sessions/{session_id}/chat")
def api_chat(session_id: str, payload: ChatTurnRequest) -> dict[str, Any]:
    session = get_session_or_404(session_id)
    message_text = payload.message.strip()
    if not message_text:
        raise HTTPException(status_code=400, detail="Message cannot be empty.")
    session.model = normalize_model(payload.model or session.model)
    assistant_message = run_agent_turn(session, message_text)
    return {
        "session": serialize_session(session),
        "assistant": serialize_message(assistant_message),
    }


@app.post("/api/sessions/{session_id}/chat/stream")
def api_chat_stream(session_id: str, payload: ChatTurnRequest) -> StreamingResponse:
    session = get_session_or_404(session_id)
    message_text = payload.message.strip()
    if not message_text:
        raise HTTPException(status_code=400, detail="Message cannot be empty.")
    session.model = normalize_model(payload.model or session.model)

    def event_stream() -> Iterator[str]:
        yield sse_event(
            "start",
            {
                "session_id": session.id,
                "workspace_root": session.workspace_root,
                "model": session.model,
            },
        )
        try:
            for event_name, event_payload in iter_agent_turn(session, message_text):
                yield sse_event(event_name, event_payload)
        except Exception as exc:
            yield sse_event("error", {"detail": str(exc)})

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/api/sessions/{session_id}/workspace")
def api_list_workspace(session_id: str, relative_path: str = Query(default=".")) -> dict[str, Any]:
    session = get_session_or_404(session_id)
    root = validate_workspace_root(session.workspace_root)
    return list_dir_tool(session, root, {"relative_path": relative_path})


@app.get("/api/sessions/{session_id}/file")
def api_read_file(
    session_id: str,
    relative_path: str,
    start_line: int = Query(default=1, ge=1),
    max_lines: int = Query(default=160, ge=1, le=MAX_READ_LINES),
) -> dict[str, Any]:
    session = get_session_or_404(session_id)
    root = validate_workspace_root(session.workspace_root)
    return read_file_tool(
        session,
        root,
        {
            "relative_path": relative_path,
            "start_line": start_line,
            "max_lines": max_lines,
        },
    )


@app.get("/api/sessions/{session_id}/file-content")
def api_read_file_content(session_id: str, relative_path: str) -> dict[str, Any]:
    session = get_session_or_404(session_id)
    root = validate_workspace_root(session.workspace_root)
    return read_file_content(session, root, relative_path)


@app.get("/api/sessions/{session_id}/search")
def api_search_workspace(
    session_id: str,
    query: str,
    relative_path: str = Query(default="."),
) -> dict[str, Any]:
    session = get_session_or_404(session_id)
    root = validate_workspace_root(session.workspace_root)
    return search_text_tool(session, root, {"query": query, "relative_path": relative_path})


@app.post("/api/sessions/{session_id}/write-file")
def api_write_file(session_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    session = get_session_or_404(session_id)
    root = validate_workspace_root(session.workspace_root)
    return write_file_tool(session, root, payload)


@app.post("/api/sessions/{session_id}/run-command")
def api_run_command(session_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    session = get_session_or_404(session_id)
    root = validate_workspace_root(session.workspace_root)
    return run_command_tool(session, root, payload)
