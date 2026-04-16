import json
import sqlite3
import uuid
from contextlib import closing
from datetime import datetime
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from uuapi_client import DEFAULT_MODEL, SUPPORTED_MODELS, iter_stream_chat, normalize_model, send_chat


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "chat_app.db"
DEFAULT_KEY_SOURCE_URL = "https://github.com/weicengxing/freeclaude/blob/main/key.txt"

app = FastAPI(title="UUAPI Web Chat")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


class CreateSessionRequest(BaseModel):
    model: str = DEFAULT_MODEL


class ChatRequest(BaseModel):
    session_id: str
    message: str = Field(min_length=1)
    model: str = DEFAULT_MODEL


class UpdateSessionRequest(BaseModel):
    title: str = Field(min_length=1, max_length=200)


class EditMessageRequest(BaseModel):
    message: str = Field(min_length=1)
    model: str = DEFAULT_MODEL


def utc_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    with closing(get_connection()) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                model TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (session_id) REFERENCES sessions(id)
            );

            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_key TEXT NOT NULL,
                source_url TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_api_key
            ON api_keys(api_key);
            """
        )
        connection.commit()


def session_exists(connection: sqlite3.Connection, session_id: str) -> bool:
    row = connection.execute("SELECT 1 FROM sessions WHERE id = ?", (session_id,)).fetchone()
    return row is not None


def build_title(first_message: str) -> str:
    compact = " ".join(first_message.strip().split())
    return compact[:40] or "New Chat"


def create_session_record(connection: sqlite3.Connection, session_id: str, model: str, title: str = "New Chat") -> None:
    now = utc_now()
    connection.execute(
        """
        INSERT INTO sessions (id, title, model, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (session_id, title, normalize_model(model), now, now),
    )
    connection.commit()


def add_message(connection: sqlite3.Connection, session_id: str, role: str, content: str) -> None:
    now = utc_now()
    connection.execute(
        """
        INSERT INTO messages (session_id, role, content, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (session_id, role, content, now),
    )
    connection.execute(
        "UPDATE sessions SET updated_at = ? WHERE id = ?",
        (now, session_id),
    )
    connection.commit()


def update_session_metadata(connection: sqlite3.Connection, session_id: str, model: str, title: str | None = None) -> None:
    now = utc_now()
    if title is None:
        connection.execute(
            "UPDATE sessions SET model = ?, updated_at = ? WHERE id = ?",
            (normalize_model(model), now, session_id),
        )
    else:
        connection.execute(
            "UPDATE sessions SET title = ?, model = ?, updated_at = ? WHERE id = ?",
            (title, normalize_model(model), now, session_id),
        )
    connection.commit()


def list_sessions(connection: sqlite3.Connection) -> list[dict]:
    rows = connection.execute(
        """
        SELECT id, title, model, created_at, updated_at
        FROM sessions
        ORDER BY updated_at DESC, created_at DESC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def get_messages(connection: sqlite3.Connection, session_id: str) -> list[dict]:
    rows = connection.execute(
        """
        SELECT id, session_id, role, content, created_at
        FROM messages
        WHERE session_id = ?
        ORDER BY id ASC
        """,
        (session_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def get_session(connection: sqlite3.Connection, session_id: str) -> dict | None:
    session = connection.execute(
        "SELECT id, title, model, created_at, updated_at FROM sessions WHERE id = ?",
        (session_id,),
    ).fetchone()
    return dict(session) if session is not None else None


def get_message(connection: sqlite3.Connection, session_id: str, message_id: int) -> dict | None:
    row = connection.execute(
        """
        SELECT id, session_id, role, content, created_at
        FROM messages
        WHERE session_id = ? AND id = ?
        """,
        (session_id, message_id),
    ).fetchone()
    return dict(row) if row is not None else None


def delete_messages_from(connection: sqlite3.Connection, session_id: str, message_id: int) -> None:
    now = utc_now()
    connection.execute(
        "DELETE FROM messages WHERE session_id = ? AND id >= ?",
        (session_id, message_id),
    )
    connection.execute(
        "UPDATE sessions SET updated_at = ? WHERE id = ?",
        (now, session_id),
    )
    connection.commit()


def delete_session_record(connection: sqlite3.Connection, session_id: str) -> None:
    connection.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))
    connection.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
    connection.commit()


def trim_title(title: str) -> str:
    compact = " ".join(title.strip().split())
    return compact[:200] or "New Chat"


def normalize_github_raw_url(url: str) -> str:
    stripped = url.strip()
    if not stripped:
        raise HTTPException(status_code=400, detail="Key source URL cannot be empty")

    if stripped.startswith("https://raw.githubusercontent.com/"):
        return stripped

    marker = "https://github.com/"
    if stripped.startswith(marker) and "/blob/" in stripped:
        path = stripped[len(marker):]
        owner_repo, remainder = path.split("/blob/", 1)
        return f"https://raw.githubusercontent.com/{owner_repo}/{remainder}"

    raise HTTPException(status_code=400, detail="Unsupported GitHub URL")


def parse_api_keys_from_text(raw_text: str) -> list[str]:
    unique_keys: list[str] = []
    seen: set[str] = set()
    for line in raw_text.splitlines():
        key = line.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        unique_keys.append(key)
    return unique_keys


def import_api_keys_from_url(connection: sqlite3.Connection, source_url: str) -> dict:
    raw_url = normalize_github_raw_url(source_url)
    try:
        response = httpx.get(raw_url, timeout=30.0, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch key file: {exc}") from exc

    parsed_keys = parse_api_keys_from_text(response.text)
    if not parsed_keys:
        raise HTTPException(status_code=400, detail="No valid keys found in the source file")

    inserted_count = 0
    now = utc_now()
    for api_key in parsed_keys:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO api_keys (api_key, source_url, created_at)
            VALUES (?, ?, ?)
            """,
            (api_key, source_url, now),
        )
        inserted_count += cursor.rowcount
    connection.commit()

    total_keys = connection.execute("SELECT COUNT(*) AS count FROM api_keys").fetchone()["count"]
    return {
        "source_url": source_url,
        "raw_url": raw_url,
        "read_count": len(parsed_keys),
        "inserted_count": inserted_count,
        "ignored_count": len(parsed_keys) - inserted_count,
        "total_keys": total_keys,
    }


def get_latest_api_key(connection: sqlite3.Connection) -> str | None:
    row = connection.execute(
        """
        SELECT api_key
        FROM api_keys
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    return row["api_key"] if row is not None else None


def build_request_messages(history: list[dict]) -> list[dict[str, str]]:
    return [{"role": item["role"], "content": item["content"]} for item in history]


def restore_messages(connection: sqlite3.Connection, messages: list[dict]) -> None:
    for item in messages:
        connection.execute(
            """
            INSERT INTO messages (id, session_id, role, content, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (item["id"], item["session_id"], item["role"], item["content"], item["created_at"]),
        )
    connection.commit()


def send_and_persist_reply(
    connection: sqlite3.Connection,
    session_id: str,
    message_text: str,
    model: str,
    replace_from_message_id: int | None = None,
) -> tuple[dict, list[dict], str]:
    if not session_exists(connection, session_id):
        create_session_record(connection, session_id, model)

    current_session = get_session(connection, session_id)
    removed_suffix: list[dict] = []
    if replace_from_message_id is not None:
        target_message = get_message(connection, session_id, replace_from_message_id)
        if target_message is None:
            raise HTTPException(status_code=404, detail="Message not found")
        removed_suffix = [
            item for item in get_messages(connection, session_id) if item["id"] >= replace_from_message_id
        ]
        delete_messages_from(connection, session_id, replace_from_message_id)

    history_before = get_messages(connection, session_id)
    add_message(connection, session_id, "user", message_text)

    should_autobuild_title = not history_before and (current_session is None or current_session["title"] == "New Chat")
    title = build_title(message_text) if should_autobuild_title else None
    request_messages = build_request_messages([*history_before, {"role": "user", "content": message_text}])

    try:
        response = send_chat(
            messages=request_messages,
            model=model,
            session_id=session_id,
            api_key=get_latest_api_key(connection),
        )
    except Exception as exc:
        # Roll back the newly inserted user message so persisted history always matches AI history.
        latest_user = connection.execute(
            """
            SELECT id
            FROM messages
            WHERE session_id = ? AND role = 'user'
            ORDER BY id DESC
            LIMIT 1
            """,
            (session_id,),
        ).fetchone()
        if latest_user is not None:
            connection.execute("DELETE FROM messages WHERE id = ?", (latest_user["id"],))
            connection.commit()
        if removed_suffix:
            restore_messages(connection, removed_suffix)
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {exc}") from exc

    assistant_text = response["text"].strip() or "(empty response)"
    add_message(connection, session_id, "assistant", assistant_text)
    update_session_metadata(connection, session_id, response["model"], title=title)
    session = get_session(connection, session_id)
    messages = get_messages(connection, session_id)
    return session, messages, assistant_text


def sse_event(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def extract_stream_text(chunk: dict) -> str:
    data = chunk.get("data", {})
    if data.get("type") == "content_block_delta":
        delta = data.get("delta", {})
        if delta.get("type") == "text_delta":
            return delta.get("text", "")
    return ""


@app.on_event("startup")
def startup() -> None:
    init_db()


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "models": sorted(SUPPORTED_MODELS),
            "default_model": DEFAULT_MODEL,
        },
    )


@app.get("/api/models")
def api_models() -> dict:
    return {
        "models": sorted(SUPPORTED_MODELS),
        "default_model": DEFAULT_MODEL,
    }


@app.get("/api/keys")
def api_keys_status() -> dict:
    with closing(get_connection()) as connection:
        total_keys = connection.execute("SELECT COUNT(*) AS count FROM api_keys").fetchone()["count"]
    return {
        "source_url": DEFAULT_KEY_SOURCE_URL,
        "total_keys": total_keys,
    }


@app.post("/api/keys/import")
def api_import_keys() -> dict:
    with closing(get_connection()) as connection:
        return import_api_keys_from_url(connection, DEFAULT_KEY_SOURCE_URL)


@app.get("/api/sessions")
def api_list_sessions() -> list[dict]:
    with closing(get_connection()) as connection:
        return list_sessions(connection)


@app.post("/api/sessions")
def api_create_session(payload: CreateSessionRequest) -> dict:
    session_id = str(uuid.uuid4())
    with closing(get_connection()) as connection:
        create_session_record(connection, session_id, payload.model)
        session = connection.execute(
            "SELECT id, title, model, created_at, updated_at FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
    return dict(session)


@app.get("/api/sessions/{session_id}")
def api_get_session(session_id: str) -> dict:
    with closing(get_connection()) as connection:
        session = get_session(connection, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        return {
            "session": session,
            "messages": get_messages(connection, session_id),
        }


@app.patch("/api/sessions/{session_id}")
def api_update_session(session_id: str, payload: UpdateSessionRequest) -> dict:
    with closing(get_connection()) as connection:
        if not session_exists(connection, session_id):
            raise HTTPException(status_code=404, detail="Session not found")
        update_session_metadata(connection, session_id, get_session(connection, session_id)["model"], title=trim_title(payload.title))
        session = get_session(connection, session_id)
    return {"session": session}


@app.delete("/api/sessions/{session_id}")
def api_delete_session(session_id: str) -> dict:
    with closing(get_connection()) as connection:
        if not session_exists(connection, session_id):
            raise HTTPException(status_code=404, detail="Session not found")
        delete_session_record(connection, session_id)
    return {"ok": True}


@app.delete("/api/sessions/{session_id}/messages/{message_id}")
def api_delete_message(session_id: str, message_id: int) -> dict:
    with closing(get_connection()) as connection:
        session = get_session(connection, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        message = get_message(connection, session_id, message_id)
        if message is None:
            raise HTTPException(status_code=404, detail="Message not found")

        delete_messages_from(connection, session_id, message_id)
        remaining_messages = get_messages(connection, session_id)

        if not remaining_messages:
            title = "New Chat"
        else:
            first_user = next((item for item in remaining_messages if item["role"] == "user"), None)
            title = session["title"] if session["title"] != "New Chat" else build_title(first_user["content"]) if first_user else "New Chat"

        update_session_metadata(connection, session_id, session["model"], title=title)
        updated_session = get_session(connection, session_id)

    return {
        "session": updated_session,
        "messages": remaining_messages,
    }


@app.post("/api/sessions/{session_id}/messages/{message_id}/resend")
def api_resend_message(session_id: str, message_id: int, payload: EditMessageRequest) -> dict:
    message_text = payload.message.strip()
    if not message_text:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    with closing(get_connection()) as connection:
        target_message = get_message(connection, session_id, message_id)
        if target_message is None:
            raise HTTPException(status_code=404, detail="Message not found")
        if target_message["role"] != "user":
            raise HTTPException(status_code=400, detail="Only user messages can be edited and resent")

        session, messages, assistant_text = send_and_persist_reply(
            connection=connection,
            session_id=session_id,
            message_text=message_text,
            model=payload.model,
            replace_from_message_id=message_id,
        )

    return {
        "session": session,
        "messages": messages,
        "reply": assistant_text,
    }


@app.post("/api/chat")
def api_chat(payload: ChatRequest) -> dict:
    message_text = payload.message.strip()
    if not message_text:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    with closing(get_connection()) as connection:
        session, messages, assistant_text = send_and_persist_reply(
            connection=connection,
            session_id=payload.session_id,
            message_text=message_text,
            model=payload.model,
        )

    return {
        "session": session,
        "messages": messages,
        "reply": assistant_text,
    }


@app.post("/api/chat/stream")
def api_chat_stream(payload: ChatRequest) -> StreamingResponse:
    message_text = payload.message.strip()
    if not message_text:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    with closing(get_connection()) as connection:
        if not session_exists(connection, payload.session_id):
            create_session_record(connection, payload.session_id, payload.model)

        current_session = get_session(connection, payload.session_id)
        history_before = get_messages(connection, payload.session_id)
        add_message(connection, payload.session_id, "user", message_text)
        inserted_user = connection.execute(
            """
            SELECT id, session_id, role, content, created_at
            FROM messages
            WHERE session_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (payload.session_id,),
        ).fetchone()

        should_autobuild_title = not history_before and (current_session is None or current_session["title"] == "New Chat")
        title = build_title(message_text) if should_autobuild_title else None
        selected_api_key = get_latest_api_key(connection)
        request_messages = [
            {"role": item["role"], "content": item["content"]}
            for item in [*history_before, {"role": "user", "content": message_text}]
        ]
        session = get_session(connection, payload.session_id)

    def event_stream():
        assistant_parts: list[str] = []
        response_model = normalize_model(payload.model)

        try:
            yield sse_event(
                "start",
                {
                    "session": session,
                    "message": {"role": "user", "content": message_text},
                },
            )

            for chunk in iter_stream_chat(
                messages=request_messages,
                model=payload.model,
                session_id=payload.session_id,
                api_key=selected_api_key,
            ):
                response_model = chunk.get("data", {}).get("model", response_model)
                text_delta = extract_stream_text(chunk)
                if text_delta:
                    assistant_parts.append(text_delta)
                    yield sse_event("delta", {"text": text_delta})

            assistant_text = "".join(assistant_parts).strip() or "(empty response)"

            with closing(get_connection()) as connection:
                add_message(connection, payload.session_id, "assistant", assistant_text)
                update_session_metadata(connection, payload.session_id, response_model, title=title)
                latest_session = get_session(connection, payload.session_id)

            yield sse_event(
                "done",
                {
                    "session": latest_session,
                    "reply": assistant_text,
                },
            )
        except Exception as exc:
            with closing(get_connection()) as rollback_connection:
                if inserted_user is not None:
                    rollback_connection.execute("DELETE FROM messages WHERE id = ?", (inserted_user["id"],))
                    rollback_connection.commit()
            yield sse_event("error", {"detail": f"Upstream request failed: {exc}"})

    return StreamingResponse(event_stream(), media_type="text/event-stream")
