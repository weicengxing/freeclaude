import json
import sqlite3
import uuid
from contextlib import closing
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from uuapi_client import DEFAULT_MODEL, SUPPORTED_MODELS, iter_stream_chat, normalize_model, send_chat


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "chat_app.db"

app = FastAPI(title="UUAPI Web Chat")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


class CreateSessionRequest(BaseModel):
    model: str = DEFAULT_MODEL


class ChatRequest(BaseModel):
    session_id: str
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


@app.post("/api/chat")
def api_chat(payload: ChatRequest) -> dict:
    message_text = payload.message.strip()
    if not message_text:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    with closing(get_connection()) as connection:
        if not session_exists(connection, payload.session_id):
            create_session_record(connection, payload.session_id, payload.model)

        history_before = get_messages(connection, payload.session_id)
        add_message(connection, payload.session_id, "user", message_text)

        title = None
        if not history_before:
            title = build_title(message_text)

        request_messages = [
            {"role": item["role"], "content": item["content"]}
            for item in [*history_before, {"role": "user", "content": message_text}]
        ]

        try:
            response = send_chat(
                messages=request_messages,
                model=payload.model,
                session_id=payload.session_id,
            )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Upstream request failed: {exc}") from exc

        assistant_text = response["text"].strip() or "(empty response)"
        add_message(connection, payload.session_id, "assistant", assistant_text)
        update_session_metadata(connection, payload.session_id, response["model"], title=title)

        session = get_session(connection, payload.session_id)
        messages = get_messages(connection, payload.session_id)

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

        history_before = get_messages(connection, payload.session_id)
        add_message(connection, payload.session_id, "user", message_text)

        title = build_title(message_text) if not history_before else None
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
            yield sse_event("error", {"detail": f"Upstream request failed: {exc}"})

    return StreamingResponse(event_stream(), media_type="text/event-stream")
