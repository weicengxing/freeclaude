import sqlite3
import uuid
from contextlib import closing
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from uuapi_client import DEFAULT_MODEL, SUPPORTED_MODELS, normalize_model, send_chat


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
        session = connection.execute(
            "SELECT id, title, model, created_at, updated_at FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        return {
            "session": dict(session),
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

        session = connection.execute(
            "SELECT id, title, model, created_at, updated_at FROM sessions WHERE id = ?",
            (payload.session_id,),
        ).fetchone()
        messages = get_messages(connection, payload.session_id)

    return {
        "session": dict(session),
        "messages": messages,
        "reply": assistant_text,
    }
