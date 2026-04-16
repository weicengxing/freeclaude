import hashlib
import json
import logging
import os
import secrets
import smtplib
import sqlite3
import uuid
from contextlib import closing
from datetime import datetime, timedelta
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from fastapi import Cookie, FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, EmailStr, Field

from uuapi_client import DEFAULT_MODEL, SUPPORTED_MODELS, iter_stream_chat, normalize_model, send_chat


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "chat_app.db"
logger = logging.getLogger(__name__)

USER_SESSION_COOKIE = "user_session"
USER_SESSION_TTL_SECONDS = 60 * 60 * 24 * 7
ROLE_USER = "User"
ROLE_SUPERADMIN = "SuperAdmin"
VALID_ROLES = {ROLE_USER, ROLE_SUPERADMIN}
VERIFY_CODE_TTL_MINUTES = 10
VERIFY_PURPOSE_REGISTER = "register"
VERIFY_PURPOSE_RESET = "reset"

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


class RegisterVerifyRequest(BaseModel):
    email: EmailStr


class RegisterRequest(BaseModel):
    username: str = Field(min_length=2, max_length=50)
    email: EmailStr
    password: str = Field(min_length=6, max_length=128)
    confirmPassword: str = Field(min_length=6, max_length=128)
    verifyCode: str = Field(min_length=4, max_length=12)


class LoginRequest(BaseModel):
    username: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=1, max_length=128)


class ResetVerifyRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    email: EmailStr
    verifyCode: str = Field(min_length=4, max_length=12)
    password: str = Field(min_length=6, max_length=128)
    confirmPassword: str = Field(min_length=6, max_length=128)


class UpdateUserStatusRequest(BaseModel):
    is_active: bool


def utc_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def now_local_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def table_has_column(connection: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    columns = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(column["name"] == column_name for column in columns)


def hash_password(password: str, salt: str) -> str:
    hashed = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        120_000,
    )
    return hashed.hex()


def generate_verify_code(length: int = 6) -> str:
    return "".join(secrets.choice("0123456789") for _ in range(length))


def env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def get_smtp_config() -> dict[str, str | int | bool]:
    return {
        "host": os.getenv("SMTP_HOST", "smtp.qq.com").strip(),
        "port": int(os.getenv("SMTP_PORT", "587").strip()),
        "use_tls": env_flag("SMTP_USE_TLS", True),
        "username": os.getenv("SMTP_USERNAME", "").strip(),
        "password": os.getenv("SMTP_PASSWORD", "").strip(),
        "from_name": os.getenv("SMTP_FROM_NAME", "UUAPI Chat").strip() or "UUAPI Chat",
    }


def send_email(to_email: str, subject: str, html_content: str) -> bool:
    smtp_config = get_smtp_config()
    if not smtp_config["username"] or not smtp_config["password"]:
        logger.warning("SMTP is not configured; skipping email send to %s", to_email)
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = formataddr((str(smtp_config["from_name"]), str(smtp_config["username"])))
        msg["To"] = to_email
        msg.attach(MIMEText(html_content, "html", "utf-8"))

        server = smtplib.SMTP(str(smtp_config["host"]), int(smtp_config["port"]))
        if smtp_config.get("use_tls", True):
            server.starttls()
        server.login(str(smtp_config["username"]), str(smtp_config["password"]))
        server.sendmail(str(smtp_config["username"]), [to_email], msg.as_string())
        server.quit()
        return True
    except Exception:
        logger.exception("send_email failed")
        return False


def normalize_role(role: str | None) -> str:
    role = str(role or "").strip()
    return role if role in VALID_ROLES else ROLE_USER


def ensure_default_admins(connection: sqlite3.Connection) -> None:
    admins = connection.execute(
        "SELECT id FROM users WHERE role = ? ORDER BY id ASC",
        (ROLE_SUPERADMIN,),
    ).fetchall()
    if len(admins) >= 2:
        return

    defaults = [
        ("admin", "admin1@example.com"),
        ("root", "admin2@example.com"),
    ]
    used_usernames = {row["username"] for row in connection.execute("SELECT username FROM users").fetchall()}
    used_emails = {row["email"] for row in connection.execute("SELECT email FROM users").fetchall()}

    for username, email in defaults:
        if len(admins) >= 2:
            break
        if username in used_usernames or email in used_emails:
            continue
        salt = secrets.token_hex(16)
        connection.execute(
            """
            INSERT INTO users (username, email, role, password_hash, salt, is_active, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                username,
                email,
                ROLE_SUPERADMIN,
                hash_password("admin123456", salt),
                salt,
                1,
                now_local_text(),
            ),
        )
        admins.append({"id": connection.execute("SELECT last_insert_rowid()").fetchone()[0]})
        used_usernames.add(username)
        used_emails.add(email)


def get_default_session_owner_id(connection: sqlite3.Connection) -> int | None:
    row = connection.execute(
        """
        SELECT id
        FROM users
        WHERE role = ? AND is_active = 1
        ORDER BY id ASC
        LIMIT 1
        """,
        (ROLE_SUPERADMIN,),
    ).fetchone()
    if row is not None:
        return int(row["id"])

    row = connection.execute(
        """
        SELECT id
        FROM users
        WHERE is_active = 1
        ORDER BY id ASC
        LIMIT 1
        """
    ).fetchone()
    return int(row["id"]) if row is not None else None


def migrate_legacy_sessions(connection: sqlite3.Connection) -> None:
    default_owner_id = get_default_session_owner_id(connection)
    if default_owner_id is None:
        return
    connection.execute(
        "UPDATE sessions SET user_id = ? WHERE user_id IS NULL",
        (default_owner_id,),
    )


def cleanup_expired_verify_codes(connection: sqlite3.Connection) -> None:
    connection.execute("DELETE FROM verify_codes WHERE expires_at <= ?", (utc_now(),))


def upsert_verify_code(connection: sqlite3.Connection, email: str, purpose: str, code: str) -> None:
    cleanup_expired_verify_codes(connection)
    now = utc_now()
    expires_at = (datetime.utcnow() + timedelta(minutes=VERIFY_CODE_TTL_MINUTES)).replace(microsecond=0).isoformat() + "Z"
    connection.execute(
        """
        INSERT INTO verify_codes (email, purpose, code, created_at, expires_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(email, purpose) DO UPDATE SET
            code = excluded.code,
            created_at = excluded.created_at,
            expires_at = excluded.expires_at
        """,
        (email, purpose, code, now, expires_at),
    )
    connection.commit()


def get_verify_code_record(connection: sqlite3.Connection, email: str, purpose: str) -> dict | None:
    cleanup_expired_verify_codes(connection)
    connection.commit()
    row = connection.execute(
        """
        SELECT email, purpose, code, created_at, expires_at
        FROM verify_codes
        WHERE email = ? AND purpose = ?
        LIMIT 1
        """,
        (email, purpose),
    ).fetchone()
    return dict(row) if row is not None else None


def delete_verify_code(connection: sqlite3.Connection, email: str, purpose: str) -> None:
    connection.execute(
        "DELETE FROM verify_codes WHERE email = ? AND purpose = ?",
        (email, purpose),
    )
    connection.commit()


def init_db() -> None:
    with closing(get_connection()) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL UNIQUE,
                role TEXT NOT NULL DEFAULT 'User',
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS auth_sessions (
                token TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS verify_codes (
                email TEXT NOT NULL,
                purpose TEXT NOT NULL,
                code TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                PRIMARY KEY (email, purpose)
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                user_id INTEGER,
                title TEXT NOT NULL,
                model TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
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
        if not table_has_column(connection, "sessions", "user_id"):
            connection.execute("ALTER TABLE sessions ADD COLUMN user_id INTEGER")
        ensure_default_admins(connection)
        migrate_legacy_sessions(connection)
        cleanup_expired_verify_codes(connection)
        connection.commit()


def create_auth_session(connection: sqlite3.Connection, user_id: int) -> str:
    token = secrets.token_hex(24)
    created_at = utc_now()
    expires_at = (datetime.utcnow() + timedelta(seconds=USER_SESSION_TTL_SECONDS)).replace(microsecond=0).isoformat() + "Z"
    connection.execute(
        """
        INSERT INTO auth_sessions (token, user_id, created_at, expires_at)
        VALUES (?, ?, ?, ?)
        """,
        (token, user_id, created_at, expires_at),
    )
    connection.commit()
    return token


def delete_auth_session(connection: sqlite3.Connection, token: str | None) -> None:
    if not token:
        return
    connection.execute("DELETE FROM auth_sessions WHERE token = ?", (token,))
    connection.commit()


def get_current_user(connection: sqlite3.Connection, token: str | None) -> dict | None:
    if not token:
        return None

    row = connection.execute(
        """
        SELECT
            auth_sessions.token,
            auth_sessions.expires_at,
            users.id,
            users.username,
            users.email,
            users.role,
            users.is_active
        FROM auth_sessions
        JOIN users ON users.id = auth_sessions.user_id
        WHERE auth_sessions.token = ?
        LIMIT 1
        """,
        (token,),
    ).fetchone()
    if row is None:
        return None
    if row["is_active"] != 1:
        delete_auth_session(connection, token)
        return None

    expires_at = datetime.fromisoformat(row["expires_at"].replace("Z", "+00:00"))
    if expires_at <= datetime.utcnow().astimezone(expires_at.tzinfo):
        delete_auth_session(connection, token)
        return None

    return {
        "id": row["id"],
        "username": row["username"],
        "email": row["email"],
        "role": normalize_role(row["role"]),
    }


def require_user(connection: sqlite3.Connection, token: str | None) -> dict:
    user = get_current_user(connection, token)
    if user is None:
        raise HTTPException(status_code=401, detail="请先登录")
    return user


def require_superadmin(connection: sqlite3.Connection, token: str | None) -> dict:
    user = require_user(connection, token)
    if user["role"] != ROLE_SUPERADMIN:
        raise HTTPException(status_code=403, detail="仅 SuperAdmin 可操作")
    return user


def session_exists(connection: sqlite3.Connection, session_id: str, user_id: int) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sessions WHERE id = ? AND user_id = ?",
        (session_id, user_id),
    ).fetchone()
    return row is not None


def build_title(first_message: str) -> str:
    compact = " ".join(first_message.strip().split())
    return compact[:40] or "New Chat"


def create_session_record(
    connection: sqlite3.Connection,
    session_id: str,
    user_id: int,
    model: str,
    title: str = "New Chat",
) -> None:
    now = utc_now()
    connection.execute(
        """
        INSERT INTO sessions (id, user_id, title, model, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (session_id, user_id, title, normalize_model(model), now, now),
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


def update_session_metadata(
    connection: sqlite3.Connection,
    session_id: str,
    user_id: int,
    model: str,
    title: str | None = None,
) -> None:
    now = utc_now()
    if title is None:
        connection.execute(
            "UPDATE sessions SET model = ?, updated_at = ? WHERE id = ? AND user_id = ?",
            (normalize_model(model), now, session_id, user_id),
        )
    else:
        connection.execute(
            "UPDATE sessions SET title = ?, model = ?, updated_at = ? WHERE id = ? AND user_id = ?",
            (title, normalize_model(model), now, session_id, user_id),
        )
    connection.commit()


def list_sessions(connection: sqlite3.Connection, user_id: int) -> list[dict]:
    rows = connection.execute(
        """
        SELECT id, user_id, title, model, created_at, updated_at
        FROM sessions
        WHERE user_id = ?
        ORDER BY updated_at DESC, created_at DESC
        """,
        (user_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def get_messages(connection: sqlite3.Connection, session_id: str, user_id: int) -> list[dict]:
    rows = connection.execute(
        """
        SELECT messages.id, messages.session_id, messages.role, messages.content, messages.created_at
        FROM messages
        JOIN sessions ON sessions.id = messages.session_id
        WHERE messages.session_id = ? AND sessions.user_id = ?
        ORDER BY messages.id ASC
        """,
        (session_id, user_id),
    ).fetchall()
    return [dict(row) for row in rows]


def get_session(connection: sqlite3.Connection, session_id: str, user_id: int) -> dict | None:
    session = connection.execute(
        """
        SELECT id, user_id, title, model, created_at, updated_at
        FROM sessions
        WHERE id = ? AND user_id = ?
        """,
        (session_id, user_id),
    ).fetchone()
    return dict(session) if session is not None else None


def get_message(connection: sqlite3.Connection, session_id: str, user_id: int, message_id: int) -> dict | None:
    row = connection.execute(
        """
        SELECT messages.id, messages.session_id, messages.role, messages.content, messages.created_at
        FROM messages
        JOIN sessions ON sessions.id = messages.session_id
        WHERE messages.session_id = ? AND sessions.user_id = ? AND messages.id = ?
        """,
        (session_id, user_id, message_id),
    ).fetchone()
    return dict(row) if row is not None else None


def delete_messages_from(connection: sqlite3.Connection, session_id: str, user_id: int, message_id: int) -> None:
    now = utc_now()
    connection.execute(
        """
        DELETE FROM messages
        WHERE session_id = ?
          AND id >= ?
          AND session_id IN (SELECT id FROM sessions WHERE id = ? AND user_id = ?)
        """,
        (session_id, message_id, session_id, user_id),
    )
    connection.execute(
        "UPDATE sessions SET updated_at = ? WHERE id = ? AND user_id = ?",
        (now, session_id, user_id),
    )
    connection.commit()


def delete_session_record(connection: sqlite3.Connection, session_id: str, user_id: int) -> None:
    connection.execute(
        """
        DELETE FROM messages
        WHERE session_id = ?
          AND session_id IN (SELECT id FROM sessions WHERE id = ? AND user_id = ?)
        """,
        (session_id, session_id, user_id),
    )
    connection.execute("DELETE FROM sessions WHERE id = ? AND user_id = ?", (session_id, user_id))
    connection.commit()


def trim_title(title: str) -> str:
    compact = " ".join(title.strip().split())
    return compact[:200] or "New Chat"


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


def auth_payload(user: dict | None) -> dict:
    if user is None:
        return {"authenticated": False, "user": None, "is_admin": False}
    return {
        "authenticated": True,
        "is_admin": user["role"] == ROLE_SUPERADMIN,
        "user": user,
    }


def list_users(connection: sqlite3.Connection) -> list[dict]:
    rows = connection.execute(
        """
        SELECT id, username, email, role, is_active, created_at
        FROM users
        ORDER BY role DESC, id ASC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def send_and_persist_reply(
    connection: sqlite3.Connection,
    session_id: str,
    user_id: int,
    message_text: str,
    model: str,
    replace_from_message_id: int | None = None,
) -> tuple[dict, list[dict], str]:
    if not session_exists(connection, session_id, user_id):
        create_session_record(connection, session_id, user_id, model)

    current_session = get_session(connection, session_id, user_id)
    removed_suffix: list[dict] = []
    if replace_from_message_id is not None:
        target_message = get_message(connection, session_id, user_id, replace_from_message_id)
        if target_message is None:
            raise HTTPException(status_code=404, detail="Message not found")
        removed_suffix = [
            item for item in get_messages(connection, session_id, user_id) if item["id"] >= replace_from_message_id
        ]
        delete_messages_from(connection, session_id, user_id, replace_from_message_id)

    history_before = get_messages(connection, session_id, user_id)
    add_message(connection, session_id, "user", message_text)

    should_autobuild_title = not history_before and (current_session is None or current_session["title"] == "New Chat")
    title = build_title(message_text) if should_autobuild_title else None
    request_messages = build_request_messages([*history_before, {"role": "user", "content": message_text}])

    try:
        response = send_chat(
            messages=request_messages,
            model=model,
            session_id=session_id,
        )
    except Exception as exc:
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
    update_session_metadata(connection, session_id, user_id, response["model"], title=title)
    session = get_session(connection, session_id, user_id)
    messages = get_messages(connection, session_id, user_id)
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
            "default_admin_accounts": [
                {"username": "admin", "password": "admin123456"},
                {"username": "root", "password": "admin123456"},
            ],
        },
    )


@app.get("/api/models")
def api_models() -> dict:
    return {
        "models": sorted(SUPPORTED_MODELS),
        "default_model": DEFAULT_MODEL,
    }


@app.get("/api/auth/me")
def api_auth_me(user_session: str | None = Cookie(default=None)) -> dict:
    with closing(get_connection()) as connection:
        user = get_current_user(connection, user_session)
        return auth_payload(user)


@app.post("/api/auth/register-verify")
def api_register_verify(payload: RegisterVerifyRequest) -> dict:
    email = payload.email.strip().lower()
    code = generate_verify_code()
    with closing(get_connection()) as connection:
        upsert_verify_code(connection, email, VERIFY_PURPOSE_REGISTER, code)

    ok = send_email(
        email,
        "注册验证码",
        f"<p>你的注册验证码是：<strong>{code}</strong></p><p>{VERIFY_CODE_TTL_MINUTES}分钟内有效。</p>",
    )
    if not ok:
        with closing(get_connection()) as connection:
            delete_verify_code(connection, email, VERIFY_PURPOSE_REGISTER)
        raise HTTPException(status_code=500, detail="验证码发送失败，请先完成 SMTP 配置")
    return {"message": "验证码已发送"}


@app.post("/api/auth/register", status_code=201)
def api_register(payload: RegisterRequest) -> dict:
    username = payload.username.strip()
    email = payload.email.strip().lower()
    password = payload.password.strip()
    confirm_password = payload.confirmPassword.strip()
    verify_code = payload.verifyCode.strip()

    if password != confirm_password:
        raise HTTPException(status_code=400, detail="两次密码不一致")

    with closing(get_connection()) as connection:
        stored = get_verify_code_record(connection, email, VERIFY_PURPOSE_REGISTER)
    current_utc = datetime.utcnow()
    if not stored:
        raise HTTPException(status_code=400, detail="请先获取验证码")
    if datetime.fromisoformat(stored["expires_at"].replace("Z", "+00:00")).replace(tzinfo=None) <= current_utc:
        with closing(get_connection()) as connection:
            delete_verify_code(connection, email, VERIFY_PURPOSE_REGISTER)
        raise HTTPException(status_code=400, detail="验证码已过期")
    if stored["code"] != verify_code:
        raise HTTPException(status_code=400, detail="验证码错误")

    with closing(get_connection()) as connection:
        exists = connection.execute(
            "SELECT id FROM users WHERE username = ? OR email = ?",
            (username, email),
        ).fetchone()
        if exists:
            raise HTTPException(status_code=409, detail="用户名或邮箱已存在")

        salt = secrets.token_hex(16)
        connection.execute(
            """
            INSERT INTO users (username, email, role, password_hash, salt, is_active, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                username,
                email,
                ROLE_USER,
                hash_password(password, salt),
                salt,
                1,
                now_local_text(),
            ),
        )
        connection.commit()

    with closing(get_connection()) as connection:
        delete_verify_code(connection, email, VERIFY_PURPOSE_REGISTER)
    return {"message": "注册成功", "username": username}


@app.post("/api/auth/login")
def api_login(payload: LoginRequest, response: Response) -> dict:
    username = payload.username.strip()
    password = payload.password.strip()

    with closing(get_connection()) as connection:
        row = connection.execute(
            """
            SELECT id, username, email, role, password_hash, salt, is_active
            FROM users
            WHERE username = ? OR email = ?
            LIMIT 1
            """,
            (username, username),
        ).fetchone()

        if row is None or hash_password(password, row["salt"]) != row["password_hash"]:
            raise HTTPException(status_code=401, detail="账号或密码错误")
        if row["is_active"] != 1:
            raise HTTPException(status_code=403, detail="账号已禁用")

        token = create_auth_session(connection, row["id"])

    response.set_cookie(
        key=USER_SESSION_COOKIE,
        value=token,
        httponly=True,
        max_age=USER_SESSION_TTL_SECONDS,
        path="/",
        samesite="lax",
    )
    return {
        "message": "登录成功",
        "user": {
            "id": row["id"],
            "username": row["username"],
            "email": row["email"],
            "role": normalize_role(row["role"]),
        },
    }


@app.post("/api/auth/logout")
def api_logout(response: Response, user_session: str | None = Cookie(default=None)) -> dict:
    with closing(get_connection()) as connection:
        delete_auth_session(connection, user_session)
    response.delete_cookie(key=USER_SESSION_COOKIE, path="/")
    return {"message": "已退出登录"}


@app.post("/api/auth/verify")
def api_reset_verify(payload: ResetVerifyRequest) -> dict:
    email = payload.email.strip().lower()

    with closing(get_connection()) as connection:
        user = connection.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
        if user is None:
            raise HTTPException(status_code=400, detail="该邮箱未注册")

    code = generate_verify_code()
    with closing(get_connection()) as connection:
        upsert_verify_code(connection, email, VERIFY_PURPOSE_RESET, code)

    ok = send_email(
        email,
        "找回密码验证码",
        f"<p>你的找回密码验证码是：<strong>{code}</strong></p><p>{VERIFY_CODE_TTL_MINUTES}分钟内有效。</p>",
    )
    if not ok:
        with closing(get_connection()) as connection:
            delete_verify_code(connection, email, VERIFY_PURPOSE_RESET)
        raise HTTPException(status_code=500, detail="验证码发送失败，请先完成 SMTP 配置")
    return {"message": "验证码已发送"}


@app.post("/api/auth/reset")
def api_reset_password(payload: ResetPasswordRequest) -> dict:
    email = payload.email.strip().lower()
    verify_code = payload.verifyCode.strip()
    password = payload.password.strip()
    confirm_password = payload.confirmPassword.strip()

    if password != confirm_password:
        raise HTTPException(status_code=400, detail="两次密码不一致")

    with closing(get_connection()) as connection:
        stored = get_verify_code_record(connection, email, VERIFY_PURPOSE_RESET)
    current_utc = datetime.utcnow()
    if not stored:
        raise HTTPException(status_code=400, detail="请先获取验证码")
    if datetime.fromisoformat(stored["expires_at"].replace("Z", "+00:00")).replace(tzinfo=None) <= current_utc:
        with closing(get_connection()) as connection:
            delete_verify_code(connection, email, VERIFY_PURPOSE_RESET)
        raise HTTPException(status_code=400, detail="验证码已过期")
    if stored["code"] != verify_code:
        raise HTTPException(status_code=400, detail="验证码错误")

    salt = secrets.token_hex(16)
    with closing(get_connection()) as connection:
        connection.execute(
            "UPDATE users SET password_hash = ?, salt = ? WHERE email = ?",
            (hash_password(password, salt), salt, email),
        )
        connection.commit()

    with closing(get_connection()) as connection:
        delete_verify_code(connection, email, VERIFY_PURPOSE_RESET)
    return {"message": "密码重置成功"}


@app.get("/api/admin/users")
def api_admin_list_users(user_session: str | None = Cookie(default=None)) -> dict:
    with closing(get_connection()) as connection:
        require_superadmin(connection, user_session)
        return {"users": list_users(connection)}


@app.patch("/api/admin/users/{user_id}")
def api_admin_update_user_status(
    user_id: int,
    payload: UpdateUserStatusRequest,
    user_session: str | None = Cookie(default=None),
) -> dict:
    with closing(get_connection()) as connection:
        admin = require_superadmin(connection, user_session)
        target = connection.execute(
            """
            SELECT id, username, email, role, is_active, created_at
            FROM users
            WHERE id = ?
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()
        if target is None:
            raise HTTPException(status_code=404, detail="用户不存在")
        if target["id"] == admin["id"] and not payload.is_active:
            raise HTTPException(status_code=400, detail="不能禁用当前登录的管理员")

        connection.execute(
            "UPDATE users SET is_active = ? WHERE id = ?",
            (1 if payload.is_active else 0, user_id),
        )
        if not payload.is_active:
            connection.execute("DELETE FROM auth_sessions WHERE user_id = ?", (user_id,))
        connection.commit()

        updated = connection.execute(
            """
            SELECT id, username, email, role, is_active, created_at
            FROM users
            WHERE id = ?
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()
        return {"user": dict(updated)}


@app.get("/api/sessions")
def api_list_sessions(user_session: str | None = Cookie(default=None)) -> list[dict]:
    with closing(get_connection()) as connection:
        user = require_user(connection, user_session)
        return list_sessions(connection, user["id"])


@app.post("/api/sessions")
def api_create_session(payload: CreateSessionRequest, user_session: str | None = Cookie(default=None)) -> dict:
    session_id = str(uuid.uuid4())
    with closing(get_connection()) as connection:
        user = require_user(connection, user_session)
        create_session_record(connection, session_id, user["id"], payload.model)
        session = get_session(connection, session_id, user["id"])
    return dict(session)


@app.get("/api/sessions/{session_id}")
def api_get_session(session_id: str, user_session: str | None = Cookie(default=None)) -> dict:
    with closing(get_connection()) as connection:
        user = require_user(connection, user_session)
        session = get_session(connection, session_id, user["id"])
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        return {
            "session": session,
            "messages": get_messages(connection, session_id, user["id"]),
        }


@app.patch("/api/sessions/{session_id}")
def api_update_session(
    session_id: str,
    payload: UpdateSessionRequest,
    user_session: str | None = Cookie(default=None),
) -> dict:
    with closing(get_connection()) as connection:
        user = require_user(connection, user_session)
        session = get_session(connection, session_id, user["id"])
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        update_session_metadata(connection, session_id, user["id"], session["model"], title=trim_title(payload.title))
        session = get_session(connection, session_id, user["id"])
    return {"session": session}


@app.delete("/api/sessions/{session_id}")
def api_delete_session(session_id: str, user_session: str | None = Cookie(default=None)) -> dict:
    with closing(get_connection()) as connection:
        user = require_user(connection, user_session)
        if not session_exists(connection, session_id, user["id"]):
            raise HTTPException(status_code=404, detail="Session not found")
        delete_session_record(connection, session_id, user["id"])
    return {"ok": True}


@app.delete("/api/sessions/{session_id}/messages/{message_id}")
def api_delete_message(session_id: str, message_id: int, user_session: str | None = Cookie(default=None)) -> dict:
    with closing(get_connection()) as connection:
        user = require_user(connection, user_session)
        session = get_session(connection, session_id, user["id"])
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        message = get_message(connection, session_id, user["id"], message_id)
        if message is None:
            raise HTTPException(status_code=404, detail="Message not found")

        delete_messages_from(connection, session_id, user["id"], message_id)
        remaining_messages = get_messages(connection, session_id, user["id"])

        if not remaining_messages:
            title = "New Chat"
        else:
            first_user = next((item for item in remaining_messages if item["role"] == "user"), None)
            title = session["title"] if session["title"] != "New Chat" else build_title(first_user["content"]) if first_user else "New Chat"

        update_session_metadata(connection, session_id, user["id"], session["model"], title=title)
        updated_session = get_session(connection, session_id, user["id"])

    return {
        "session": updated_session,
        "messages": remaining_messages,
    }


@app.post("/api/sessions/{session_id}/messages/{message_id}/resend")
def api_resend_message(
    session_id: str,
    message_id: int,
    payload: EditMessageRequest,
    user_session: str | None = Cookie(default=None),
) -> dict:
    message_text = payload.message.strip()
    if not message_text:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    with closing(get_connection()) as connection:
        user = require_user(connection, user_session)
        target_message = get_message(connection, session_id, user["id"], message_id)
        if target_message is None:
            raise HTTPException(status_code=404, detail="Message not found")
        if target_message["role"] != "user":
            raise HTTPException(status_code=400, detail="Only user messages can be edited and resent")

        session, messages, assistant_text = send_and_persist_reply(
            connection=connection,
            session_id=session_id,
            user_id=user["id"],
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
def api_chat(payload: ChatRequest, user_session: str | None = Cookie(default=None)) -> dict:
    message_text = payload.message.strip()
    if not message_text:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    with closing(get_connection()) as connection:
        user = require_user(connection, user_session)
        session, messages, assistant_text = send_and_persist_reply(
            connection=connection,
            session_id=payload.session_id,
            user_id=user["id"],
            message_text=message_text,
            model=payload.model,
        )

    return {
        "session": session,
        "messages": messages,
        "reply": assistant_text,
    }


@app.post("/api/chat/stream")
def api_chat_stream(payload: ChatRequest, user_session: str | None = Cookie(default=None)) -> StreamingResponse:
    message_text = payload.message.strip()
    if not message_text:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    with closing(get_connection()) as connection:
        user = require_user(connection, user_session)
        if not session_exists(connection, payload.session_id, user["id"]):
            create_session_record(connection, payload.session_id, user["id"], payload.model)

        current_session = get_session(connection, payload.session_id, user["id"])
        history_before = get_messages(connection, payload.session_id, user["id"])
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
        request_messages = build_request_messages([*history_before, {"role": "user", "content": message_text}])
        session = get_session(connection, payload.session_id, user["id"])

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
                update_session_metadata(connection, payload.session_id, user["id"], response_model, title=title)
                latest_session = get_session(connection, payload.session_id, user["id"])

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
