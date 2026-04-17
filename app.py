import base64
import binascii
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
from typing import Any

import httpx
from fastapi import Cookie, FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, EmailStr, Field

from uuapi_client import (
    DEFAULT_MODEL,
    SUPPORTED_IMAGE_MEDIA_TYPES,
    SUPPORTED_MODELS,
    iter_stream_chat,
    normalize_model,
    send_chat,
)


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "chat_app.db"
DEFAULT_KEY_SOURCE_URL = "https://github.com/weicengxing/freeclaude/blob/main/key.txt"
logger = logging.getLogger(__name__)

USER_SESSION_COOKIE = "user_session"
USER_SESSION_TTL_SECONDS = 60 * 60 * 24 * 7
ROLE_USER = "User"
ROLE_SUPERADMIN = "SuperAdmin"
VALID_ROLES = {ROLE_USER, ROLE_SUPERADMIN}
VERIFY_CODE_TTL_MINUTES = 10
VERIFY_PURPOSE_REGISTER = "register"
VERIFY_PURPOSE_RESET = "reset"
DEFAULT_USER_KEY_BATCH_SIZE = 5
LEGACY_FIXED_USER_KEY_BATCH_SIZE = 5
MAX_IMAGE_BYTES = 5 * 1024 * 1024
STRUCTURED_MESSAGE_VERSION = 1


def get_env_int(name: str, default: int, minimum: int = 1) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or not raw_value.strip():
        return default
    try:
        value = int(raw_value.strip())
    except ValueError as exc:
        raise RuntimeError(f"Environment variable {name} must be an integer") from exc
    if value < minimum:
        raise RuntimeError(f"Environment variable {name} must be >= {minimum}")
    return value


USER_KEY_BATCH_SIZE = get_env_int("USER_KEY_BATCH_SIZE", DEFAULT_USER_KEY_BATCH_SIZE)

app = FastAPI(title="UUAPI Web Chat")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


class CreateSessionRequest(BaseModel):
    model: str = DEFAULT_MODEL


class ImagePayload(BaseModel):
    media_type: str
    data: str = Field(min_length=1)
    name: str | None = None


class ChatRequest(BaseModel):
    session_id: str
    message: str = ""
    image: ImagePayload | None = None
    images: list[ImagePayload] = Field(default_factory=list)
    model: str = DEFAULT_MODEL
    replace_from_message_id: int | None = None


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


def rollback_if_needed(connection: sqlite3.Connection) -> None:
    if connection.in_transaction:
        connection.rollback()


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


def model_to_dict(model: BaseModel | None) -> dict[str, Any] | None:
    if model is None:
        return None
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def normalize_image_payload(image: ImagePayload | dict[str, Any] | None) -> dict[str, str] | None:
    if image is None:
        return None

    raw = model_to_dict(image) if isinstance(image, BaseModel) else image
    if raw is None:
        return None

    media_type = str(raw.get("media_type", "")).strip().lower()
    data = str(raw.get("data", "")).strip()
    name = str(raw.get("name", "")).strip()

    if media_type not in SUPPORTED_IMAGE_MEDIA_TYPES:
        raise HTTPException(status_code=400, detail="Unsupported image format")
    if not data:
        raise HTTPException(status_code=400, detail="Image data cannot be empty")

    try:
        image_bytes = base64.b64decode(data, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Image data must be valid base64") from exc

    if not image_bytes:
        raise HTTPException(status_code=400, detail="Image data cannot be empty")
    if len(image_bytes) > MAX_IMAGE_BYTES:
        raise HTTPException(status_code=400, detail=f"Image too large; max {MAX_IMAGE_BYTES // (1024 * 1024)} MB")

    normalized = {
        "media_type": media_type,
        "data": base64.b64encode(image_bytes).decode("ascii"),
    }
    if name:
        normalized["name"] = name[:255]
    return normalized


def normalize_image_payloads(images: list[ImagePayload | dict[str, Any]] | None) -> list[dict[str, str]]:
    normalized_images: list[dict[str, str]] = []
    for image in images or []:
        normalized_image = normalize_image_payload(image)
        if normalized_image is not None:
            normalized_images.append(normalized_image)
    return normalized_images


def normalize_request_images(
    image: ImagePayload | dict[str, Any] | None,
    images: list[ImagePayload | dict[str, Any]] | None,
) -> list[dict[str, str]]:
    if images:
        return normalize_image_payloads(images)

    normalized_image = normalize_image_payload(image)
    return [normalized_image] if normalized_image is not None else []


def assign_message_images(target: dict[str, Any], images: list[dict[str, str]]) -> dict[str, Any]:
    if images:
        target["images"] = images
        target["image"] = images[0]
    return target


def parse_stored_message_content(raw_content: str) -> dict[str, Any]:
    if raw_content:
        stripped = raw_content.lstrip()
        if stripped.startswith("{"):
            try:
                payload = json.loads(raw_content)
            except json.JSONDecodeError:
                payload = None
            if isinstance(payload, dict) and payload.get("v") == STRUCTURED_MESSAGE_VERSION:
                text = str(payload.get("text", "") or "")
                images: list[dict[str, str]] = []
                try:
                    if isinstance(payload.get("images"), list):
                        images = normalize_image_payloads(payload.get("images"))
                    elif isinstance(payload.get("image"), dict):
                        legacy_image = normalize_image_payload(payload.get("image"))
                        if legacy_image is not None:
                            images = [legacy_image]
                except HTTPException:
                    images = []
                return assign_message_images({"content": text}, images)
    return {"content": raw_content}


def serialize_message_content(content: str, images: list[dict[str, str]] | None = None) -> str:
    if not images:
        return content
    payload: dict[str, Any] = {
        "v": STRUCTURED_MESSAGE_VERSION,
        "text": content,
    }
    if len(images) == 1:
        payload["image"] = images[0]
    else:
        payload["images"] = images
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def build_title_for_message(message_text: str, images: list[dict[str, Any]] | None = None) -> str:
    compact = " ".join(message_text.split()).strip()
    if compact:
        return build_title(compact)
    if images:
        return "图片消息"
    return "New Chat"


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

            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_key TEXT NOT NULL,
                source_url TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_api_key
            ON api_keys(api_key);

            CREATE TABLE IF NOT EXISTS api_key_allocator_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                next_batch_start_id INTEGER,
                updated_at TEXT NOT NULL
            );
            """
        )
        if not table_has_column(connection, "sessions", "user_id"):
            connection.execute("ALTER TABLE sessions ADD COLUMN user_id INTEGER")
        if not table_has_column(connection, "users", "api_key_batch_start_id"):
            connection.execute("ALTER TABLE users ADD COLUMN api_key_batch_start_id INTEGER")
        if not table_has_column(connection, "users", "current_api_key_id"):
            connection.execute("ALTER TABLE users ADD COLUMN current_api_key_id INTEGER")
        if not table_has_column(connection, "users", "api_key_batch_size"):
            connection.execute("ALTER TABLE users ADD COLUMN api_key_batch_size INTEGER")
        connection.execute(
            """
            UPDATE users
            SET api_key_batch_size = ?
            WHERE api_key_batch_size IS NULL
              AND api_key_batch_start_id IS NOT NULL
              AND current_api_key_id IS NOT NULL
            """,
            (LEGACY_FIXED_USER_KEY_BATCH_SIZE,),
        )
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


def add_message(
    connection: sqlite3.Connection,
    session_id: str,
    role: str,
    content: str,
    images: list[dict[str, str]] | None = None,
) -> None:
    now = utc_now()
    connection.execute(
        """
        INSERT INTO messages (session_id, role, content, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (session_id, role, serialize_message_content(content, images), now),
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
    messages: list[dict] = []
    for row in rows:
        item = dict(row)
        parsed = parse_stored_message_content(item["content"])
        item["content"] = parsed["content"]
        assign_message_images(item, parsed.get("images") or [])
        messages.append(item)
    return messages


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
    if row is None:
        return None
    item = dict(row)
    parsed = parse_stored_message_content(item["content"])
    item["content"] = parsed["content"]
    assign_message_images(item, parsed.get("images") or [])
    return item


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


def get_api_key_id_bounds(connection: sqlite3.Connection) -> tuple[int | None, int | None]:
    row = connection.execute(
        "SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM api_keys"
    ).fetchone()
    if row is None or row["min_id"] is None or row["max_id"] is None:
        return None, None
    return int(row["min_id"]), int(row["max_id"])


def get_api_key_count(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COUNT(*) AS count FROM api_keys").fetchone()
    return int(row["count"]) if row is not None else 0


def get_api_key_record_by_id(connection: sqlite3.Connection, key_id: int | None) -> dict | None:
    if key_id is None:
        return None
    row = connection.execute(
        """
        SELECT id, api_key, source_url, created_at
        FROM api_keys
        WHERE id = ?
        LIMIT 1
        """,
        (key_id,),
    ).fetchone()
    return dict(row) if row is not None else None


def get_user_key_state(connection: sqlite3.Connection, user_id: int) -> dict | None:
    row = connection.execute(
        """
        SELECT id, api_key_batch_start_id, current_api_key_id, api_key_batch_size
        FROM users
        WHERE id = ?
        LIMIT 1
        """,
        (user_id,),
    ).fetchone()
    return dict(row) if row is not None else None


def get_effective_user_batch_size(user_state: dict | None) -> int | None:
    if user_state is None:
        return None
    batch_size = user_state.get("api_key_batch_size")
    if batch_size is None:
        return None
    return int(batch_size)


def init_allocator_state_row(connection: sqlite3.Connection) -> None:
    row = connection.execute(
        "SELECT singleton FROM api_key_allocator_state WHERE singleton = 1"
    ).fetchone()
    if row is None:
        connection.execute(
            """
            INSERT INTO api_key_allocator_state (singleton, next_batch_start_id, updated_at)
            VALUES (1, NULL, ?)
            """,
            (utc_now(),),
        )


def allocate_key_batch_locked(connection: sqlite3.Connection, user_id: int) -> dict:
    min_key_id, _ = get_api_key_id_bounds(connection)
    if min_key_id is None:
        raise HTTPException(status_code=503, detail="当前没有可用的 API Key")

    try:
        connection.execute("BEGIN IMMEDIATE")
        init_allocator_state_row(connection)
        state = connection.execute(
            """
            SELECT next_batch_start_id
            FROM api_key_allocator_state
            WHERE singleton = 1
            LIMIT 1
            """
        ).fetchone()

        batch_start_id = min_key_id
        if state is not None and state["next_batch_start_id"] is not None:
            batch_start_id = max(int(state["next_batch_start_id"]), min_key_id)

        allocated_batch_size = USER_KEY_BATCH_SIZE
        batch_end_id = batch_start_id + allocated_batch_size - 1
        available_count = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM api_keys
            WHERE id BETWEEN ? AND ?
            """,
            (batch_start_id, batch_end_id),
        ).fetchone()
        if available_count is None or int(available_count["count"]) != allocated_batch_size:
            raise HTTPException(
                status_code=503,
                detail=f"可分配的连续 API Key 不足 {allocated_batch_size} 个",
            )

        now = utc_now()
        connection.execute(
            """
            UPDATE api_key_allocator_state
            SET next_batch_start_id = ?, updated_at = ?
            WHERE singleton = 1
            """,
            (batch_end_id + 1, now),
        )
        connection.execute(
            """
            UPDATE users
            SET api_key_batch_start_id = ?, current_api_key_id = ?, api_key_batch_size = ?
            WHERE id = ?
            """,
            (batch_start_id, batch_start_id, allocated_batch_size, user_id),
        )
        connection.commit()
    except Exception:
        rollback_if_needed(connection)
        raise

    key_record = get_api_key_record_by_id(connection, batch_start_id)
    if key_record is None:
        raise HTTPException(status_code=503, detail="分配的 API Key 不存在")
    return key_record


def ensure_user_api_key(connection: sqlite3.Connection, user_id: int) -> dict:
    user_state = get_user_key_state(connection, user_id)
    current_key_id = None if user_state is None else user_state["current_api_key_id"]
    key_record = get_api_key_record_by_id(connection, current_key_id)
    if key_record is not None:
        return key_record
    return allocate_key_batch_locked(connection, user_id)


def advance_user_api_key(connection: sqlite3.Connection, user_id: int, exhausted_key_id: int | None) -> dict:
    try:
        connection.execute("BEGIN IMMEDIATE")
        user_state = get_user_key_state(connection, user_id)
        if user_state is None:
            raise HTTPException(status_code=404, detail="用户不存在")

        current_key_id = user_state["current_api_key_id"]
        batch_start_id = user_state["api_key_batch_start_id"]
        batch_size = get_effective_user_batch_size(user_state)

        if current_key_id is None or batch_start_id is None or batch_size is None:
            rollback_if_needed(connection)
            return allocate_key_batch_locked(connection, user_id)

        if exhausted_key_id is not None and current_key_id != exhausted_key_id:
            connection.commit()
            key_record = get_api_key_record_by_id(connection, current_key_id)
            if key_record is None:
                return allocate_key_batch_locked(connection, user_id)
            return key_record

        batch_end_id = int(batch_start_id) + batch_size - 1
        if int(current_key_id) < batch_end_id:
            next_key_id = int(current_key_id) + 1
            key_record = get_api_key_record_by_id(connection, next_key_id)
            if key_record is None:
                raise HTTPException(status_code=503, detail="下一个 API Key 不存在")
            connection.execute(
                "UPDATE users SET current_api_key_id = ? WHERE id = ?",
                (next_key_id, user_id),
            )
            connection.commit()
            return key_record

        connection.commit()
        return allocate_key_batch_locked(connection, user_id)
    except Exception:
        rollback_if_needed(connection)
        raise


def is_api_key_quota_error(exc: Exception) -> bool:
    if isinstance(exc, HTTPException):
        return False

    message_parts = [str(exc)]
    if isinstance(exc, httpx.HTTPStatusError):
        message_parts.append(exc.response.text)

    normalized = " ".join(part.lower() for part in message_parts if part)
    keywords = (
        "quota",
        "余额",
        "额度",
        "insufficient",
        "credit",
        "rate limit",
        "api key has been disabled",
        "invalid x-api-key",
        "unauthorized",
        "forbidden",
    )
    return any(keyword in normalized for keyword in keywords)


def get_exception_detail(exc: Exception) -> str:
    if isinstance(exc, HTTPException):
        return str(exc.detail)
    return str(exc)


def send_chat_with_user_api_key(
    connection: sqlite3.Connection,
    user_id: int,
    messages: list[dict[str, Any]],
    model: str,
    session_id: str,
) -> dict:
    retry_limit = max(1, min(get_api_key_count(connection), 50))
    attempted_key_ids: set[int] = set()
    last_exc: Exception | None = None

    for _ in range(retry_limit):
        key_record = ensure_user_api_key(connection, user_id)
        key_id = int(key_record["id"])
        if key_id in attempted_key_ids:
            break
        attempted_key_ids.add(key_id)

        try:
            return send_chat(
                messages=messages,
                model=model,
                session_id=session_id,
                api_key=key_record["api_key"],
            )
        except Exception as exc:
            last_exc = exc
            if not is_api_key_quota_error(exc):
                raise
            advance_user_api_key(connection, user_id, key_id)

    if last_exc is not None:
        raise HTTPException(status_code=503, detail=f"所有可切换的 API Key 都不可用: {last_exc}") from last_exc
    raise HTTPException(status_code=503, detail="当前没有可用的 API Key")


def build_request_messages(history: list[dict]) -> list[dict[str, Any]]:
    request_messages: list[dict[str, Any]] = []
    for item in history:
        message: dict[str, Any] = {
            "role": item["role"],
            "content": item.get("content", ""),
        }
        images = item.get("images")
        if isinstance(images, list) and images:
            message["images"] = images
        elif item.get("image") is not None:
            message["images"] = [item["image"]]
        request_messages.append(message)
    return request_messages


def restore_messages(connection: sqlite3.Connection, messages: list[dict]) -> None:
    for item in messages:
        connection.execute(
            """
            INSERT INTO messages (id, session_id, role, content, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                item["id"],
                item["session_id"],
                item["role"],
                serialize_message_content(item.get("content", ""), item.get("images") or []),
                item["created_at"],
            ),
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
    images: list[dict[str, str]],
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
    add_message(connection, session_id, "user", message_text, images=images)

    should_autobuild_title = not history_before and (current_session is None or current_session["title"] == "New Chat")
    title = build_title_for_message(message_text, images) if should_autobuild_title else None
    request_messages = build_request_messages([*history_before, {"role": "user", "content": message_text, "images": images}])

    try:
        response = send_chat_with_user_api_key(
            connection=connection,
            user_id=user_id,
            messages=request_messages,
            model=model,
            session_id=session_id,
        )
    except HTTPException:
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
        raise
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


@app.get("/api/keys")
def api_keys_status(user_session: str | None = Cookie(default=None)) -> dict:
    with closing(get_connection()) as connection:
        require_superadmin(connection, user_session)
        total_keys = connection.execute("SELECT COUNT(*) AS count FROM api_keys").fetchone()["count"]
    return {
        "source_url": DEFAULT_KEY_SOURCE_URL,
        "total_keys": total_keys,
    }


@app.post("/api/keys/import")
def api_import_keys(user_session: str | None = Cookie(default=None)) -> dict:
    with closing(get_connection()) as connection:
        require_superadmin(connection, user_session)
        return import_api_keys_from_url(connection, DEFAULT_KEY_SOURCE_URL)


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
        user_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.commit()
        try:
            ensure_user_api_key(connection, user_id)
        except HTTPException:
            logger.warning("register user %s without preallocated api key batch", user_id)

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
            title = (
                session["title"]
                if session["title"] != "New Chat"
                else build_title_for_message(first_user["content"], first_user.get("images") or []) if first_user else "New Chat"
            )

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
        if target_message.get("images"):
            raise HTTPException(status_code=400, detail="Image messages cannot be edited and resent yet")

        session, messages, assistant_text = send_and_persist_reply(
            connection=connection,
            session_id=session_id,
            user_id=user["id"],
            message_text=message_text,
            images=[],
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
    images = normalize_request_images(payload.image, payload.images)
    if not message_text and not images:
        raise HTTPException(status_code=400, detail="Message or image is required")

    with closing(get_connection()) as connection:
        user = require_user(connection, user_session)
        session, messages, assistant_text = send_and_persist_reply(
            connection=connection,
            session_id=payload.session_id,
            user_id=user["id"],
            message_text=message_text,
            images=images,
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
    images = normalize_request_images(payload.image, payload.images)
    if not message_text and not images:
        raise HTTPException(status_code=400, detail="Message or image is required")

    with closing(get_connection()) as connection:
        user = require_user(connection, user_session)
        removed_suffix: list[dict] = []
        replace_from_message_id = payload.replace_from_message_id

        if replace_from_message_id is not None:
            target_message = get_message(connection, payload.session_id, user["id"], replace_from_message_id)
            if target_message is None:
                raise HTTPException(status_code=404, detail="Message not found")
            if target_message["role"] != "user":
                raise HTTPException(status_code=400, detail="Only user messages can be edited and resent")
            if target_message.get("images"):
                raise HTTPException(status_code=400, detail="Image messages cannot be edited and resent yet")
            removed_suffix = [
                item for item in get_messages(connection, payload.session_id, user["id"]) if item["id"] >= replace_from_message_id
            ]
            delete_messages_from(connection, payload.session_id, user["id"], replace_from_message_id)
        elif not session_exists(connection, payload.session_id, user["id"]):
            create_session_record(connection, payload.session_id, user["id"], payload.model)

        current_session = get_session(connection, payload.session_id, user["id"])
        history_before = get_messages(connection, payload.session_id, user["id"])
        add_message(connection, payload.session_id, "user", message_text, images=images)
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
        title = build_title_for_message(message_text, images) if should_autobuild_title else None
        request_messages = build_request_messages([*history_before, {"role": "user", "content": message_text, "images": images}])
        session = get_session(connection, payload.session_id, user["id"])
        retry_limit = max(1, min(get_api_key_count(connection), 50))

    def event_stream():
        assistant_parts: list[str] = []
        response_model = normalize_model(payload.model)

        try:
            yield sse_event(
                "start",
                {
                    "session": session,
                    "message": assign_message_images({"role": "user", "content": message_text}, images),
                },
            )

            attempted_key_ids: set[int] = set()
            last_exc: Exception | None = None
            stream_completed = False
            for _ in range(retry_limit):
                with closing(get_connection()) as key_connection:
                    key_record = ensure_user_api_key(key_connection, user["id"])

                key_id = int(key_record["id"])
                if key_id in attempted_key_ids:
                    break
                attempted_key_ids.add(key_id)

                emitted_text = False
                try:
                    for chunk in iter_stream_chat(
                        messages=request_messages,
                        model=payload.model,
                        session_id=payload.session_id,
                        api_key=key_record["api_key"],
                    ):
                        response_model = chunk.get("data", {}).get("model", response_model)
                        text_delta = extract_stream_text(chunk)
                        if text_delta:
                            emitted_text = True
                            assistant_parts.append(text_delta)
                            yield sse_event("delta", {"text": text_delta})
                    stream_completed = True
                    break
                except Exception as exc:
                    last_exc = exc
                    if emitted_text or not is_api_key_quota_error(exc):
                        raise
                    with closing(get_connection()) as rotate_connection:
                        advance_user_api_key(rotate_connection, user["id"], key_id)

            if not stream_completed:
                if last_exc is not None:
                    raise HTTPException(status_code=503, detail=f"所有可切换的 API Key 都不可用: {last_exc}") from last_exc
                raise HTTPException(status_code=503, detail="当前没有可用的 API Key")

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
                if removed_suffix:
                    restore_messages(rollback_connection, removed_suffix)
            yield sse_event("error", {"detail": get_exception_detail(exc)})

    return StreamingResponse(event_stream(), media_type="text/event-stream")
