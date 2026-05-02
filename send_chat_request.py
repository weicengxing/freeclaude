import base64
import json
import os
import socket
import ssl
import struct
import threading
import time
import uuid
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError


CONFIG_PATH = "chat_profiles.json"


def load_config(path: str = CONFIG_PATH) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


CONFIG = load_config()
PROFILES = CONFIG.get("profiles", [])
if not PROFILES:
    raise RuntimeError("chat_profiles.json must contain at least one profile")

_profile_cursor = 0
_profile_cursor_lock = threading.Lock()
_ws_url_cache: dict[str, dict[str, Any]] = {}
_ws_url_cache_lock = threading.Lock()
_ws_refresh_stop = threading.Event()


def select_profile() -> dict[str, Any]:
    global _profile_cursor

    strategy = CONFIG.get("selection_strategy", "active")
    if strategy == "round_robin":
        with _profile_cursor_lock:
            profile = PROFILES[_profile_cursor % len(PROFILES)]
            _profile_cursor += 1
            return profile

    active_name = CONFIG.get("active_profile")
    for profile in PROFILES:
        if profile.get("name") == active_name:
            return profile

    return PROFILES[0]


def profile_name(profile: dict[str, Any]) -> str:
    return profile.get("name") or profile.get("account_id") or "unnamed"

QUESTION = CONFIG.get("question", "你好")
CONVERSATION_ID = CONFIG.get("conversation_id") or None
PARENT_MESSAGE_ID = CONFIG.get("parent_message_id") or None
START_NEW_CONVERSATION = bool(CONFIG.get("start_new_conversation", True))
NEW_CONVERSATION_PARENT_ID = CONFIG.get("new_conversation_parent_id", "client-created-root")
MODEL = CONFIG.get("model", "gpt-5-5-thinking")
DEFAULT_MODEL_ALIASES = {
    "codex-mini-latest": "gpt-5-4-mini",
    "gpt-5.2": "gpt-5-2",
    "gpt-5.3": "gpt-5-3",
    "gpt-5.4": "gpt-5-4",
    "gpt-5.4-mini": "gpt-5-4-mini",
    "gpt-5.5": "gpt-5-5-thinking",
    "gpt-5.5-thinking": "gpt-5-5-thinking",
    "gpt-5.5-pro": "gpt-5-5-pro",
}
MODEL_ALIASES = {**DEFAULT_MODEL_ALIASES, **CONFIG.get("model_aliases", {})}
AUTO_REFRESH_WS_URL = bool(CONFIG.get("auto_refresh_ws_url", True))
WS_REFRESH_INTERVAL_SEC = int(CONFIG.get("ws_refresh_interval_sec", 120))
WS_REFRESH_STAGGER_SEC = int(CONFIG.get("ws_refresh_stagger_sec", 15))
WS_SEND_AUTH_HEADERS = bool(CONFIG.get("ws_send_auth_headers", False))
PRINT_RAW_TOPIC_MESSAGES = bool(CONFIG.get("print_raw_topic_messages", False))
PRINT_STREAMING_TEXT = bool(CONFIG.get("print_streaming_text", False))
WS_SUBSCRIBE_WITH_OFFSET = bool(CONFIG.get("ws_subscribe_with_offset", False))

WS_COMMAND_START_ID = 1
_ws_command_next_id = WS_COMMAND_START_ID


def usable(value: str) -> bool:
    return bool(value and value.strip() and not value.startswith("PASTE_"))


def validate_config() -> None:
    if CONFIG.get("selection_strategy", "active") == "round_robin":
        for profile in PROFILES:
            validate_profile(profile)
        return

    validate_profile(select_profile())


def validate_profile(profile: dict[str, Any]) -> None:
    required = {
        "bearer_token": profile.get("bearer_token", ""),
        "account_id": profile.get("account_id", ""),
        "conduit_token": profile.get("conduit_token", ""),
        "cookie": profile.get("cookie", ""),
        "oai_device_id": profile.get("oai_device_id", ""),
        "oai_session_id": profile.get("oai_session_id", ""),
    }
    missing = [name for name, value in required.items() if not usable(value)]
    if missing:
        joined = ", ".join(missing)
        raise RuntimeError(f"Profile {profile_name(profile)} missing required values: {joined}")


def clean_bearer_token(token: str) -> str:
    token = token.strip()
    if token.lower().startswith("bearer "):
        return token[7:].strip()
    return token


def resolve_model(requested_model: str | None = None) -> str:
    if not requested_model:
        return MODEL

    requested_model = requested_model.strip()
    if not requested_model:
        return MODEL

    if requested_model in MODEL_ALIASES:
        return MODEL_ALIASES[requested_model]

    normalized = requested_model.replace(".", "-")
    if normalized in MODEL_ALIASES:
        return MODEL_ALIASES[normalized]
    if normalized.startswith("gpt-"):
        return normalized
    return MODEL


def build_body(
    question: str,
    parent_message_id: str,
    conversation_id: str | None,
    model: str | None = None,
) -> bytes:
    upstream_model = resolve_model(model)
    payload = {
        "action": "next",
        "messages": [
            {
                "id": str(uuid.uuid4()),
                "author": {"role": "user"},
                "create_time": time.time(),
                "content": {
                    "content_type": "text",
                    "parts": [question],
                },
                "metadata": {
                    "developer_mode_connector_ids": [],
                    "selected_connector_ids": [],
                    "selected_sync_knowledge_store_ids": [],
                    "selected_sources": [],
                    "selected_github_repos": [],
                    "selected_all_github_repos": False,
                    "serialization_metadata": {"custom_symbol_offsets": []},
                },
            }
        ],
        "parent_message_id": parent_message_id,
        "model": upstream_model,
        "timezone_offset_min": -480,
        "timezone": "Asia/Shanghai",
        "conversation_mode": {"kind": "primary_assistant"},
        "enable_message_followups": True,
        "system_hints": [],
        "supports_buffering": True,
        "supported_encodings": ["v1"],
        "client_contextual_info": {
            "is_dark_mode": False,
            "time_since_loaded": 1,
            "page_height": 712,
            "page_width": 971,
            "pixel_ratio": 1.25,
            "screen_height": 864,
            "screen_width": 1536,
            "app_name": "chat.sharedchat.cc",
        },
        "paragen_cot_summary_display_override": "allow",
        "force_parallel_switch": "auto",
    }
    thinking_effort = CONFIG.get("thinking_effort")
    if thinking_effort:
        payload["thinking_effort"] = thinking_effort
    elif "thinking" in upstream_model or upstream_model.endswith("-pro"):
        payload["thinking_effort"] = "extended"
    if conversation_id:
        payload["conversation_id"] = conversation_id

    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def build_headers(
    conversation_id: str | None = CONVERSATION_ID,
    profile: dict[str, Any] | None = None,
) -> dict[str, str]:
    profile = profile or PROFILES[0]
    referer = "https://chat.sharedchat.cc/"
    if conversation_id:
        referer = f"https://chat.sharedchat.cc/c/{conversation_id}"

    headers = {
        "accept": "text/event-stream",
        "authorization": f"Bearer {clean_bearer_token(profile.get('bearer_token', ''))}",
        "chatgpt-account-id": profile.get("account_id", ""),
        "content-type": "application/json",
        "oai-language": "zh-CN",
        "origin": "https://chat.sharedchat.cc",
        "referer": referer,
        "x-conduit-token": profile.get("conduit_token", ""),
        "x-oai-turn-trace-id": str(uuid.uuid4()),
        "x-openai-target-path": "/backend-api/f/conversation",
        "x-openai-target-route": "/backend-api/f/conversation",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/147.0.0.0 Safari/537.36"
        ),
    }

    if usable(profile.get("sentinel_token", "")):
        headers["openai-sentinel-chat-requirements-token"] = profile["sentinel_token"]
    if usable(profile.get("cookie", "")):
        headers["cookie"] = profile["cookie"]
    if usable(profile.get("oai_device_id", "")):
        headers["oai-device-id"] = profile["oai_device_id"]
    if usable(profile.get("oai_session_id", "")):
        headers["oai-session-id"] = profile["oai_session_id"]

    return headers


def build_ws_url_headers(
    conversation_id: str | None = CONVERSATION_ID,
    profile: dict[str, Any] | None = None,
) -> dict[str, str]:
    profile = profile or PROFILES[0]
    referer = "https://chat.sharedchat.cc/"
    if conversation_id:
        referer = f"https://chat.sharedchat.cc/c/{conversation_id}"

    headers = {
        "accept": "*/*",
        "chatgpt-account-id": profile.get("account_id", ""),
        "oai-client-build-number": "5561002",
        "oai-client-version": "prod-8bd5c4ba133b610a0563c545f5e81318b3890627",
        "oai-language": "zh-CN",
        "origin": "https://chat.sharedchat.cc",
        "referer": referer,
        "x-openai-target-path": "/backend-api/celsius/ws/user",
        "x-openai-target-route": "/backend-api/celsius/ws/user",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/147.0.0.0 Safari/537.36"
        ),
    }

    if usable(profile.get("oai_device_id", "")):
        headers["oai-device-id"] = profile["oai_device_id"]
    if usable(profile.get("oai_session_id", "")):
        headers["oai-session-id"] = profile["oai_session_id"]
    if usable(profile.get("cookie", "")):
        headers["cookie"] = profile["cookie"]
    if usable(profile.get("bearer_token", "")):
        headers["authorization"] = f"Bearer {clean_bearer_token(profile['bearer_token'])}"

    return headers


def fetch_websocket_url(
    conversation_id: str | None = CONVERSATION_ID,
    profile: dict[str, Any] | None = None,
) -> str:
    url = "https://chat.sharedchat.cc/backend-api/celsius/ws/user"
    req = request.Request(url, headers=build_ws_url_headers(conversation_id, profile), method="GET")

    with request.urlopen(req, timeout=30) as res:
        body = res.read().decode("utf-8", errors="replace")

    data = json.loads(body)
    websocket_url = data.get("websocket_url")
    if not isinstance(websocket_url, str) or not websocket_url.startswith(("ws://", "wss://")):
        raise RuntimeError(f"Unexpected websocket_url response: {body}")

    return websocket_url


def refresh_ws_url_for_profile(profile: dict[str, Any], conversation_id: str | None = None) -> str:
    websocket_url = fetch_websocket_url(conversation_id, profile)
    name = profile_name(profile)
    with _ws_url_cache_lock:
        _ws_url_cache[name] = {
            "url": websocket_url,
            "refreshed_at": time.time(),
        }
    return websocket_url


def get_cached_ws_url(profile: dict[str, Any], conversation_id: str | None = None) -> str:
    name = profile_name(profile)
    now = time.time()
    with _ws_url_cache_lock:
        cached = _ws_url_cache.get(name)
        if cached and now - cached["refreshed_at"] < WS_REFRESH_INTERVAL_SEC:
            return cached["url"]

    return refresh_ws_url_for_profile(profile, conversation_id)


def start_ws_refresh_workers(conversation_id: str | None = None) -> list[threading.Thread]:
    if not AUTO_REFRESH_WS_URL:
        return []

    threads = []
    for index, profile in enumerate(PROFILES):
        delay = index * WS_REFRESH_STAGGER_SEC
        thread = threading.Thread(
            target=ws_refresh_worker,
            args=(profile, conversation_id, delay),
            daemon=True,
            name=f"ws-refresh-{profile_name(profile)}",
        )
        thread.start()
        threads.append(thread)
    return threads


def ws_refresh_worker(profile: dict[str, Any], conversation_id: str | None, initial_delay: int) -> None:
    if initial_delay > 0:
        _ws_refresh_stop.wait(initial_delay)

    while not _ws_refresh_stop.is_set():
        try:
            refresh_ws_url_for_profile(profile, conversation_id)
            print(f"[ws-refresh] refreshed {profile_name(profile)}")
        except Exception as exc:
            print(f"[ws-refresh] {profile_name(profile)} failed: {exc}")

        _ws_refresh_stop.wait(WS_REFRESH_INTERVAL_SEC)


def parse_sse_data_line(line: str) -> Any | None:
    if not line.startswith("data:"):
        return None

    data = line.removeprefix("data:").strip()
    if data == "[DONE]":
        return {"type": "done"}

    try:
        return json.loads(data)
    except json.JSONDecodeError:
        return data


def send_question(
    question: str,
    parent_message_id: str,
    conversation_id: str | None,
    model: str | None = None,
    profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = profile or PROFILES[0]
    url = "https://chat.sharedchat.cc/backend-api/f/conversation"
    req = request.Request(
        url,
        data=build_body(question, parent_message_id, conversation_id, model),
        headers=build_headers(conversation_id, profile),
        method="POST",
    )

    result: dict[str, Any] = {
        "conversation_id": None,
        "resume_token": None,
        "turn_exchange_id": None,
        "topic_id": None,
        "options": [],
    }

    print(f"Sending question: {question}")
    print()

    with request.urlopen(req, timeout=120) as res:
        print(f"POST status: {res.status} {res.reason}")

        for raw_line in res:
            line = raw_line.decode("utf-8", errors="replace").rstrip()
            parsed = parse_sse_data_line(line)
            if parsed is None:
                continue

            if parsed == {"type": "done"}:
                print("POST stream done.")
                break

            if isinstance(parsed, str):
                print(f"SSE data: {parsed}")
                continue

            event_type = parsed.get("type")
            if event_type == "resume_conversation_token":
                result["resume_token"] = parsed.get("token")
                result["conversation_id"] = parsed.get("conversation_id")
                print("Received resume_conversation_token.")
            elif event_type == "stream_handoff":
                result["conversation_id"] = parsed.get("conversation_id")
                result["turn_exchange_id"] = parsed.get("turn_exchange_id")
                result["options"] = parsed.get("options", [])
                result["topic_id"] = find_topic_id(result["options"])
                print(f"Received stream_handoff topic_id: {result['topic_id']}")
            else:
                print("SSE JSON:", json.dumps(parsed, ensure_ascii=False))

    return result


def find_topic_id(options: list[dict[str, Any]]) -> str | None:
    for option in options:
        topic_id = option.get("topic_id")
        if topic_id:
            return topic_id
    return None


def render_template(value: Any, context: dict[str, str]) -> Any:
    if isinstance(value, str):
        for key, replacement in context.items():
            value = value.replace("{" + key + "}", replacement)
        return value
    if isinstance(value, list):
        return [render_template(item, context) for item in value]
    if isinstance(value, dict):
        return {key: render_template(item, context) for key, item in value.items()}
    return value


def next_ws_command_id() -> int:
    global _ws_command_next_id
    current = _ws_command_next_id
    _ws_command_next_id += 1
    return current


def build_ws_subscribe_frame(topic_id: str, include_connect: bool) -> list[dict[str, Any]]:
    target_subscribe_command = {
        "type": "subscribe",
        "topic_id": "{topic_id}",
    }
    if WS_SUBSCRIBE_WITH_OFFSET:
        target_subscribe_command["offset"] = "{offset}"

    template = []
    if include_connect:
        template.extend([
            {
                "id": next_ws_command_id(),
                "command": {
                    "type": "connect",
                    "presence": {
                        "type": "presence",
                        "state": "background",
                    },
                },
            },
            {
                "id": next_ws_command_id(),
                "command": {
                    "type": "subscribe",
                    "topic_id": "conversations",
                },
            },
            {
                "id": next_ws_command_id(),
                "command": {
                    "type": "subscribe",
                    "topic_id": "app_notifications",
                },
            },
        ])

    template.append(
        {
            "id": next_ws_command_id(),
            "command": target_subscribe_command,
        },
    )

    offset = f"{int(time.time() * 1000)}-0"
    return render_template(template, {"topic_id": topic_id, "offset": offset})


class SimpleWebSocket:
    def __init__(self, url: str, headers: dict[str, str]):
        self.url = url
        self.headers = headers
        self.sock: ssl.SSLSocket | socket.socket | None = None

    def __enter__(self) -> "SimpleWebSocket":
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        if self.sock:
            self.sock.close()

    def connect(self) -> None:
        parsed = parse.urlparse(self.url)
        host = parsed.hostname
        if not host:
            raise ValueError("WS_URL is missing a hostname")

        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query

        raw_sock = socket.create_connection((host, port), timeout=30)
        if parsed.scheme == "wss":
            sock = ssl.create_default_context().wrap_socket(raw_sock, server_hostname=host)
        else:
            sock = raw_sock

        key = base64.b64encode(os.urandom(16)).decode("ascii")
        handshake_headers = {
            "Host": host,
            "Upgrade": "websocket",
            "Connection": "Upgrade",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "Sec-WebSocket-Key": key,
            "Sec-WebSocket-Version": "13",
            "Origin": "https://chat.sharedchat.cc",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "User-Agent": self.headers.get("user-agent", ""),
        }
        if WS_SEND_AUTH_HEADERS and usable(self.headers.get("cookie", "")):
            handshake_headers["Cookie"] = self.headers["cookie"]
        if WS_SEND_AUTH_HEADERS and usable(self.headers.get("authorization", "")):
            handshake_headers["Authorization"] = self.headers["authorization"]

        request_lines = [f"GET {path} HTTP/1.1"]
        request_lines.extend(f"{name}: {value}" for name, value in handshake_headers.items() if value)
        request_lines.extend(["", ""])
        sock.sendall("\r\n".join(request_lines).encode("utf-8"))

        response = b""
        while b"\r\n\r\n" not in response:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk

        response_text = response.decode("utf-8", errors="replace")
        status_line = response_text.split("\r\n", 1)[0]
        if " 101 " not in status_line:
            raise RuntimeError(f"WebSocket handshake failed: {status_line}\n{response_text}")

        self.sock = sock
        print(f"WebSocket connected: {status_line}")

    def send_text(self, text: str) -> None:
        if not self.sock:
            raise RuntimeError("WebSocket is not connected")

        payload = text.encode("utf-8")
        frame = bytearray([0x81])
        length = len(payload)
        if length < 126:
            frame.append(0x80 | length)
        elif length < 65536:
            frame.append(0x80 | 126)
            frame.extend(struct.pack("!H", length))
        else:
            frame.append(0x80 | 127)
            frame.extend(struct.pack("!Q", length))

        mask = os.urandom(4)
        frame.extend(mask)
        frame.extend(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        self.sock.sendall(frame)

    def read_text(self, timeout: int = 180) -> str | None:
        if not self.sock:
            raise RuntimeError("WebSocket is not connected")

        self.sock.settimeout(timeout)
        message = bytearray()
        expecting_continuation = False

        while True:
            frame = self._read_frame()
            if frame is None:
                return None

            fin, opcode, payload = frame

            if opcode == 0x8:
                return None
            if opcode == 0x9:
                self._send_control_frame(0xA, payload)
                continue
            if opcode == 0xA:
                continue

            if opcode == 0x1:
                message.extend(payload)
                expecting_continuation = not fin
            elif opcode == 0x0 and expecting_continuation:
                message.extend(payload)
                expecting_continuation = not fin
            else:
                continue

            if not expecting_continuation:
                return bytes(message).decode("utf-8", errors="replace")

    def _read_frame(self) -> tuple[bool, int, bytes] | None:
        first = self._recv_exact_or_none(2)
        if not first:
            return None

        fin = bool(first[0] & 0x80)
        opcode = first[0] & 0x0F
        masked = bool(first[1] & 0x80)
        length = first[1] & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._recv_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._recv_exact(8))[0]

        mask = self._recv_exact(4) if masked else b""
        payload = self._recv_exact(length)
        if masked:
            payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))

        return fin, opcode, payload

    def _recv_exact_or_none(self, length: int) -> bytes | None:
        if not self.sock:
            raise RuntimeError("WebSocket is not connected")

        chunks = bytearray()
        while len(chunks) < length:
            chunk = self.sock.recv(length - len(chunks))
            if not chunk:
                return None
            chunks.extend(chunk)
        return bytes(chunks)

    def _recv_exact(self, length: int) -> bytes:
        if not self.sock:
            raise RuntimeError("WebSocket is not connected")

        chunks = bytearray()
        while len(chunks) < length:
            chunk = self.sock.recv(length - len(chunks))
            if not chunk:
                raise ConnectionError("WebSocket closed while reading a frame")
            chunks.extend(chunk)
        return bytes(chunks)

    def _send_control_frame(self, opcode: int, payload: bytes) -> None:
        if not self.sock:
            raise RuntimeError("WebSocket is not connected")

        mask = os.urandom(4)
        frame = bytearray([0x80 | opcode, 0x80 | len(payload)])
        frame.extend(mask)
        frame.extend(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        self.sock.sendall(frame)


def subscribe_and_print(topic_id: str, profile: dict[str, Any] | None = None) -> dict[str, Any] | None:
    profile = profile or PROFILES[0]
    ws_url = get_runtime_ws_url(profile)
    if not ws_url:
        print()
        print("No WS_URL set and AUTO_REFRESH_WS_URL is disabled.")
        print(f"Target topic_id: {topic_id}")
        return None

    headers = build_headers(profile=profile)
    with SimpleWebSocket(ws_url, headers) as ws:
        result = subscribe_topic_and_collect(ws, topic_id, include_connect=True)
        return result


def get_runtime_ws_url(profile: dict[str, Any] | None = None) -> str:
    profile = profile or PROFILES[0]
    if AUTO_REFRESH_WS_URL:
        ws_url = get_cached_ws_url(profile, None if START_NEW_CONVERSATION else CONVERSATION_ID)
        return ws_url

    return profile.get("ws_url", "")


def subscribe_topic_and_collect(ws: SimpleWebSocket, topic_id: str, include_connect: bool) -> dict[str, Any]:
    frame = build_ws_subscribe_frame(topic_id, include_connect=include_connect)
    text = json.dumps(frame, ensure_ascii=False, separators=(",", ":"))
    print(f"Sending WS command frame: {text}")
    ws.send_text(text)

    print()
    print(f"Listening for topic_id: {topic_id}")
    answer = ""
    assistant_message_id = None
    started_at = time.perf_counter()

    while True:
        text = ws.read_text()
        if text is None:
            print("WebSocket closed.")
            break
        if not text:
            continue

        matched = topic_id in text
        parsed = None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            pass

        if matched:
            result = handle_topic_message(parsed if parsed is not None else text, answer)
            answer = result["answer"]
            assistant_message_id = result.get("assistant_message_id") or assistant_message_id
            if result["done"]:
                return {
                    "answer": answer,
                    "assistant_message_id": assistant_message_id,
                    "elapsed_sec": time.perf_counter() - started_at,
                }
        elif parsed is not None:
            print_ws_status(parsed)

    return {
        "answer": answer,
        "assistant_message_id": assistant_message_id,
        "elapsed_sec": time.perf_counter() - started_at,
    }


def subscribe_topic_stream(ws: SimpleWebSocket, topic_id: str, include_connect: bool):
    frame = build_ws_subscribe_frame(topic_id, include_connect=include_connect)
    text = json.dumps(frame, ensure_ascii=False, separators=(",", ":"))
    print(f"Sending WS command frame: {text}")
    ws.send_text(text)

    print()
    print(f"Streaming topic_id: {topic_id}")
    answer = ""
    assistant_message_id = None
    started_at = time.perf_counter()

    while True:
        text = ws.read_text()
        if text is None:
            break
        if not text:
            continue

        matched = topic_id in text
        parsed = None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            pass

        if matched:
            result = handle_topic_message(parsed if parsed is not None else text, answer)
            updated_answer = result["answer"]
            assistant_message_id = result.get("assistant_message_id") or assistant_message_id

            if updated_answer != answer:
                if updated_answer.startswith(answer):
                    delta = updated_answer[len(answer):]
                else:
                    delta = updated_answer
                answer = updated_answer
                if delta:
                    yield {
                        "type": "delta",
                        "delta": delta,
                        "answer": answer,
                        "assistant_message_id": assistant_message_id,
                    }

            if result["done"]:
                yield {
                    "type": "done",
                    "answer": answer,
                    "assistant_message_id": assistant_message_id,
                    "elapsed_sec": time.perf_counter() - started_at,
                }
                return
        elif parsed is not None:
            print_ws_status(parsed)

    yield {
        "type": "done",
        "answer": answer,
        "assistant_message_id": assistant_message_id,
        "elapsed_sec": time.perf_counter() - started_at,
    }


def print_ws_status(message: Any) -> None:
    if not isinstance(message, list):
        return

    for item in message:
        if not isinstance(item, dict):
            continue

        item_type = item.get("type")
        topic_id = item.get("topic_id")
        if item_type == "message" and topic_id in {"conversations", "app_notifications"}:
            payload_type = item.get("payload", {}).get("type")
            print(f"WS {topic_id}: {payload_type}")
        elif "id" in item:
            print("WS command response:", json.dumps(item, ensure_ascii=False))


def handle_topic_message(message: Any, answer: str) -> dict[str, Any]:
    if PRINT_RAW_TOPIC_MESSAGES:
        print()
        print("WS topic message:")
        print(json.dumps(message, ensure_ascii=False, indent=2) if not isinstance(message, str) else message)

    done = False
    assistant_message_id = None
    for encoded_item in extract_encoded_items(message):
        for event in parse_encoded_sse(encoded_item):
            data = event.get("data")

            if data == "[DONE]":
                done = True
                continue

            if not isinstance(data, dict):
                continue

            assistant_message_id = extract_assistant_message_id(data) or assistant_message_id

            if data.get("type") == "message_stream_complete":
                done = True
                continue

            updated_answer = apply_delta_to_answer(answer, data)
            if updated_answer != answer:
                if PRINT_STREAMING_TEXT and updated_answer.startswith(answer):
                    print(updated_answer[len(answer):], end="", flush=True)
                answer = updated_answer

            if data.get("type") == "done":
                done = True

    if is_ws_done_message(message):
        done = True

    return {"answer": answer, "done": done, "assistant_message_id": assistant_message_id}


def extract_encoded_items(message: Any) -> list[str]:
    items: list[str] = []

    def walk(node: Any, key: str | None = None) -> None:
        if key == "encoded_item" and isinstance(node, str):
            items.append(node)
            return
        if isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            for child_key, child_value in node.items():
                walk(child_value, child_key)

    walk(message)
    return items


def parse_encoded_sse(encoded_item: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    current_event = "message"
    data_lines: list[str] = []

    def flush() -> None:
        nonlocal current_event, data_lines
        if not data_lines:
            current_event = "message"
            return

        data_text = "\n".join(data_lines)
        if data_text == "[DONE]":
            data: Any = "[DONE]"
        else:
            try:
                data = json.loads(data_text)
            except json.JSONDecodeError:
                data = data_text

        events.append({"event": current_event, "data": data})
        current_event = "message"
        data_lines = []

    for raw_line in encoded_item.splitlines():
        line = raw_line.strip()
        if not line:
            flush()
            continue
        if line.startswith("event:"):
            current_event = line.removeprefix("event:").strip()
        elif line.startswith("data:"):
            data_lines.append(line.removeprefix("data:").strip())

    flush()
    return events


def apply_delta_to_answer(answer: str, delta: dict[str, Any]) -> str:
    full_text = extract_assistant_message_text(delta)
    if full_text is not None:
        return merge_full_text(answer, full_text)

    if is_patch_operation(delta) and delta.get("o") != "patch":
        answer = apply_patch_operation(answer, delta)

    value = delta.get("v")
    if isinstance(value, str) and not is_patch_operation(delta):
        return answer + value

    if isinstance(value, list):
        for patch in value:
            if isinstance(patch, dict) and is_patch_operation(patch):
                answer = apply_patch_operation(answer, patch)

    return answer


def merge_full_text(answer: str, full_text: str) -> str:
    if not full_text:
        return answer
    if not answer:
        return full_text
    if full_text == answer or answer.endswith(full_text):
        return answer
    if full_text.startswith(answer):
        return full_text
    return answer + "\n\n" + full_text


def extract_assistant_message_text(delta: dict[str, Any]) -> str | None:
    candidates = []
    if isinstance(delta.get("message"), dict):
        candidates.append(delta["message"])

    value = delta.get("v")
    if isinstance(value, dict) and isinstance(value.get("message"), dict):
        candidates.append(value["message"])

    for message in candidates:
        author = message.get("author")
        if isinstance(author, dict) and author.get("role") != "assistant":
            continue

        content = message.get("content")
        if not isinstance(content, dict):
            continue

        parts = content.get("parts")
        if isinstance(parts, list) and parts and isinstance(parts[0], str):
            return parts[0]

    return None


def extract_assistant_message_id(delta: dict[str, Any]) -> str | None:
    candidates = []
    if isinstance(delta.get("message"), dict):
        candidates.append(delta["message"])

    value = delta.get("v")
    if isinstance(value, dict) and isinstance(value.get("message"), dict):
        candidates.append(value["message"])

    for message in candidates:
        author = message.get("author")
        if isinstance(author, dict) and author.get("role") != "assistant":
            continue

        message_id = message.get("id")
        if isinstance(message_id, str) and message_id:
            return message_id

    return None


def is_patch_operation(value: dict[str, Any]) -> bool:
    return isinstance(value.get("p"), str) and isinstance(value.get("o"), str)


def apply_patch_operation(answer: str, operation: dict[str, Any]) -> str:
    path = operation.get("p")
    op = operation.get("o")
    value = operation.get("v")

    if path == "/message/content/parts/0":
        if op == "append" and isinstance(value, str):
            return answer + value
        if op in {"replace", "add"} and isinstance(value, str):
            return merge_full_text(answer, value)

    if path == "/message/content/parts":
        if op in {"replace", "add"} and isinstance(value, list) and value and isinstance(value[0], str):
            return merge_full_text(answer, value[0])

    if path == "/message/content":
        if op in {"replace", "add"} and isinstance(value, dict):
            parts = value.get("parts")
            if isinstance(parts, list) and parts and isinstance(parts[0], str):
                return merge_full_text(answer, parts[0])

    if path == "/message" and op in {"replace", "add"} and isinstance(value, dict):
        text = extract_assistant_message_text({"message": value})
        if text is not None:
            return merge_full_text(answer, text)

    if path == "" and op in {"replace", "add"} and isinstance(value, dict):
        text = extract_assistant_message_text(value)
        if text is not None:
            return merge_full_text(answer, text)

    return answer


def is_ws_done_message(message: Any) -> bool:
    if not isinstance(message, list):
        return False

    for item in message:
        if not isinstance(item, dict):
            continue
        payload = item.get("payload")
        if not isinstance(payload, dict):
            continue
        nested = payload.get("payload")
        if isinstance(nested, dict) and nested.get("type") == "done":
            return True

    return False


def ask_ai(
    question: str,
    profile: dict[str, Any] | None = None,
    model: str | None = None,
) -> dict[str, Any]:
    profile = profile or select_profile()

    validate_profile(profile)
    print(f"Using profile: {profile_name(profile)}")

    conversation_id = None if START_NEW_CONVERSATION else CONVERSATION_ID
    parent_message_id = NEW_CONVERSATION_PARENT_ID if START_NEW_CONVERSATION else PARENT_MESSAGE_ID
    handoff = send_question(question, parent_message_id, conversation_id, model, profile)

    topic_id = handoff.get("topic_id")
    if not topic_id:
        raise RuntimeError(f"No topic_id found in stream_handoff: {json.dumps(handoff, ensure_ascii=False)}")

    result = subscribe_and_print(topic_id, profile)
    if result is None:
        raise RuntimeError("No WebSocket URL available")

    result["profile"] = profile_name(profile)
    result["conversation_id"] = handoff.get("conversation_id")
    result["topic_id"] = topic_id
    return result


def stream_ai(
    question: str,
    profile: dict[str, Any] | None = None,
    model: str | None = None,
):
    profile = profile or select_profile()

    validate_profile(profile)
    print(f"Using profile: {profile_name(profile)}")

    conversation_id = None if START_NEW_CONVERSATION else CONVERSATION_ID
    parent_message_id = NEW_CONVERSATION_PARENT_ID if START_NEW_CONVERSATION else PARENT_MESSAGE_ID
    handoff = send_question(question, parent_message_id, conversation_id, model, profile)

    topic_id = handoff.get("topic_id")
    if not topic_id:
        raise RuntimeError(f"No topic_id found in stream_handoff: {json.dumps(handoff, ensure_ascii=False)}")

    ws_url = get_runtime_ws_url(profile)
    if not ws_url:
        raise RuntimeError("No WebSocket URL available")

    headers = build_headers(profile=profile)
    with SimpleWebSocket(ws_url, headers) as ws:
        for item in subscribe_topic_stream(ws, topic_id, include_connect=True):
            item["profile"] = profile_name(profile)
            item["conversation_id"] = handoff.get("conversation_id")
            item["topic_id"] = topic_id
            yield item


def run_turn(question: str, profile: dict[str, Any]) -> None:
    try:
        result = ask_ai(question, profile)
        print()
        print("Final answer:")
        print(result["answer"])
    except HTTPError as exc:
        print(f"HTTP error: {exc.code} {exc.reason}")
        print(exc.read().decode("utf-8", errors="replace"))
    except URLError as exc:
        print(f"Request failed: {exc.reason}")
    except RuntimeError as exc:
        message = str(exc)
        print()
        print(message)
        if "401" in message and "WebSocket handshake failed" in message:
            print()
            print("The POST request succeeded, but the copied WS_URL was rejected.")
            print("Most likely the wss://...verify=... value is expired, one-time, or missing WS-specific cookies.")


def main() -> None:
    try:
        validate_config()
    except RuntimeError as exc:
        print(exc)
        print("Fill chat_profiles.json with valid profile values.")
        return

    refresh_threads = start_ws_refresh_workers(None if START_NEW_CONVERSATION else CONVERSATION_ID)
    if refresh_threads:
        print(f"Started {len(refresh_threads)} WebSocket URL refresh worker(s).")

    try:
        if not CONFIG.get("interactive_loop", False):
            run_turn(QUESTION, select_profile())
            return

        print("Interactive loop started. Type /exit to quit.")
        while True:
            question = input("\nYou> ").strip()
            if question in {"/exit", "exit", "quit", "q"}:
                break
            if not question:
                continue

            run_turn(question, select_profile())
    finally:
        _ws_refresh_stop.set()


if __name__ == "__main__":
    main()
