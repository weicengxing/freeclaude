import base64
import json
import os
import socket
import ssl
import struct
import time
import uuid
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError


QUESTION = "你好"

# Paste fresh values copied from the browser request.
BEARER_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6IjE5MzM0NGU2NS1iYmM5LTQ0ZDEtYTlkMC1mOTU3YjA3OWJkMGUiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOlsiaHR0cHM6Ly9hcGkub3BlbmFpLmNvbS92MSJdLCJhenAiOiJwZGxMSVgyWTcyTUlsMnJoTGhURTlWVjliTjkwNWtCaCIsImNsaWVudF9pZCI6ImFwcF9XWHJGMUxTa2lUdGZZcWlMNlh0anlndlgiLCJleHAiOjE3NjIxOTY5MzQsImh0dHBzOi8vYXBpIjp7Im9wZW5haSI6eyJjb20vcHJvZmlsZSI6eyJlbWFpbCI6ImJ1aXNhbnZvd2EwMDEifX19LCJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsicG9pZCI6Im9yZy1WTFp6RTJva2E3TjVRRkk3Z21HdXlXY1kiLCJ1c2VyX2lkIjoidXNlci13bEo1S2FFR2tpUUdiV0xwU0VVMkUxYlcifSwiaHR0cHM6Ly9hcGkub3BlbmFpLmNvbS9wcm9maWxlIjp7ImVtYWlsIjoia2lyc3RpbmNoYWR3aWNrMjU3MTk4MGtkZUBnbWFpbC5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZX0sImlhdCI6MTc2MTMzMjkzMywiaXNzIjoiaHR0cHM6Ly9hdXRoLm9wZW5haS5jb20iLCJqdGkiOiIxNmIxZjE4NS0xODljLTQ2ZmYtOGQzMi0wZmZhYWFlZTI0MTMiLCJuYmYiOjE3NjEzMzI5MzMsInB3ZF9hdXRoX3RpbWUiOjE3NTQwMzU3MDQsInNjcCI6WyJvcGVuaWQiLCJwcm9maWxlIiwiZW1haWwiLCJtb2RlbC5yZWFkIiwibW9kZWwucmVxdWVzdCIsIm9yZ2FuaXphdGlvbi5yZWFkIiwib3JnYW5pemF0aW9uLndyaXRlIiwib2ZmbGluZV9hY2Nlc3MiXSwic2Vzc2lvbl9pZCI6IjdybHZOejdlcHJiVXMxZFlHZ2drOW5sV29wSGVCOVAyIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDUyMDMyOTQyNDE1NDE4MDY2MDUifQ.BBpzpYUorm_RsFv-JNNWELMXhhta36kNLh5S_59TJZQRyjYKxoIQfzjk3wJN_1Vd-ZNPgQ9eRTPMkdtrgah1dqgbLGkgg2T4oyJq4z-1tTLYb50t6g-vj2MW-YcArqouwmYL8avCGe90A_bL9LcxEwzL1hLtODSMn94mbLPCi26VPUBj9xwQJdP3deY-e2GpvIwbysz_eH-hP2vHooFhE0jebrE0FE-cuzyHU5b2h6YbzrfU_hwiPXuvGiZwON9LxndprXYuup5wzyNZEnufVTwDagGX5pCyGLSVk9bSILUz6zJkYMtFEBPyyDx-bL9hH041uDfDBUxi15w3hGVMgA"
ACCOUNT_ID = "842059c9-144b-4058-89ca-21f0f2b78f14"
CONDUIT_TOKEN = "b8635660-b384-44ed-9381-e20cc8957d5f"
SENTINEL_TOKEN = "yyy"
COOKIE = r"""_account_is_fedramp=false; gfsessionid=1vuoiuz1d5zsxndi6l5lq8sndw2pz332; oai-gn=; _account_residency_region=no_constraint; oai-default-model-config=%7B%22model%22%3A%22gpt-5-3%22%2C%22juices%22%3A%7B%7D%7D; oai-last-model-config=%7B%22model%22%3A%22gpt-5-5-thinking%22%2C%22effort%22%3A%22extended%22%7D; oai-client-auth-info=%7B%22user%22%3A%7B%22name%22%3A%22eRFulTrH%7C%22%2C%22email%22%3A%22eRFulTrH%7C%22%2C%22picture%22%3A%22%2Favatars.png%22%2C%22connectionType%22%3A2%2C%22timestamp%22%3A1777633486823%7D%2C%22loggedInWithGoogleOneTap%22%3Afalse%2C%22isOptedOut%22%3Afalse%7D; _account=842059c9-144b-4058-89ca-21f0f2b78f14; _dd_s=aid=9ae44759-8265-4fb2-9f5f-10e3cc0b9731&logs=1&id=a4b0c21d-4f9f-4dca-bc40-f75cb655ac11&created=1777630828451&expire=1777635852195; _ga=GA1.1.798167618.1776071405; _ga_9SHBSK2D9J=GS2.1.s1777633236$o24$g1$t1777633506$j40$l0$h0"""
OAI_DEVICE_ID = "d66635a1-2331-499a-ad38-7f9803bfb09f"
OAI_SESSION_ID = "1f136a6e-011a-4f8f-a9b3-e6e9c511a1c1"

CONVERSATION_ID = "69f488d7-121c-8327-8b4c-c7ccf7e88cda"
PARENT_MESSAGE_ID = "b88270e9-242e-47c3-9839-728b0bd19e8d"
MODEL = "gpt-5-5-thinking"

# Optional: paste the full wss://... URL from DevTools if you want the script
# to try subscribing after /conversation returns stream_handoff.
WS_URL = "wss://ws.xyhelper.cn/p4/ws/user/user-GjEm6cTEWpp7t23kHqklyLtV__32ab0d2f-c05c-4a58-826c-7b4620337f17?verify=1777647717-tRhg8sc3OfolkHCRVedSiXyXwg9%252FPWUBo47TghxIMTo%253D"
WS_SEND_AUTH_HEADERS = False
PRINT_RAW_TOPIC_MESSAGES = False
PRINT_STREAMING_TEXT = False
WS_SUBSCRIBE_WITH_OFFSET = False

# Browser-captured protocol shape:
# [
#   {"id": 1, "command": {"type": "connect", ...}},
#   {"id": 2, "command": {"type": "subscribe", "topic_id": "conversations"}},
#   {"id": 3, "command": {"type": "subscribe", "topic_id": "app_notifications"}},
#   {"id": 4, "command": {"type": "subscribe", "topic_id": "{topic_id}", "offset": "{offset}"}}
# ]
WS_COMMAND_START_ID = 1


def usable(value: str) -> bool:
    return bool(value and value.strip() and not value.startswith("PASTE_"))


def validate_config() -> None:
    required = {
        "BEARER_TOKEN": BEARER_TOKEN,
        "ACCOUNT_ID": ACCOUNT_ID,
        "CONDUIT_TOKEN": CONDUIT_TOKEN,
        "COOKIE": COOKIE,
        "OAI_DEVICE_ID": OAI_DEVICE_ID,
        "OAI_SESSION_ID": OAI_SESSION_ID,
    }
    missing = [name for name, value in required.items() if not usable(value)]
    if missing:
        joined = ", ".join(missing)
        raise RuntimeError(f"Missing required config values: {joined}")


def clean_bearer_token(token: str) -> str:
    token = token.strip()
    if token.lower().startswith("bearer "):
        return token[7:].strip()
    return token


def build_body() -> bytes:
    payload = {
        "action": "next",
        "messages": [
            {
                "id": str(uuid.uuid4()),
                "author": {"role": "user"},
                "create_time": time.time(),
                "content": {
                    "content_type": "text",
                    "parts": [QUESTION],
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
        "conversation_id": CONVERSATION_ID,
        "parent_message_id": PARENT_MESSAGE_ID,
        "model": MODEL,
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
        "thinking_effort": "extended",
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def build_headers() -> dict[str, str]:
    headers = {
        "accept": "text/event-stream",
        "authorization": f"Bearer {clean_bearer_token(BEARER_TOKEN)}",
        "chatgpt-account-id": ACCOUNT_ID,
        "content-type": "application/json",
        "oai-language": "zh-CN",
        "origin": "https://chat.sharedchat.cc",
        "referer": f"https://chat.sharedchat.cc/c/{CONVERSATION_ID}",
        "x-conduit-token": CONDUIT_TOKEN,
        "x-oai-turn-trace-id": str(uuid.uuid4()),
        "x-openai-target-path": "/backend-api/f/conversation",
        "x-openai-target-route": "/backend-api/f/conversation",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/147.0.0.0 Safari/537.36"
        ),
    }

    if usable(SENTINEL_TOKEN):
        headers["openai-sentinel-chat-requirements-token"] = SENTINEL_TOKEN
    if usable(COOKIE):
        headers["cookie"] = COOKIE
    if usable(OAI_DEVICE_ID):
        headers["oai-device-id"] = OAI_DEVICE_ID
    if usable(OAI_SESSION_ID):
        headers["oai-session-id"] = OAI_SESSION_ID

    return headers


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


def send_question() -> dict[str, Any]:
    url = "https://chat.sharedchat.cc/backend-api/f/conversation"
    req = request.Request(url, data=build_body(), headers=build_headers(), method="POST")

    result: dict[str, Any] = {
        "conversation_id": None,
        "resume_token": None,
        "turn_exchange_id": None,
        "topic_id": None,
        "options": [],
    }

    print(f"Sending question: {QUESTION}")
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


def build_ws_subscribe_frame(topic_id: str) -> list[dict[str, Any]]:
    command_id = WS_COMMAND_START_ID

    def next_id() -> int:
        nonlocal command_id
        current = command_id
        command_id += 1
        return current

    target_subscribe_command = {
        "type": "subscribe",
        "topic_id": "{topic_id}",
    }
    if WS_SUBSCRIBE_WITH_OFFSET:
        target_subscribe_command["offset"] = "{offset}"

    template = [
        {
            "id": next_id(),
            "command": {
                "type": "connect",
                "presence": {
                    "type": "presence",
                    "state": "background",
                },
            },
        },
        {
            "id": next_id(),
            "command": {
                "type": "subscribe",
                "topic_id": "conversations",
            },
        },
        {
            "id": next_id(),
            "command": {
                "type": "subscribe",
                "topic_id": "app_notifications",
            },
        },
        {
            "id": next_id(),
            "command": target_subscribe_command,
        },
    ]

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
        if WS_SEND_AUTH_HEADERS and usable(COOKIE):
            handshake_headers["Cookie"] = COOKIE
        if WS_SEND_AUTH_HEADERS and usable(BEARER_TOKEN):
            handshake_headers["Authorization"] = f"Bearer {clean_bearer_token(BEARER_TOKEN)}"

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


def subscribe_and_print(topic_id: str) -> None:
    if not WS_URL:
        print()
        print("No WS_URL set. Paste the wss://... Request URL into WS_URL to auto-subscribe.")
        print(f"Target topic_id: {topic_id}")
        return

    headers = build_headers()
    with SimpleWebSocket(WS_URL, headers) as ws:
        frame = build_ws_subscribe_frame(topic_id)
        text = json.dumps(frame, ensure_ascii=False, separators=(",", ":"))
        print(f"Sending WS command frame: {text}")
        ws.send_text(text)

        print()
        print(f"Listening for topic_id: {topic_id}")
        answer = ""
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
                if result["done"]:
                    print()
                    print("Final answer:")
                    print(answer)
                    break
            elif parsed is not None:
                print_ws_status(parsed)


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
    for encoded_item in extract_encoded_items(message):
        for event in parse_encoded_sse(encoded_item):
            data = event.get("data")

            if data == "[DONE]":
                done = True
                continue

            if not isinstance(data, dict):
                continue

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

    return {"answer": answer, "done": done}


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


def main() -> None:
    try:
        validate_config()
    except RuntimeError as exc:
        print(exc)
        print("Fill these values at the top of send_chat_request.py from the latest browser request.")
        return

    try:
        handoff = send_question()
    except HTTPError as exc:
        print(f"HTTP error: {exc.code} {exc.reason}")
        print(exc.read().decode("utf-8", errors="replace"))
        return
    except URLError as exc:
        print(f"Request failed: {exc.reason}")
        return

    topic_id = handoff.get("topic_id")
    if not topic_id:
        print("No topic_id found in stream_handoff.")
        print(json.dumps(handoff, ensure_ascii=False, indent=2))
        return

    try:
        subscribe_and_print(topic_id)
    except RuntimeError as exc:
        message = str(exc)
        print()
        print(message)
        if "401" in message and "WebSocket handshake failed" in message:
            print()
            print("The POST request succeeded, but the copied WS_URL was rejected.")
            print("Most likely the wss://...verify=... value is expired, one-time, or missing WS-specific cookies.")
            print("Copy a fresh WebSocket Request URL from DevTools after reloading/logging in, then run again.")
            print(f"Topic to subscribe after POST: {topic_id}")


if __name__ == "__main__":
    main()
