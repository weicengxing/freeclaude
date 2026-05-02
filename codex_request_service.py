import json
import logging
import time
import threading
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from argparse import Namespace
from pathlib import Path
from typing import Any
from urllib import request
from urllib.error import HTTPError, URLError

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from send_codex_request import (
    CODEX_INSTRUCTIONS,
    auth_headers_from_data,
    build_codex_body,
    collect_text_from_response,
    make_request,
    response_url,
)


CONFIG_PATH = Path("codex_profiles.json")
CODEX_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
REFRESH_TOKEN_URL = "https://auth.openai.com/oauth/token"
TOKEN_REFRESH_INTERVAL = timedelta(days=8)

app = FastAPI(title="Codex Request Service")
_profile_cursor = 0
_profile_lock = threading.Lock()
_refresh_lock = threading.Lock()
_maintenance_thread: threading.Thread | None = None
_maintenance_stop = threading.Event()
_response_profiles: dict[str, str] = {}
logger = logging.getLogger("codex_request_service")


def load_service_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"Missing {path}. Copy codex_profiles.example.json or create codex_profiles.json.")
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def save_service_config(config: dict[str, Any], path: Path = CONFIG_PATH) -> None:
    with path.open("w", encoding="utf-8") as file:
        json.dump(config, file, ensure_ascii=False, indent=2)
        file.write("\n")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_refresh_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None

    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def should_skip_manual_refresh(profile: dict[str, Any], force: bool) -> bool:
    if force:
        return False

    last_refresh = parse_refresh_time(profile.get("last_refresh"))
    if last_refresh is None:
        return False

    return utc_now() - last_refresh < TOKEN_REFRESH_INTERVAL


def maintenance_config(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("token_maintenance") or {}


def maintenance_enabled(config: dict[str, Any]) -> bool:
    return bool(maintenance_config(config).get("enabled", True))


def maintenance_check_interval_seconds(config: dict[str, Any]) -> int:
    value = maintenance_config(config).get("check_interval_seconds", 3600)
    try:
        return max(60, int(value))
    except (TypeError, ValueError):
        return 3600


def maintenance_refresh_interval(config: dict[str, Any]) -> timedelta:
    value = maintenance_config(config).get("refresh_interval_days", 8)
    try:
        return timedelta(days=max(1, float(value)))
    except (TypeError, ValueError):
        return TOKEN_REFRESH_INTERVAL


def should_refresh_for_maintenance(profile: dict[str, Any], config: dict[str, Any]) -> bool:
    last_refresh = parse_refresh_time(profile.get("last_refresh"))
    if last_refresh is None:
        return True
    return utc_now() - last_refresh >= maintenance_refresh_interval(config)


def profile_name(profile: dict[str, Any]) -> str:
    return str(profile.get("name") or profile.get("account_id") or "unnamed")


def profiles(config: dict[str, Any]) -> list[dict[str, Any]]:
    values = config.get("profiles") or []
    if not values:
        raise RuntimeError("codex_profiles.json must contain at least one profile")
    return values


def profile_index_by_name(config: dict[str, Any], requested_name: str | None) -> int:
    values = profiles(config)
    if requested_name:
        for index, profile in enumerate(values):
            if profile.get("name") == requested_name:
                return index
    return 0


def select_profile(config: dict[str, Any]) -> dict[str, Any]:
    global _profile_cursor

    values = profiles(config)

    if config.get("selection_strategy", "active") == "round_robin":
        with _profile_lock:
            profile = values[_profile_cursor % len(values)]
            _profile_cursor += 1
            return profile

    active_name = config.get("active_profile")
    for profile in values:
        if profile.get("name") == active_name:
            return profile

    return values[0]


def ordered_failover_profiles(config: dict[str, Any]) -> list[dict[str, Any]]:
    values = profiles(config)
    if config.get("selection_strategy") != "sequential_failover":
        return [select_profile(config)]

    start = profile_index_by_name(config, config.get("active_profile"))
    return values[start:] + values[:start]


def set_active_profile(config: dict[str, Any], profile: dict[str, Any]) -> None:
    name = profile.get("name")
    if isinstance(name, str) and name:
        config["active_profile"] = name


def find_profile(config: dict[str, Any], requested_name: str | None) -> dict[str, Any]:
    if not requested_name:
        return select_profile(config)

    for profile in config.get("profiles") or []:
        if profile.get("name") == requested_name:
            return profile

    raise RuntimeError(f"Profile not found: {requested_name}")


def load_profile_base_url(profile: dict[str, Any], service_config: dict[str, Any]) -> str:
    defaults = service_config.get("request_defaults") or {}
    base_url = str(profile.get("base_url") or defaults.get("base_url") or "https://api.openai.com/v1").strip()
    if not base_url:
        raise RuntimeError(f"Profile {profile_name(profile)} is missing base_url")
    return base_url.rstrip("/")


def configured_models(config: dict[str, Any]) -> list[str]:
    defaults = config.get("request_defaults") or {}
    candidates = [
        defaults.get("model"),
        "gpt-5.5",
        "gpt-5.5-thinking",
        "gpt-5.5-pro",
        "gpt-5.4",
        "gpt-5.4-mini",
        "codex-mini-latest",
    ]
    for profile in config.get("profiles") or []:
        candidates.append(profile.get("model"))

    models: list[str] = []
    seen = set()
    for candidate in candidates:
        if not isinstance(candidate, str) or not candidate.strip():
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        models.append(candidate)
    return models


def profile_request_args(
    prompt: str,
    service_config: dict[str, Any],
    profile: dict[str, Any],
    body: dict[str, Any],
) -> Namespace:
    defaults = service_config.get("request_defaults") or {}

    return Namespace(
        prompt=prompt,
        model=body.get("model") or profile.get("model") or defaults.get("model"),
        reasoning_effort=(
            body.get("reasoning_effort")
            or profile.get("reasoning_effort")
            or defaults.get("reasoning_effort")
        ),
        instructions=body.get("instructions") or defaults.get("instructions") or CODEX_INSTRUCTIONS,
        stream=bool(body.get("stream", defaults.get("stream", False))),
        store=body.get("store", defaults.get("store", False)),
        shell_tool=bool(body.get("shell_tool", defaults.get("shell_tool", False))),
        include_encrypted_reasoning=bool(
            body.get("include_encrypted_reasoning", defaults.get("include_encrypted_reasoning", False))
        ),
    )


def upstream_request(profile: dict[str, Any], base_url: str, codex_body: dict[str, Any]) -> request.Request:
    headers = auth_headers_from_data(profile, f"profile {profile_name(profile)}")
    return make_request(response_url(base_url), headers, codex_body)


def upstream_endpoint_request(
    profile: dict[str, Any],
    base_url: str,
    path: str,
    method: str,
    body: dict[str, Any] | None = None,
) -> request.Request:
    headers = auth_headers_from_data(profile, f"profile {profile_name(profile)}")
    endpoint = response_url(base_url).rstrip("/") + path
    encoded_body = None
    if body is not None:
        encoded_body = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    req_headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "originator": "codex_cli_rs",
        "user-agent": "codex_cli_rs/0.126.0 (Windows 10; x86_64)",
        "version": "0.126.0",
        **headers,
    }
    return request.Request(endpoint, data=encoded_body, headers=req_headers, method=method)


def normalize_body_for_upstream(base_url: str, codex_body: dict[str, Any]) -> dict[str, Any]:
    if "chatgpt.com/backend-api/codex" not in base_url:
        return codex_body

    normalized = dict(codex_body)
    normalized["stream"] = True
    normalized.pop("truncation", None)
    return normalized


def apply_request_defaults(body: dict[str, Any], config: dict[str, Any], profile: dict[str, Any]) -> dict[str, Any]:
    defaults = config.get("request_defaults") or {}
    updated = dict(body)
    updated.setdefault("model", profile.get("model") or defaults.get("model") or "gpt-5.5")
    updated.setdefault("instructions", defaults.get("instructions") or CODEX_INSTRUCTIONS)
    updated.setdefault("store", defaults.get("store", False))
    updated.setdefault("parallel_tool_calls", True)

    reasoning_effort = (
        updated.pop("reasoning_effort", None)
        or profile.get("reasoning_effort")
        or defaults.get("reasoning_effort")
    )
    if reasoning_effort and "reasoning" not in updated:
        updated["reasoning"] = {"effort": reasoning_effort}

    return updated


def read_http_error(exc: HTTPError) -> str:
    return exc.read().decode("utf-8", errors="replace")


def is_quota_or_balance_error(status_code: int, details: str) -> bool:
    if status_code == 429:
        return True

    lowered = details.lower()
    markers = [
        "insufficient_quota",
        "quota",
        "rate_limit",
        "rate limit",
        "billing",
        "balance",
        "余额",
        "额度",
        "限额",
        "次数",
    ]
    return any(marker in lowered for marker in markers)


def refresh_chatgpt_tokens(profile: dict[str, Any]) -> dict[str, bool]:
    tokens = profile.get("tokens")
    if not isinstance(tokens, dict):
        raise RuntimeError(f"Profile {profile_name(profile)} is missing tokens")

    refresh_token = tokens.get("refresh_token")
    if not isinstance(refresh_token, str) or not refresh_token.strip():
        raise RuntimeError(f"Profile {profile_name(profile)} is missing tokens.refresh_token")

    payload = {
        "client_id": profile.get("client_id") or CODEX_CLIENT_ID,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token.strip(),
    }
    req = request.Request(
        REFRESH_TOKEN_URL,
        data=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        headers={
            "accept": "application/json",
            "content-type": "application/json",
            "originator": "codex_cli_rs",
            "user-agent": "codex_cli_rs/0.126.0 (Windows 10; x86_64)",
            "version": "0.126.0",
        },
        method="POST",
    )

    with request.urlopen(req, timeout=60) as res:
        data = json.loads(res.read().decode("utf-8", errors="replace"))

    updated = {
        "id_token": False,
        "access_token": False,
        "refresh_token": False,
    }
    for key in updated:
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            tokens[key] = value
            updated[key] = True

    profile["last_refresh"] = utc_now().isoformat().replace("+00:00", "Z")
    return updated


def refresh_profile_with_lock(profile: dict[str, Any]) -> dict[str, bool]:
    with _refresh_lock:
        return refresh_chatgpt_tokens(profile)


def open_upstream_with_refresh_retry(
    service_config: dict[str, Any],
    profile: dict[str, Any],
    base_url: str,
    codex_body: dict[str, Any],
    timeout: int,
):
    req = upstream_request(profile, base_url, codex_body)
    try:
        return request.urlopen(req, timeout=timeout)
    except HTTPError as exc:
        if exc.code != 401:
            raise

        refresh_profile_with_lock(profile)
        save_service_config(service_config)
        retry_req = upstream_request(profile, base_url, codex_body)
        return request.urlopen(retry_req, timeout=timeout)


def open_upstream_with_failover(
    service_config: dict[str, Any],
    codex_body: dict[str, Any],
    timeout: int,
):
    last_error: tuple[int, str] | None = None

    for profile in ordered_failover_profiles(service_config):
        base_url = load_profile_base_url(profile, service_config)
        upstream_body = normalize_body_for_upstream(base_url, codex_body)

        try:
            response = open_upstream_with_refresh_retry(service_config, profile, base_url, upstream_body, timeout)
            set_active_profile(service_config, profile)
            save_service_config(service_config)
            return profile, base_url, response
        except HTTPError as exc:
            details = read_http_error(exc)
            if is_quota_or_balance_error(exc.code, details):
                last_error = (exc.code, details)
                continue
            raise HTTPException(status_code=exc.code, detail=details) from exc

    if last_error:
        raise HTTPException(status_code=last_error[0], detail=last_error[1])

    raise HTTPException(status_code=500, detail="No profile available")


def remember_response_profile(raw_line: bytes, profile: dict[str, Any]) -> None:
    try:
        line = raw_line.decode("utf-8", errors="replace").strip()
    except Exception:
        return
    if not line.startswith("data:"):
        return

    data = line.removeprefix("data:").strip()
    if not data or data == "[DONE]":
        return

    try:
        payload = json.loads(data)
    except json.JSONDecodeError:
        return

    response = payload.get("response") if isinstance(payload, dict) else None
    response_id = response.get("id") if isinstance(response, dict) else None
    if isinstance(response_id, str):
        _response_profiles[response_id] = profile_name(profile)


def urllib_error_to_http(exc: HTTPError) -> HTTPException:
    details = exc.read().decode("utf-8", errors="replace")
    return HTTPException(status_code=exc.code, detail=details)


def prompt_from_body(body: dict[str, Any]) -> str:
    prompt = body.get("prompt")
    if isinstance(prompt, str):
        return prompt

    input_value = body.get("input")
    if isinstance(input_value, str):
        return input_value

    if isinstance(input_value, list):
        parts: list[str] = []
        for item in input_value:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if isinstance(content, str):
                parts.append(content)
            elif isinstance(content, list):
                for content_item in content:
                    if isinstance(content_item, dict):
                        text = content_item.get("text")
                        if isinstance(text, str):
                            parts.append(text)
        if parts:
            return "\n".join(parts)

    raise HTTPException(status_code=400, detail="Request body must include prompt or input text.")


def stream_upstream(
    service_config: dict[str, Any],
    codex_body: dict[str, Any],
    timeout: int,
):
    try:
        profile, _, res = open_upstream_with_failover(service_config, codex_body, timeout)
        with res:
            for raw_line in res:
                remember_response_profile(raw_line, profile)
                yield raw_line
    except HTTPError as exc:
        error = read_http_error(exc)
        yield f"event: error\ndata: {json.dumps({'error': error}, ensure_ascii=False)}\n\n".encode("utf-8")
    except HTTPException as exc:
        yield f"event: error\ndata: {json.dumps({'error': exc.detail}, ensure_ascii=False)}\n\n".encode("utf-8")
    except URLError as exc:
        yield f"event: error\ndata: {json.dumps({'error': str(exc)}, ensure_ascii=False)}\n\n".encode("utf-8")


def token_maintenance_worker() -> None:
    while not _maintenance_stop.is_set():
        try:
            service_config = load_service_config()
            if maintenance_enabled(service_config):
                changed = False
                for profile in profiles(service_config):
                    if not should_refresh_for_maintenance(profile, service_config):
                        continue
                    try:
                        updated = refresh_profile_with_lock(profile)
                        changed = True
                        logger.info("refreshed profile %s: %s", profile_name(profile), updated)
                    except Exception as exc:
                        logger.warning("failed to refresh profile %s: %s", profile_name(profile), exc)
                if changed:
                    save_service_config(service_config)
            interval = maintenance_check_interval_seconds(service_config)
        except Exception as exc:
            logger.warning("token maintenance failed: %s", exc)
            interval = 3600

        _maintenance_stop.wait(interval)


@app.on_event("startup")
def start_token_maintenance() -> None:
    global _maintenance_thread
    if _maintenance_thread and _maintenance_thread.is_alive():
        return
    _maintenance_stop.clear()
    _maintenance_thread = threading.Thread(
        target=token_maintenance_worker,
        daemon=True,
        name="token-maintenance",
    )
    _maintenance_thread.start()


@app.on_event("shutdown")
def stop_token_maintenance() -> None:
    _maintenance_stop.set()


@app.get("/health")
def health() -> dict[str, Any]:
    config = load_service_config()
    return {
        "ok": True,
        "selection_strategy": config.get("selection_strategy", "active"),
        "profiles": [profile_name(profile) for profile in config.get("profiles", [])],
        "token_maintenance": {
            "enabled": maintenance_enabled(config),
            "check_interval_seconds": maintenance_check_interval_seconds(config),
            "refresh_interval_days": maintenance_refresh_interval(config).days,
        },
    }


@app.get("/v1/models")
def list_models() -> dict[str, Any]:
    config = load_service_config()
    return {
        "object": "list",
        "data": [
            {
                "id": model,
                "object": "model",
                "created": 0,
                "owned_by": "local-codex-proxy",
            }
            for model in configured_models(config)
        ],
    }


@app.get("/v1/models/{model_id}")
def get_model(model_id: str) -> dict[str, Any]:
    config = load_service_config()
    if model_id not in configured_models(config):
        raise HTTPException(status_code=404, detail=f"Model not found: {model_id}")
    return {
        "id": model_id,
        "object": "model",
        "created": 0,
        "owned_by": "local-codex-proxy",
    }


@app.get("/profiles/status")
def profiles_status() -> dict[str, Any]:
    config = load_service_config()
    return {
        "selection_strategy": config.get("selection_strategy", "active"),
        "active_profile": config.get("active_profile"),
        "token_maintenance": {
            "enabled": maintenance_enabled(config),
            "check_interval_seconds": maintenance_check_interval_seconds(config),
            "refresh_interval_days": maintenance_refresh_interval(config).days,
        },
        "profiles": [
            {
                "name": profile_name(profile),
                "last_refresh": profile.get("last_refresh"),
                "needs_maintenance_refresh": should_refresh_for_maintenance(profile, config),
                "has_access_token": bool((profile.get("tokens") or {}).get("access_token")),
                "has_refresh_token": bool((profile.get("tokens") or {}).get("refresh_token")),
            }
            for profile in profiles(config)
        ],
    }


@app.post("/send")
async def send(request_obj: Request):
    body = await request_obj.json()
    prompt = prompt_from_body(body)
    service_config = load_service_config()
    first_profile = ordered_failover_profiles(service_config)[0]
    args = profile_request_args(prompt, service_config, first_profile, body)
    codex_body = build_codex_body(args, {})
    first_base_url = load_profile_base_url(first_profile, service_config)
    codex_body = normalize_body_for_upstream(first_base_url, codex_body)
    timeout = int((service_config.get("request_defaults") or {}).get("timeout", 300))

    try:
        if codex_body.get("stream"):
            return StreamingResponse(
                stream_upstream(service_config, codex_body, timeout),
                media_type="text/event-stream",
            )

        profile, base_url, res = open_upstream_with_failover(service_config, codex_body, timeout)
        with res:
            data = json.loads(res.read().decode("utf-8", errors="replace"))

        return {
            "profile": profile_name(profile),
            "base_url": base_url,
            "response": data,
            "text": collect_text_from_response(data),
        }
    except HTTPError as exc:
        raise urllib_error_to_http(exc) from exc
    except URLError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/refresh-profile")
async def refresh_profile(request_obj: Request):
    body = await request_obj.json()
    requested_name = body.get("profile") if isinstance(body, dict) else None
    force = bool(body.get("force")) if isinstance(body, dict) else False
    service_config = load_service_config()

    try:
        profile = find_profile(service_config, requested_name)
        if should_skip_manual_refresh(profile, force):
            return {
                "ok": True,
                "profile": profile_name(profile),
                "skipped": True,
                "reason": "last_refresh is within 8 days; pass force:true to refresh anyway",
                "last_refresh": profile.get("last_refresh"),
            }

        updated = refresh_profile_with_lock(profile)
        save_service_config(service_config)
        return {
            "ok": True,
            "profile": profile_name(profile),
            "skipped": False,
            "updated": updated,
            "last_refresh": profile.get("last_refresh"),
        }
    except HTTPError as exc:
        raise urllib_error_to_http(exc) from exc
    except URLError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/v1/responses")
async def responses(request_obj: Request):
    body = await request_obj.json()
    service_config = load_service_config()
    first_profile = ordered_failover_profiles(service_config)[0]
    codex_body = apply_request_defaults(body, service_config, first_profile)
    first_base_url = load_profile_base_url(first_profile, service_config)
    codex_body = normalize_body_for_upstream(first_base_url, codex_body)
    timeout = int((service_config.get("request_defaults") or {}).get("timeout", 300))

    if codex_body.get("stream", True):
        return StreamingResponse(
            stream_upstream(service_config, codex_body, timeout),
            media_type="text/event-stream",
        )

    profile, _, res = open_upstream_with_failover(service_config, codex_body, timeout)
    with res:
        data = json.loads(res.read().decode("utf-8", errors="replace"))
    response_id = data.get("id")
    if isinstance(response_id, str):
        _response_profiles[response_id] = profile_name(profile)
    return JSONResponse(data)


def profile_for_response(config: dict[str, Any], response_id: str) -> dict[str, Any]:
    remembered = _response_profiles.get(response_id)
    if remembered:
        try:
            return find_profile(config, remembered)
        except RuntimeError:
            pass
    return select_profile(config)


@app.get("/v1/responses/{response_id}")
def get_response(response_id: str):
    config = load_service_config()
    profile = profile_for_response(config, response_id)
    base_url = load_profile_base_url(profile, config)
    req = upstream_endpoint_request(profile, base_url, f"/{response_id}", "GET")
    try:
        with request.urlopen(req, timeout=60) as res:
            return JSONResponse(json.loads(res.read().decode("utf-8", errors="replace")))
    except HTTPError as exc:
        raise urllib_error_to_http(exc) from exc
    except URLError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/v1/responses/{response_id}/cancel")
def cancel_response(response_id: str):
    config = load_service_config()
    profile = profile_for_response(config, response_id)
    base_url = load_profile_base_url(profile, config)
    req = upstream_endpoint_request(profile, base_url, f"/{response_id}/cancel", "POST", {})
    try:
        with request.urlopen(req, timeout=60) as res:
            return JSONResponse(json.loads(res.read().decode("utf-8", errors="replace")))
    except HTTPError as exc:
        raise urllib_error_to_http(exc) from exc
    except URLError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
