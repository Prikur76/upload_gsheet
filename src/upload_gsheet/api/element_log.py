"""Контекст HTTP-запросов к 1С для логов техподдержки."""

import json
import logging
import re
from typing import Any

import requests

_MAX_RESPONSE_CHARS = 500
_last_request: dict[str, Any] = {}
_CAUSE_MESSAGE = re.compile(r", '([^']+)'\)\)?$")


def format_exc(exc: BaseException) -> str:
    """Текст ошибки; для обёрток urllib3 показывает исходную причину."""
    message = str(exc)
    if "Caused by" in message:
        match = _CAUSE_MESSAGE.search(message)
        if match:
            return match.group(1)
    cause = exc.__cause__
    if cause is not None:
        cause_text = str(cause)
        if "Max retries exceeded" in message:
            return cause_text
        if cause_text not in message:
            return f"{message} (причина: {cause})"
    return message


def set_request_context(
    method: str,
    url: str,
    *,
    user: str | None = None,
    json_body: Any = None,
    params: dict | None = None,
    timeout: tuple[int, int] | None = None,
) -> None:
    """Запоминает параметры последнего запроса к 1С."""
    global _last_request
    _last_request = {
        "method": method,
        "url": url,
        "user": user,
        "json_body": json_body,
        "params": params,
        "timeout": timeout,
    }


def describe_request(
    method: str,
    url: str,
    *,
    user: str | None = None,
    json_body: Any = None,
    params: dict | None = None,
    timeout: tuple[int, int] | None = None,
) -> str:
    """Описание запроса без пароля (для передачи в техподдержку 1С)."""
    parts = [f"{method} {url}"]
    if params:
        parts.append(
            "params="
            + json.dumps(params, ensure_ascii=False, sort_keys=True)
        )
    if json_body is not None:
        parts.append(
            "body="
            + json.dumps(json_body, ensure_ascii=False, sort_keys=True)
        )
    if user:
        parts.append(f"user={user}")
    if timeout is not None:
        parts.append(f"timeout=connect:{timeout[0]}s,read:{timeout[1]}s")
    return "; ".join(parts)


def _response_preview(response: requests.Response) -> str:
    try:
        text = response.text
    except Exception as exc:
        return f"status={response.status_code}, тело недоступно: {exc}"
    if len(text) > _MAX_RESPONSE_CHARS:
        text = text[:_MAX_RESPONSE_CHARS] + "…"
    return f"status={response.status_code}, body={text!r}"


def log_1c_request_error(
    log: logging.Logger,
    exc: BaseException,
    method: str,
    url: str,
    *,
    user: str | None = None,
    json_body: Any = None,
    params: dict | None = None,
    timeout: tuple[int, int] | None = None,
) -> None:
    """Пишет в лог ошибку и параметры запроса для техподдержки 1С."""
    request_info = describe_request(
        method,
        url,
        user=user,
        json_body=json_body,
        params=params,
        timeout=timeout,
    )
    response = getattr(exc, "response", None)
    if isinstance(exc, requests.exceptions.HTTPError) and response is not None:
        log.error(
            "Ошибка запроса к 1С: %s | запрос: %s | ответ: %s",
            format_exc(exc),
            request_info,
            _response_preview(response),
        )
        return
    log.error(
        "Ошибка запроса к 1С: %s | запрос: %s",
        format_exc(exc),
        request_info,
    )


def log_last_request_error(
    log: logging.Logger, exc: BaseException
) -> None:
    """Логирует ошибку с контекстом последнего запроса к 1С."""
    if not _last_request:
        return
    log_1c_request_error(log, exc, **_last_request)
