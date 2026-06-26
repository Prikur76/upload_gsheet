"""Проверка доступности API 1С:Элемент."""

import json
import logging
import sys
from urllib.parse import urlparse

import requests

from upload_gsheet.api.element import get_1c_session
from upload_gsheet.api.element_log import describe_request
from upload_gsheet.config import (
    DRIVERS_URL,
    ELEMENT_CONNECT_TIMEOUT,
    ELEMENT_READ_TIMEOUT,
    LOG_DIR,
    PASSWORD,
    USER,
)

_STATE_FILE = LOG_DIR / "1c_health.state"
_HEALTH_LOG = LOG_DIR / "health_1c.log"
_PROBE_CHUNK_SIZE = 4096
_FAIL_ALERT_AFTER = 3
_OK_ALERT_AFTER = 2
_PROBE_BODY = {"Status": ["Работает"]}

logger = logging.getLogger(__name__)
_alert_logger = logging.getLogger("upload_gsheet.healthcheck.alert")


def _setup_logging() -> None:
    """Лог проверок в health_1c.log, алерты — в errors.log."""
    log_format = (
        "%(asctime)s - [%(levelname)s] - %(name)s - "
        "(%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
    )
    formatter = logging.Formatter(log_format)

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for handler in list(root.handlers):
        root.removeHandler(handler)

    health_handler = logging.FileHandler(
        _HEALTH_LOG, encoding="utf-8"
    )
    health_handler.setFormatter(formatter)
    root.addHandler(health_handler)

    from upload_gsheet.config import LOG_FILE

    alert_handler = logging.FileHandler(
        LOG_DIR / LOG_FILE, encoding="utf-8"
    )
    alert_handler.setLevel(logging.WARNING)
    alert_handler.setFormatter(formatter)
    _alert_logger.addHandler(alert_handler)
    _alert_logger.setLevel(logging.WARNING)
    _alert_logger.propagate = False


def _load_state() -> dict:
    """Состояние алерта и счётчики подряд идущих проверок."""
    default = {"alert": None, "fail_streak": 0, "ok_streak": 0}
    if not _STATE_FILE.exists():
        return default
    raw = _STATE_FILE.read_text(encoding="utf-8").strip()
    if not raw:
        return default
    if raw in ("ok", "fail"):
        return {"alert": raw, "fail_streak": 0, "ok_streak": 0}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return default
    return {
        "alert": data.get("alert"),
        "fail_streak": int(data.get("fail_streak", 0)),
        "ok_streak": int(data.get("ok_streak", 0)),
    }


def _save_state(state: dict) -> None:
    _STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False),
        encoding="utf-8",
    )


def _tcp_reachable(url: str, timeout: float) -> tuple[bool, str]:
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    if not host:
        return False, "некорректный URL"
    try:
        import socket

        with socket.create_connection((host, port), timeout=timeout):
            pass
        return True, f"TCP {host}:{port} OK"
    except OSError as exc:
        return False, f"TCP {host}:{port} FAIL: {exc}"


def _probe_request_desc() -> str:
    return describe_request(
        "POST",
        DRIVERS_URL,
        user=USER,
        json_body=_PROBE_BODY,
        timeout=(ELEMENT_CONNECT_TIMEOUT, ELEMENT_READ_TIMEOUT),
    )


def probe_1c() -> tuple[bool, str]:
    """Проверяет API: TCP, затем начало ответа без полной загрузки JSON."""
    tcp_ok, tcp_msg = _tcp_reachable(
        DRIVERS_URL, float(ELEMENT_CONNECT_TIMEOUT)
    )
    if not tcp_ok:
        return False, tcp_msg

    try:
        with get_1c_session().post(
            DRIVERS_URL,
            auth=(USER, PASSWORD),
            json=_PROBE_BODY,
            stream=True,
            timeout=(ELEMENT_CONNECT_TIMEOUT, ELEMENT_READ_TIMEOUT),
        ) as response:
            response.raise_for_status()
            chunk = next(
                response.iter_content(chunk_size=_PROBE_CHUNK_SIZE),
                b"",
            )
        if not chunk:
            return False, f"{tcp_msg}; HTTP {response.status_code}, пустой ответ"
        return (
            True,
            f"{tcp_msg}; HTTP {response.status_code}, "
            f"получено {len(chunk)} bytes",
        )
    except requests.exceptions.ConnectTimeout:
        return (
            False,
            f"{tcp_msg}; ConnectTimeout ({ELEMENT_CONNECT_TIMEOUT}s)",
        )
    except requests.exceptions.ReadTimeout:
        return (
            False,
            f"{tcp_msg}; ReadTimeout ({ELEMENT_READ_TIMEOUT}s)",
        )
    except requests.exceptions.HTTPError as exc:
        code = exc.response.status_code if exc.response is not None else "?"
        return False, f"{tcp_msg}; HTTP {code}"
    except requests.exceptions.RequestException as exc:
        return False, f"{tcp_msg}; {type(exc).__name__}: {exc}"


def _update_alerts(
    ok: bool, detail: str, state: dict, request_desc: str
) -> dict:
    """Обновляет счётчики и пишет в errors.log только после серии сбоев."""
    if ok:
        state["ok_streak"] += 1
        state["fail_streak"] = 0
        if (
            state["alert"] == "fail"
            and state["ok_streak"] >= _OK_ALERT_AFTER
        ):
            _alert_logger.warning("1С API восстановлена: %s", detail)
            state["alert"] = "ok"
    else:
        state["fail_streak"] += 1
        state["ok_streak"] = 0
        if (
            state["alert"] != "fail"
            and state["fail_streak"] >= _FAIL_ALERT_AFTER
        ):
            _alert_logger.error(
                "1С API недоступна: %s | запрос: %s",
                detail,
                request_desc,
            )
            state["alert"] = "fail"
    return state


def main() -> None:
    """Одна проверка доступности 1С. Код выхода: 0 — OK, 1 — сбой."""
    _setup_logging()
    request_desc = _probe_request_desc()
    ok, detail = probe_1c()
    state = _update_alerts(ok, detail, _load_state(), request_desc)

    if ok:
        logger.info("OK: %s", detail)
    else:
        logger.warning("FAIL: %s", detail)

    _save_state(state)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
