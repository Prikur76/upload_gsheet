"""Проверка доступности API 1С:Элемент."""

import logging
import sys
from pathlib import Path
from urllib.parse import urlparse

import requests

from upload_gsheet.config import DRIVERS_URL, LOG_DIR, PASSWORD, USER

_STATE_FILE = LOG_DIR / "1c_health.state"
_HEALTH_LOG = LOG_DIR / "health_1c.log"
_CONNECT_TIMEOUT = 5
_READ_TIMEOUT = 15

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


def _read_state() -> str | None:
    if not _STATE_FILE.exists():
        return None
    return _STATE_FILE.read_text(encoding="utf-8").strip() or None


def _write_state(state: str) -> None:
    _STATE_FILE.write_text(state, encoding="utf-8")


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


def probe_1c() -> tuple[bool, str]:
    """Проверяет доступность API водителей. Возвращает (успех, описание)."""
    tcp_ok, tcp_msg = _tcp_reachable(DRIVERS_URL, _CONNECT_TIMEOUT)
    if not tcp_ok:
        return False, tcp_msg

    try:
        response = requests.post(
            DRIVERS_URL,
            auth=(USER, PASSWORD),
            json={"Status": ["Работает"]},
            timeout=(_CONNECT_TIMEOUT, _READ_TIMEOUT),
        )
        response.raise_for_status()
        return (
            True,
            f"{tcp_msg}; HTTP {response.status_code}, "
            f"{len(response.content)} bytes",
        )
    except requests.exceptions.ConnectTimeout:
        return False, f"{tcp_msg}; ConnectTimeout ({_CONNECT_TIMEOUT}s)"
    except requests.exceptions.ReadTimeout:
        return False, f"{tcp_msg}; ReadTimeout ({_READ_TIMEOUT}s)"
    except requests.exceptions.HTTPError as exc:
        code = exc.response.status_code if exc.response is not None else "?"
        return False, f"{tcp_msg}; HTTP {code}"
    except requests.exceptions.RequestException as exc:
        return False, f"{tcp_msg}; {type(exc).__name__}: {exc}"


def _notify_state_change(
    previous: str | None, ok: bool, detail: str
) -> None:
    new_state = "ok" if ok else "fail"
    if previous == new_state:
        return
    if ok:
        _alert_logger.warning("1С API восстановлена: %s", detail)
    else:
        _alert_logger.error("1С API недоступна: %s", detail)


def main() -> None:
    """Одна проверка доступности 1С. Код выхода: 0 — OK, 1 — сбой."""
    _setup_logging()
    ok, detail = probe_1c()
    previous = _read_state()
    new_state = "ok" if ok else "fail"

    if ok:
        logger.info("OK: %s", detail)
    else:
        logger.warning("FAIL: %s", detail)

    _notify_state_change(previous, ok, detail)
    _write_state(new_state)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
