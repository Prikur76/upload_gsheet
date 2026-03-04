"""Настройка логирования для приложения."""

import logging

from upload_gsheet.config import LOG_DIR, LOG_FILE


def setup_logging(level: int = logging.INFO) -> None:
    """Настраивает корневой логгер: файл + консоль."""
    log_format = (
        "%(asctime)s - [%(levelname)s] - %(name)s - "
        "(%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
    )
    formatter = logging.Formatter(log_format)

    root = logging.getLogger()
    root.setLevel(level)
    for h in list(root.handlers):
        root.removeHandler(h)

    stream = logging.StreamHandler()
    stream.setLevel(level)
    stream.setFormatter(formatter)
    root.addHandler(stream)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / LOG_FILE
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
