"""Единая конфигурация из переменных окружения."""

import os
from pathlib import Path

from environs import Env

env = Env()
env.read_env()

# 1С:Элемент
USER = env.str("ELEMENT_LOGIN")
PASSWORD = env.str("ELEMENT_PASSWORD")
DRIVERS_URL = env.str("ELEMENT_DRIVERS_URL")
CARS_URL = env.str("ELEMENT_CARS_URL")

# Основная таблица (водители + автопарк)
REPORT_ID = env.str("REPORT_SPREADSHEETS_ID")
RANGE_FOR_UPLOAD = env.str("RANGE_FOR_UPLOAD")
RANGE_FOR_UPLOAD_DRIVERS = env.str("RANGE_FOR_UPLOAD_DRIVERS")

# Google: один файл учёток для всех операций
BASE_DIR = Path(__file__).resolve().parent.parent.parent
_creds_raw = env.str("GOOGLE_CREDENTIALS_PATH", "creds.json")
GOOGLE_CREDENTIALS_PATH = (
    Path(_creds_raw) if os.path.isabs(_creds_raw) else BASE_DIR / _creds_raw
)

# Таблица кураторов (ID таблицы и диапазон листа)
SUPERVISERS_SPREADSHEET_ID = env.str("SUPERVISERS_SPREADSHEET_ID", "")
SUPERVISERS_RANGE = env.str("SUPERVISERS_RANGE", "B1:J")

# Исключаемые условия работы водителей
EXCLUDE_ROSTER = [
    "",
    "Комфорт",
    "Штатный",
    "Подключашки 2 %",
    "ПОДКЛЮЧАШКА 3%",
]

# Логи
LOG_DIR = Path(env.str("LOG_DIR", str(BASE_DIR / "logs")))
LOG_FILE = env.str("LOG_FILE", "errors.log")
