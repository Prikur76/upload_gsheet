"""Выгрузка таблицы кураторов (водители с куратором и датой создания)."""

import logging
from datetime import datetime

import httpx
import polars as pl
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from upload_gsheet.config import (
    DRIVERS_URL,
    PASSWORD,
    SUPERVISERS_RANGE,
    SUPERVISERS_SPREADSHEET_ID,
    USER,
)
from upload_gsheet.sheets.client import SheetsClient

logger = logging.getLogger(__name__)

_RENAME = {
    "ID": "Идентификатор",
    "FIO": "ФИО",
    "PhoneNumber": "Номер телефона",
    "DriverDateCreate": "Дата создания",
    "Status": "Статус",
    "Comment": "Комментарий",
    "Supervisor": "Куратор",
}

_REQUIRED_COLUMNS = [
    "Идентификатор",
    "ФИО",
    "Номер телефона",
    "Дата создания",
    "Год-мес",
    "Статус",
    "Куратор",
    "Комментарий",
    "Обновлено",
]


@retry(
    retry=retry_if_exception_type(
        (
            httpx.ConnectError,
            httpx.TimeoutException,
        )
    ),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
def _fetch_drivers_json() -> list:
    with httpx.Client(timeout=20) as client:
        resp = client.get(
            DRIVERS_URL,
            auth=(USER, PASSWORD),
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()


def run_supervisers(client: SheetsClient | None = None) -> None:
    """Один проход: выгрузка кураторов в отдельную таблицу."""
    if not SUPERVISERS_SPREADSHEET_ID:
        logger.info(
            "SUPERVISERS_SPREADSHEET_ID не задан, пропуск выгрузки кураторов"
        )
        return
    sheets = client or SheetsClient()
    data = _fetch_drivers_json()
    df = pl.DataFrame(data)
    df = df.rename(_RENAME)
    df = df.filter(
        (pl.col("Куратор") != "")
        & (pl.col("Дата создания") > "2023-12-31T23:59:59")
    )
    df = df.with_columns(
        pl.col("Дата создания")
        .str.to_datetime()
        .dt.strftime("%Y-%m")
        .alias("Год-мес"),
        pl.col("Дата создания")
        .str.to_datetime()
        .dt.strftime("%Y-%m-%d")
        .alias("Дата создания"),
        pl.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias(
            "Обновлено"
        ),
    )
    for col in _REQUIRED_COLUMNS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))
    df = df.select(_REQUIRED_COLUMNS).sort("Идентификатор", descending=True)
    rows = [list(df.columns)] + [list(row) for row in df.iter_rows()]
    sheets.batch_update_values(
        SUPERVISERS_SPREADSHEET_ID, SUPERVISERS_RANGE, rows
    )


def run_supervisers_safe(client: SheetsClient | None = None) -> bool:
    """Выполняет run_supervisers с перехватом ошибок. Возвращает True при успехе."""
    try:
        run_supervisers(client)
        return True
    except Exception as e:
        logger.error("Ошибка выгрузки кураторов: %s", e)
        return False
