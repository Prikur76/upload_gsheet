"""Единый клиент Google Sheets (service account)."""

import socket
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from upload_gsheet.config import GOOGLE_CREDENTIALS_PATH

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_service: Any = None


def _get_service():
    """Возвращает кэшированный сервис Google Sheets API."""
    global _service
    if _service is None:
        creds = service_account.Credentials.from_service_account_file(
            str(GOOGLE_CREDENTIALS_PATH), scopes=_SCOPES
        )
        _service = build(
            "sheets",
            "v4",
            credentials=creds,
            static_discovery=False,
            cache_discovery=False,
        )
    return _service


def _should_retry(exc: BaseException) -> bool:
    """Сетевые ошибки и временные ошибки Google API (5xx, 409)."""
    if isinstance(exc, (socket.timeout, OSError)):
        return True
    if isinstance(exc, HttpError) and exc.resp is not None:
        return exc.resp.status in (409, 500, 502, 503)
    return False


class SheetsClient:
    """Клиент для записи и чтения Google Таблиц."""

    @retry(
        retry=retry_if_exception(_should_retry),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def batch_update_values(
        self, spreadsheet_id: str, sheet_range: str, data: list
    ) -> dict:
        """Записывает данные в диапазон. data — список строк (списков ячеек)."""
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": [{"range": sheet_range, "values": data}],
        }
        sheet = _get_service().spreadsheets()
        response = (
            sheet.values()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
            .execute()
        )
        if not response:
            raise HttpError(resp=None, content=b"")
        return response

    @retry(
        retry=retry_if_exception(_should_retry),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def clear_range(self, spreadsheet_id: str, sheet_range: str) -> dict:
        """Очищает диапазон."""
        body = {"ranges": [sheet_range]}
        response = (
            _get_service()
            .spreadsheets()
            .values()
            .batchClear(spreadsheetId=spreadsheet_id, body=body)
            .execute()
        )
        if not response:
            raise HttpError(resp=None, content=b"")
        return response
