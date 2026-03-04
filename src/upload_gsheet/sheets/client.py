"""Единый клиент Google Sheets (service account)."""

import socket
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from upload_gsheet.config import GOOGLE_CREDENTIALS_PATH

socket.setdefaulttimeout(150)

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


class SheetsClient:
    """Клиент для записи и чтения Google Таблиц."""

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
