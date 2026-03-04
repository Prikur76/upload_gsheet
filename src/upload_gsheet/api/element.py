"""Клиент API 1С:Элемент (водители и автомобили)."""

from typing import Any

import polars as pl
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from upload_gsheet.formatters.drivers_cars import (
    format_date_string,
    remove_chars,
)


@retry(
    retry=retry_if_exception_type(
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        )
    ),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
def _post_json(url: str, auth: tuple[str, str], json: dict) -> list:
    with requests.post(url=url, auth=auth, json=json, stream=True) as resp:
        resp.raise_for_status()
        return resp.json()


@retry(
    retry=retry_if_exception_type(
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        )
    ),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
def _get_json(
    url: str, auth: tuple[str, str], params: dict | None = None
) -> list:
    with requests.get(
        url=url, params=params or {}, auth=auth, stream=True
    ) as resp:
        resp.raise_for_status()
        return resp.json()


class ElementClient:
    """Клиент 1С:Элемент для водителей и автомобилей."""

    def __init__(self, user: str | None = None, password: str | None = None):
        self.user = user or ""
        self.password = password or ""

    def _auth(self) -> tuple[str, str]:
        return (self.user, self.password)

    def get_drivers_raw(self, url: str) -> list[dict[str, Any]]:
        """Сырой список водителей (JSON)."""
        return _post_json(url, self._auth(), {"Status": ["Работает"]})

    def fetch_active_drivers(
        self,
        url: str,
        conditions_exclude: list[str] | None = None,
    ) -> pl.DataFrame:
        """Отфильтрованный список работающих водителей."""
        exclude = (
            conditions_exclude if conditions_exclude is not None else [""]
        )
        raw = self.get_drivers_raw(url)
        df = pl.DataFrame(raw, infer_schema_length=None)
        df = df.filter(
            (pl.col("Status") == "Работает")
            & (~pl.col("ExternalCar"))
            & (~pl.col("NameConditionWork").is_in(exclude))
            & (pl.col("PhoneNumber").str.strip_chars() != "")
            & (pl.col("DriversLicenseSerialNumber").str.strip_chars() != "")
            & (pl.col("Car").str.strip_chars() != "")
        )
        df = df.with_columns(
            pl.col("DatePL")
            .map_elements(
                lambda s: format_date_string(str(s), "%Y-%m-%d"),
                return_dtype=pl.Utf8,
            )
            .alias("DatePL"),
            pl.col("PhoneNumber")
            .map_elements(
                lambda s: remove_chars(str(s)) if s else "",
                return_dtype=pl.Utf8,
            )
            .alias("PhoneNumber"),
        )
        return df.sort("FIO")

    def get_cars_raw(
        self, url: str, inn: str | None = None
    ) -> list[dict[str, Any]]:
        """Сырой список машин (JSON)."""
        return _get_json(url, self._auth(), {"inn": inn} if inn else None)

    def fetch_active_cars(
        self, url: str, inn: str | None = None
    ) -> pl.DataFrame:
        """Список активных машин."""
        raw = self.get_cars_raw(url, inn)
        df = pl.DataFrame(raw, infer_schema_length=None)
        df = df.filter(
            pl.col("Activity")
            & ~pl.col("DisableDocumentStatus")
            & ~pl.col("DisableContract")
            & (~pl.col("Department").is_in(["ЛИЧНАЯ", "КАРШЕРИНГ"]))
            & (~pl.col("Status").is_in(["АРХИВ"]))
        )
        df = df.sort("Code")
        df = df.with_columns(
            pl.col("YearCar")
            .map_elements(
                lambda s: format_date_string(str(s), "%Y") if s else "",
                return_dtype=pl.Utf8,
            )
            .alias("YearCar")
        )
        return df
