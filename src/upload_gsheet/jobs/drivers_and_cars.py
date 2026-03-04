"""Выгрузка водителей и автопарка в основную таблицу."""

import logging
from datetime import datetime
from typing import Any

import polars as pl
import pytz
import requests
from googleapiclient.errors import HttpError

from upload_gsheet.api.element import ElementClient
from upload_gsheet.config import (
    CARS_URL,
    DRIVERS_URL,
    EXCLUDE_ROSTER,
    PASSWORD,
    RANGE_FOR_UPLOAD,
    RANGE_FOR_UPLOAD_DRIVERS,
    REPORT_ID,
    USER,
)
from upload_gsheet.formatters import drivers_cars as fmt
from upload_gsheet.sheets.client import SheetsClient

logger = logging.getLogger(__name__)

_DRIVERS_COLUMNS = [
    "ID",
    "FIO",
    "Sex",
    "BirthDate",
    "Phones",
    "PassportInfo",
    "PassportRegistrationAddress",
    "ActualAddress",
    "DriverLicenseInfo",
    "Comment",
    "SNILS",
    "INN",
    "OGRN",
    "DriverDateCreate",
    "EmploymentDate",
    "DismissalDate",
    "Experience",
    "NameConditionWork",
    "Car",
    "CarDepartment",
    "BeginContract",
    "EndContract",
    "DatePL",
    "ConsolidBalance",
    "Supervisor",
    "KIS_ART_DriverId",
    "Marketing",
    "DefaultID",
]

_ROSTER_COLUMNS = [
    "CarInfo",
    "Model",
    "Number",
    "VIN",
    "YearCar",
    "Transmission",
    "GBO",
    "MileAge",
    "MileageGroup",
    "BodyColor",
    "Brand",
    "LandLord",
    "Organization",
    "STSDetail",
    "STSSeriesNumber",
    "STSIssueDate",
    "STSValidityDate",
    "TODetail",
    "TOSeriesNumber",
    "TOIssueDate",
    "TOValidityDate",
    "OSAGOInsurer",
    "OSAGODetail",
    "OSAGOSeriesNumber",
    "OSAGOIssueDate",
    "OSAGOValidityDate",
    "LicenseLicensee",
    "LicenseDetail",
    "LicenseSeriesNumber",
    "LicenseIssueDate",
    "LicenseValidityDate",
    "StatusDetail",
    "Status",
    "SubStatus",
    "StatusFix",
    "SubStatusFix",
    "Reason",
    "Comment",
    "FormattedCommentCar",
    "CarLocation",
    "Department",
    "Region",
    "RegionFix",
    "DriverInfo",
    "DatePL",
    "DateUpload",
]


def _row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    """Преобразует строку Polars (struct/dict) в обычный dict для форматтеров."""
    return dict(row) if isinstance(row, dict) else row


def _add_driver_formatted_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Добавляет колонки Phones, PassportInfo, DriverLicenseInfo."""
    return df.with_columns(
        pl.struct(["PhoneNumber", "PhoneNumber2"])
        .map_elements(
            lambda s: fmt.format_driver_phones(_row_to_dict(s)),
            return_dtype=pl.Utf8,
        )
        .alias("Phones"),
        pl.struct(
            [
                "PassportSerialNumber",
                "PassportIssueDate",
                "PassportDepartmentName",
            ]
        )
        .map_elements(
            lambda s: fmt.format_passport_info(_row_to_dict(s)),
            return_dtype=pl.Utf8,
        )
        .alias("PassportInfo"),
        pl.struct(
            [
                "DriversLicenseSerialNumber",
                "DriversLicenseIssueDate",
                "DriversLicenseExpiryDate",
                "DriversLicenseExperienceTotalSince",
            ]
        )
        .map_elements(
            lambda s: fmt.format_driver_license(_row_to_dict(s)),
            return_dtype=pl.Utf8,
        )
        .alias("DriverLicenseInfo"),
    )


def _add_car_formatted_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Добавляет все производные колонки для машин."""
    df = df.with_columns(
        pl.when(pl.col("KPPType") == "АКПП")
        .then(pl.lit("АТ"))
        .when(pl.col("KPPType") == "МКПП")
        .then(pl.lit("МТ"))
        .otherwise(pl.lit(""))
        .alias("Transmission"),
        pl.when(pl.col("Gas").fill_null(False))
        .then(pl.lit("ГБО"))
        .otherwise(pl.lit(""))
        .alias("GBO"),
        pl.when(
            pl.col("STSNumber").is_not_null()
            & (pl.col("STSNumber").str.len_chars() > 0)
        )
        .then(
            pl.col("STSSeries").fill_null("")
            + pl.col("STSNumber").cast(pl.Utf8)
        )
        .otherwise(pl.lit(""))
        .alias("STSSeriesNumber"),
        pl.when(
            pl.col("OSAGONumber").is_not_null()
            & (pl.col("OSAGONumber").str.len_chars() > 0)
        )
        .then(
            pl.col("OSAGOSeries").fill_null("")
            + " "
            + pl.col("OSAGONumber").cast(pl.Utf8)
        )
        .otherwise(pl.lit(""))
        .alias("OSAGOSeriesNumber"),
    )
    all_cols = df.columns
    for name, fn in [
        ("STSDetail", fmt.format_sts_detail),
        ("OSAGODetail", fmt.format_osago_detail),
        ("TODetail", fmt.format_dc_detail),
        ("LicenseDetail", fmt.format_license_detail),
        ("CarInfo", fmt.format_car_info),
        ("StatusDetail", fmt.format_status_detail),
        ("FormattedCommentCar", fmt.format_comment_car),
        ("CarLocation", fmt.get_car_location),
    ]:
        df = df.with_columns(
            pl.struct(all_cols)
            .map_elements(
                lambda s, _fn=fn: _fn(_row_to_dict(s)),
                return_dtype=pl.Utf8,
            )
            .alias(name),
        )
        all_cols = df.columns
    return df


def _add_roster_computed_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Добавляет вычисляемые колонки: MileageGroup, RegionFix, StatusFix, SubStatusFix."""
    mileage_km = pl.col("MileAge").fill_null(0) / 1000
    mileage_group = (
        pl.when(mileage_km < 50)
        .then(pl.lit("0-49"))
        .when(mileage_km < 100)
        .then(pl.lit("50-99"))
        .when(mileage_km < 200)
        .then(pl.lit("100-199"))
        .when(mileage_km < 300)
        .then(pl.lit("200-299"))
        .when(mileage_km < 400)
        .then(pl.lit("300-399"))
        .when(mileage_km < 500)
        .then(pl.lit("400-499"))
        .otherwise(pl.lit("500+"))
        .alias("MileageGroup")
    )
    region_fix = (
        pl.when(
            (pl.col("Region") == "Ярославль")
            & (pl.col("Model") == "LADA VESTA")
        )
        .then(pl.lit("Кострома"))
        .otherwise(pl.col("Region").fill_null(""))
        .alias("RegionFix")
    )
    status_fix = (
        pl.when(pl.col("Status") == "ДТП")
        .then(pl.lit("ТР"))
        .otherwise(pl.col("Status").fill_null(""))
        .alias("StatusFix")
    )
    substatus_fix = (
        pl.when(pl.col("Status") == "ДТП")
        .then(pl.lit("ДТП"))
        .otherwise(pl.col("SubStatus").fill_null(""))
        .alias("SubStatusFix")
    )
    return df.with_columns(
        [mileage_group, region_fix, status_fix, substatus_fix]
    )


def run_drivers_and_cars(client: SheetsClient | None = None) -> None:
    """Один проход: водители + автопарк в основную таблицу."""
    sheets = client or SheetsClient()
    element = ElementClient(USER, PASSWORD)
    date_upload = datetime.now(pytz.timezone("Europe/Moscow")).strftime(
        "%d.%m.%Y %H:%M:%S"
    )

    active_drivers = element.fetch_active_drivers(
        url=DRIVERS_URL, conditions_exclude=EXCLUDE_ROSTER
    )
    active_drivers = active_drivers.sort(["CarDepartment", "Car", "DatePL"])
    active_drivers = _add_driver_formatted_columns(active_drivers)

    formatted_drivers = active_drivers.select(
        [c for c in _DRIVERS_COLUMNS if c in active_drivers.columns]
    ).sort("FIO")
    formatted_drivers = formatted_drivers.with_columns(
        pl.lit(date_upload).alias("DateUpload")
    )
    drivers_data = [list(row) for row in formatted_drivers.iter_rows()]
    sheets.batch_update_values(
        REPORT_ID, RANGE_FOR_UPLOAD_DRIVERS, drivers_data
    )

    drivers_sub = active_drivers.select(
        [
            "FIO",
            "PhoneNumber",
            "Balance",
            "DatePL",
            "Car",
            "NameConditionWork",
        ]
    )
    drivers_sub = drivers_sub.with_columns(
        pl.struct(drivers_sub.columns)
        .map_elements(
            lambda s: fmt.format_driver_info(_row_to_dict(s)),
            return_dtype=pl.Utf8,
        )
        .alias("DriverInfo"),
    )
    drivers_sub = drivers_sub.sort(["Car", "DatePL"])
    driver_info_agg = (
        drivers_sub.group_by("Car")
        .agg(pl.col("DriverInfo").unique().alias("DriverInfo"))
        .with_columns(
            pl.col("DriverInfo").list.join("\n\n").alias("DriverInfo")
        )
    )

    active_cars = element.fetch_active_cars(CARS_URL)
    if "DatePL" in active_cars.columns:
        active_cars = active_cars.drop("DatePL")
    active_cars = _add_car_formatted_columns(active_cars)
    active_cars = _add_roster_computed_columns(active_cars)
    active_cars = active_cars.with_columns(
        pl.lit(date_upload).alias("DateUpload")
    )

    merged = (
        active_cars.join(
            driver_info_agg, left_on="Number", right_on="Car", how="left"
        )
        .unique(subset=["VIN"], keep="first")
        .fill_null("")
        .sort(["Region", "Department", "Model", "VIN"])
    )
    merged = merged.with_columns(
        pl.col("DriverInfo")
        .map_elements(
            lambda s: fmt.extract_date_pl_from_driver_info(s or ""),
            return_dtype=pl.Utf8,
        )
        .alias("DatePL")
    )
    roster = merged.select([c for c in _ROSTER_COLUMNS if c in merged.columns])
    roster_data = [list(row) for row in roster.iter_rows()]
    sheets.batch_update_values(REPORT_ID, RANGE_FOR_UPLOAD, roster_data)


def run_drivers_and_cars_safe() -> bool:
    """Выполняет run_drivers_and_cars с перехватом ошибок. Возвращает True при успехе."""
    try:
        run_drivers_and_cars()
        return True
    except HttpError as e:
        logger.error("Ошибка Google Sheets: %s", e)
        return False
    except requests.exceptions.HTTPError as e:
        logger.error("Ошибка HTTP: %s", e)
        return False
    except requests.exceptions.ChunkedEncodingError as e:
        logger.error("Ошибка обработки пакета: %s", e)
        return False
    except requests.exceptions.Timeout as e:
        logger.error("Timeout: %s", e)
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error("Ошибка соединения: %s", e)
        return False
