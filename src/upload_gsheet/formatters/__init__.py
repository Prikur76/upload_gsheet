"""Форматирование строк для водителей и автомобилей."""

from upload_gsheet.formatters.drivers_cars import (
    extract_date_pl_from_driver_info,
    format_car_info,
    format_comment_car,
    format_date_string,
    format_dc_detail,
    format_driver_info,
    format_driver_license,
    format_driver_phones,
    format_license_detail,
    format_osago_detail,
    format_passport_info,
    format_status_detail,
    format_sts_detail,
    get_car_location,
    remove_chars,
)

__all__ = [
    "extract_date_pl_from_driver_info",
    "format_car_info",
    "format_comment_car",
    "format_dc_detail",
    "format_driver_info",
    "format_driver_license",
    "format_driver_phones",
    "format_date_string",
    "format_license_detail",
    "format_osago_detail",
    "format_passport_info",
    "format_status_detail",
    "format_sts_detail",
    "get_car_location",
    "remove_chars",
]
