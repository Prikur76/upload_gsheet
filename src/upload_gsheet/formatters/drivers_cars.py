"""Форматирование данных водителей и автомобилей (вход: dict-строка)."""

import re
import textwrap
from datetime import datetime
from typing import Any


def remove_chars(s: str) -> str:
    """Оставляет в строке только цифры, буквы и пробелы."""
    return re.sub(r"[^0-9a-zA-Zа-яА-Яё]+", " ", s)


def format_date_string(date_string: str, fmt: str = "%d.%m.%Y") -> str:
    """Форматирует дату из ISO-строки 1С или YYYY-MM-DD в заданный формат."""
    if not date_string or "0001-01-01" in str(date_string):
        return ""
    s = str(date_string).strip()[:19]
    try:
        if "T" in s:
            dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
        else:
            dt = datetime.strptime(s[:10], "%Y-%m-%d")
        return dt.strftime(fmt)
    except (ValueError, TypeError):
        return ""


def _clean_phone(val: Any) -> tuple[str | None, bool]:
    if val is None or (isinstance(val, float) and str(val) == "nan"):
        return None, False
    clean_data = re.sub(r"[^0-9]+", " ", str(val))
    phones = ", ".join(re.findall(r"([+7|8|7]+[0-9]{10})", clean_data))
    return (phones, True) if phones else (None, False)


def format_driver_phones(row: dict[str, Any]) -> str:
    """Строка с телефонами водителя (осн./доп.)."""
    main_phone, main_ok = _clean_phone(row.get("PhoneNumber"))
    add_phone, add_ok = _clean_phone(row.get("PhoneNumber2"))
    parts = []
    if main_ok:
        parts.append(f"осн.: {main_phone}")
    if add_ok:
        parts.append(f"доп.: {add_phone}")
    return "\n".join(parts)


def format_passport_info(row: dict[str, Any]) -> str:
    """Строка с данными паспорта."""
    num = row.get("PassportSerialNumber")
    if not num:
        return ""
    issue = format_date_string(str(row.get("PassportIssueDate", "")))
    dept = (row.get("PassportDepartmentName") or "").upper()
    return textwrap.dedent(f"паспорт {num} выдан {issue} {dept}").strip()


def format_driver_license(row: dict[str, Any]) -> str:
    """Строка с данными водительского удостоверения."""
    serial = row.get("DriversLicenseSerialNumber")
    if not serial:
        return ""
    issue = format_date_string(str(row.get("DriversLicenseIssueDate", "")))
    expiry = format_date_string(str(row.get("DriversLicenseExpiryDate", "")))
    lines = [f"ВУ {serial}", f"выдано {issue}", f"действует до {expiry}"]
    since = str(row.get("DriversLicenseExperienceTotalSince", ""))
    if since and "0001-01-01" not in since:
        lines.append(f"стаж c {format_date_string(since)}")
    return "\n".join(lines)


def format_driver_info(row: dict[str, Any]) -> str:
    """Строка с информацией о водителе для блока по машине."""
    fio = row.get("FIO")
    if not fio:
        return ""
    date_pl = str(row.get("DatePL", ""))
    pl_date = (
        "нет даты"
        if "0001-01-01" in date_pl
        else format_date_string(date_pl, "%d.%m.%Y")
    )
    balance = row.get("Balance")
    if balance is None or balance == 0.0:
        balance_str = "0 руб."
    else:
        balance_str = str(balance).replace(".", ",") + " руб."
    return (
        f"{fio}\n"
        f"тел.: {row.get('PhoneNumber', '')}\n"
        f"баланс: {balance_str}\n"
        f"усл.: {row.get('NameConditionWork', '')}\n"
        f"контроль: {pl_date}"
    )


def format_car_info(row: dict[str, Any]) -> str:
    """Строка с краткой информацией о машине."""
    model = row.get("Model", "")
    year = row.get("YearCar", "")
    vin = row.get("VIN", "")
    number = row.get("Number", "")
    cap = row.get("EngineCapacity")
    cap_str = f"{round(cap / 1000, 1)}" if cap else ""
    trans = row.get("Transmission", "")
    line = f"{model} ({year})\nvin: {vin}\nгнз: {number}\n{cap_str} {trans}"
    if row.get("GBO"):
        line += ", ГБО"
    return line


def format_status_detail(row: dict[str, Any]) -> str:
    """Детализация статуса машины."""
    status = row.get("Status", "")
    reason = row.get("Reason", "")
    if reason:
        return f"{status}\n\nпричина: {reason}"
    return status or ""


def format_dc_detail(row: dict[str, Any]) -> str:
    """Строка по диагностической карте."""
    series = row.get("TOSeriesNumber")
    if not series:
        return ""
    date_str = format_date_string(str(row.get("TOIssueDate", "")))
    return f"ДК {series} от {date_str}"


def format_osago_detail(row: dict[str, Any]) -> str:
    """Строка по ОСАГО."""
    series = row.get("OSAGOSeriesNumber")
    if not series:
        return ""
    date_str = format_date_string(str(row.get("OSAGOIssueDate", "")))
    return f"Полис ОСАГО\n{series}\nот {date_str}"


def format_license_detail(row: dict[str, Any]) -> str:
    """Строка по лицензии такси."""
    series = row.get("LicenseSeriesNumber")
    if not series:
        return ""
    date_str = format_date_string(str(row.get("LicenseIssueDate", "")))
    return f"Реестр N {series}\nот {date_str}"


def format_sts_detail(row: dict[str, Any]) -> str:
    """Строка по СТС."""
    series = row.get("STSSeriesNumber")
    if not series:
        return ""
    date_str = format_date_string(str(row.get("STSIssueDate", "")))
    return f"СТС {series}\nот {date_str}"


def get_car_location(row: dict[str, Any]) -> str:
    """Извлекает локацию из комментария машины."""
    comment = row.get("CommentCar") or ""
    parts = [p.upper().strip() for p in comment.split("||") if p]
    for p in parts:
        if "ЛОКАЦИЯ" in p:
            return p.replace("ЛОКАЦИЯ", "").replace(":", "").strip()
    return ""


def extract_date_pl_from_driver_info(driver_info: str) -> str:
    """Извлекает значение даты по ключу «контроль:» из текста DriverInfo.
    При нескольких водителях возвращает максимальную дату (DD.MM.YYYY).
    """
    if not driver_info:
        return ""
    pattern = re.compile(r"контроль:\s*([^\n]+)", re.IGNORECASE)
    matches = pattern.findall(driver_info)
    if not matches:
        return ""
    dates = []
    for m in matches:
        part = m.strip()
        if not part or part == "нет даты":
            continue
        try:
            dt = datetime.strptime(part, "%d.%m.%Y")
            dates.append(dt)
        except ValueError:
            continue
    if not dates:
        return ""
    return max(dates).strftime("%d.%m.%Y")


def format_comment_car(row: dict[str, Any]) -> str:
    """Комментарий по машине (все подстроки через ||)."""
    comment = row.get("CommentCar") or ""
    return "\n".join(p.strip() for p in comment.split("||") if p.strip())
