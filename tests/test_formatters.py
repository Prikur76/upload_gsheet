"""Smoke-тесты для форматтеров водителей и машин."""

from upload_gsheet.formatters.drivers_cars import (
    extract_date_pl_from_driver_info,
    format_car_info,
    format_comment_car,
    format_date_string,
    format_dc_detail,
    format_driver_info,
    format_driver_license,
    format_driver_phones,
    format_passport_info,
    format_status_detail,
    get_car_location,
    remove_chars,
)


class TestRemoveChars:
    def test_keeps_letters_and_digits(self):
        assert remove_chars("+7(906)123-45-64") == " 7 906 123 45 64"

    def test_replaces_special_chars_with_spaces(self):
        assert remove_chars("abc#@$def") == "abc def"


class TestFormatDateString:
    def test_iso_date(self):
        assert format_date_string("2024-01-15T12:30:00") == "15.01.2024"

    def test_date_only(self):
        assert format_date_string("2024-01-15") == "15.01.2024"

    def test_empty_date(self):
        assert format_date_string("") == ""

    def test_zero_date(self):
        assert format_date_string("0001-01-01T00:00:00") == ""

    def test_none_date(self):
        assert format_date_string(None) == ""  # type: ignore[arg-type]


class TestFormatDriverPhones:
    def test_main_only(self):
        row = {"PhoneNumber": "+79061234564", "PhoneNumber2": None}
        result = format_driver_phones(row)
        assert "осн.:" in result
        assert "9061234564" in result

    def test_main_and_add(self):
        row = {
            "PhoneNumber": "+79061234564",
            "PhoneNumber2": "+79069876543",
        }
        result = format_driver_phones(row)
        assert "осн.:" in result
        assert "доп.:" in result

    def test_empty_phones(self):
        row = {"PhoneNumber": "", "PhoneNumber2": None}
        assert format_driver_phones(row) == ""


class TestFormatPassportInfo:
    def test_full(self):
        row = {
            "PassportSerialNumber": "4510 123456",
            "PassportIssueDate": "2020-05-15T00:00:00",
            "PassportDepartmentName": "ОВД МОСКВЫ",
        }
        result = format_passport_info(row)
        assert "4510 123456" in result
        assert "15.05.2020" in result
        assert "ОВД МОСКВЫ" in result

    def test_no_serial(self):
        assert format_passport_info({"PassportSerialNumber": ""}) == ""


class TestFormatDriverLicense:
    def test_full(self):
        row = {
            "DriversLicenseSerialNumber": "7712 123456",
            "DriversLicenseIssueDate": "2018-03-10T00:00:00",
            "DriversLicenseExpiryDate": "2028-03-10T00:00:00",
            "DriversLicenseExperienceTotalSince": "2010-01-01T00:00:00",
        }
        result = format_driver_license(row)
        assert "ВУ 7712 123456" in result
        assert "выдано 10.03.2018" in result
        assert "действует до 10.03.2028" in result
        assert "стаж c" in result

    def test_no_serial(self):
        assert format_driver_license({}) == ""


class TestFormatDriverInfo:
    def test_full(self):
        row = {
            "FIO": "Иванов Иван Иванович",
            "PhoneNumber": "+79061234564",
            "Balance": 1500.5,
            "NameConditionWork": "Штатный",
            "DatePL": "2024-06-20",
        }
        result = format_driver_info(row)
        assert "Иванов Иван Иванович" in result
        assert "тел.: +79061234564" in result
        assert "баланс: 1500,5 руб." in result
        assert "усл.: Штатный" in result
        assert "контроль: 20.06.2024" in result

    def test_zero_balance(self):
        row = {
            "FIO": "Петров",
            "PhoneNumber": "",
            "Balance": 0.0,
            "NameConditionWork": "",
            "DatePL": "2024-01-01",
        }
        result = format_driver_info(row)
        assert "0 руб." in result

    def test_no_date(self):
        row = {
            "FIO": "Сидоров",
            "PhoneNumber": "",
            "Balance": 0,
            "NameConditionWork": "",
            "DatePL": "0001-01-01T00:00:00",
        }
        result = format_driver_info(row)
        assert "нет даты" in result

    def test_no_fio(self):
        assert format_driver_info({}) == ""


class TestFormatCarInfo:
    def test_full(self):
        row = {
            "Model": "Hyundai Solaris",
            "YearCar": "2020",
            "VIN": "XW1234567890",
            "Number": "ТУ28377",
            "EngineCapacity": 1600,
            "Transmission": "АТ",
            "GBO": "ГБО",
        }
        result = format_car_info(row)
        assert "Hyundai Solaris (2020)" in result
        assert "vin: XW1234567890" in result
        assert "гнз: ТУ28377" in result
        assert "1.6" in result  # 1600 / 1000
        assert "АТ" in result
        assert "ГБО" in result


class TestFormatStatusDetail:
    def test_with_reason(self):
        row = {"Status": "ДТП", "Reason": "Авария"}
        result = format_status_detail(row)
        assert "ДТП" in result
        assert "причина: Авария" in result

    def test_no_reason(self):
        result = format_status_detail({"Status": "Работает"})
        assert result == "Работает"

    def test_empty(self):
        assert format_status_detail({}) == ""


class TestFormatDCDetail:
    def test_full(self):
        row = {
            "TOSeriesNumber": "123456",
            "TOIssueDate": "2024-01-15T00:00:00",
        }
        result = format_dc_detail(row)
        assert "ДК 123456" in result
        assert "15.01.2024" in result

    def test_no_series(self):
        assert format_dc_detail({}) == ""


class TestExtractDatePL:
    def test_single_driver(self):
        info = (
            "Иванов Иван\n"
            "тел.: +79061234564\n"
            "баланс: 0 руб.\n"
            "усл.: Штатный\n"
            "контроль: 20.06.2024"
        )
        assert extract_date_pl_from_driver_info(info) == "20.06.2024"

    def test_no_date(self):
        info = "контроль: нет даты"
        assert extract_date_pl_from_driver_info(info) == ""

    def test_empty(self):
        assert extract_date_pl_from_driver_info("") == ""

    def test_max_date(self):
        info = (
            "Водитель1\nконтроль: 15.06.2024\n\n"
            "Водитель2\nконтроль: 20.06.2024"
        )
        assert extract_date_pl_from_driver_info(info) == "20.06.2024"


class TestGetCarLocation:
    def test_location_found(self):
        row = {"CommentCar": "||ЛОКАЦИЯ: МОСКВА||"}
        assert get_car_location(row) == "МОСКВА"

    def test_no_location(self):
        row = {"CommentCar": "||ДРУГОЕ: текст||"}
        assert get_car_location(row) == ""

    def test_empty(self):
        assert get_car_location({}) == ""


class TestFormatCommentCar:
    def test_multiple_parts(self):
        row = {"CommentCar": "часть1||часть2||часть3"}
        result = format_comment_car(row)
        assert result == "часть1\nчасть2\nчасть3"

    def test_empty_parts_skipped(self):
        row = {"CommentCar": "||часть1||||часть2||"}
        result = format_comment_car(row)
        assert result == "часть1\nчасть2"

    def test_empty(self):
        assert format_comment_car({}) == ""
