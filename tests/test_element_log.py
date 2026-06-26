"""Тесты логирования запросов к 1С."""

from upload_gsheet.api.element_log import describe_request, format_exc


def test_describe_request_without_password() -> None:
    desc = describe_request(
        "POST",
        "https://example/hs/Driver/v1/Get",
        user="robotapi",
        json_body={"Status": ["Работает"]},
        timeout=(30, 180),
    )
    assert "robotapi" in desc
    assert "secret" not in desc
    assert 'body={"Status": ["Работает"]}' in desc
    assert "timeout=connect:30s,read:180s" in desc


def test_format_exc_shows_cause() -> None:
    wrapped = (
        "HTTPSConnectionPool(host='1c.0nalog.com', port=1710): "
        "Max retries exceeded with url: /test (Caused by "
        "ConnectTimeoutError(..., "
        "'Connection to 1c.0nalog.com timed out. (connect timeout=30)'))"
    )
    assert (
        format_exc(ConnectionError(wrapped))
        == "Connection to 1c.0nalog.com timed out. (connect timeout=30)"
    )
