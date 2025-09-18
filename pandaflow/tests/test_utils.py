import pytest
from datetime import datetime
from pandaflow.utils import (
    parse_date,
    parse_float,
    formatter_float,
    formatter_date,
    get_input_parser,
    get_output_formatter,
)


# ---------- parse_date ----------
def test_parse_date_valid():
    parser = parse_date("%Y-%m-%d")
    assert parser("2023-09-18") == datetime(2023, 9, 18)


def test_parse_date_invalid():
    parser = parse_date("%Y-%m-%d")
    assert parser("not-a-date") is None


def test_parse_date_empty():
    parser = parse_date("%Y-%m-%d")
    assert parser("") is None


def test_parse_date_with_locale(monkeypatch):
    monkeypatch.setattr("locale.setlocale", lambda *args, **kwargs: None)
    parser = parse_date("%d-%b-%Y", locale_value="it_IT.UTF-8")
    assert parser("18-Sep-2025") == datetime(2025, 9, 18)


# ---------- parse_float ----------
def test_parse_float_basic():
    parser = parse_float(["€", "$"], ".", ",", 0)
    assert parser("€1.234,56") == 1234.56


def test_parse_float_us_format():
    parser = parse_float(["$"], "", ".", 0)
    assert parser("$1234.56") == 1234.56


def test_parse_float_empty():
    parser = parse_float(["$"], "", ".", 42)
    assert parser("") == 42.0


def test_parse_float_invalid():
    parser = parse_float(["$"], "", ".", 99)
    assert parser("not-a-number") == 99.0


# ---------- formatter_float ----------
def test_formatter_float_as_string():
    fmt = formatter_float(2, ",", True, "€", "")
    assert fmt("1234.567") == "€1234,57"


def test_formatter_float_as_number():
    fmt = formatter_float(2, ".", False, "", "")
    assert fmt("1234.567") == 1234.57


def test_formatter_float_invalid_input():
    fmt = formatter_float(2, ",", True, "", "")
    assert fmt("invalid") == "0,00"


# ---------- formatter_date ----------
def test_formatter_date_string_input():
    fmt = formatter_date("%Y-%m-%d", True)
    assert fmt("2025-09-18") == "2025-09-18"


def test_formatter_date_datetime_input():
    fmt = formatter_date("%d-%m-%Y", True)
    assert fmt(datetime(2025, 9, 18)) == "18-09-2025"


def test_formatter_date_pass_through():
    fmt = formatter_date("%d-%m-%Y")
    assert fmt(datetime(2025, 9, 18)) == datetime(2025, 9, 18)


def test_formatter_date_invalid_string():
    fmt = formatter_date("%Y-%m-%d", True)
    with pytest.raises(ValueError):
        fmt("not-a-date")


def test_formatter_date_none():
    fmt = formatter_date("%Y-%m-%d", True)
    assert fmt(None) == ""


# ---------- get_input_parser ----------
def test_get_input_parser_known():
    parser = get_input_parser("iso_dashed_date")
    assert parser("2025-09-18") == datetime(2025, 9, 18)


def test_get_input_parser_none():
    parser = get_input_parser(None)
    assert parser("anything") == "anything"


def test_get_input_parser_unknown():
    with pytest.raises(KeyError):
        get_input_parser("unknown_parser")


# ---------- get_output_formatter ----------
def test_get_output_formatter_known():
    fmt = get_output_formatter("float_2dec")
    assert fmt("1234.5") == "1234,50"


def test_get_output_formatter_none():
    fmt = get_output_formatter(None)
    assert fmt("test") == "test"


def test_get_output_formatter_unknown():
    with pytest.raises(ValueError):
        get_output_formatter("unknown_formatter")
