from datetime import datetime
import locale


def parse_date(format, locale_value=None):
    def parser(value):
        if not value:
            return None
        try:
            if locale_value:
                locale.setlocale(locale.LC_TIME, locale_value)
            return datetime.strptime(value.strip(), format)
        except ValueError:
            return None

    return parser


def parse_float(currency_symbols, thousands_sep, decimal_sep, default_value):
    def parser(value):
        s = str(value).strip()
        for sym in currency_symbols:
            s = s.replace(sym, "")
        if thousands_sep:
            s = s.replace(thousands_sep, "")
        if decimal_sep and decimal_sep != ".":
            s = s.replace(decimal_sep, ".")
        try:
            return float(s) if s else float(default_value)
        except ValueError:
            return float(default_value)

    return parser


def formatter_float(decimals, decimals_sep, as_string, prefix, suffix):
    def format_float(val):
        try:
            val = float(val)
        except (ValueError, TypeError):
            val = 0.0
        if as_string:
            return f"{prefix}{val:.{decimals}f}{suffix}".replace(".", decimals_sep)
        return round(val, decimals)

    return format_float


def formatter_date(output_format, as_string: bool = False):
    def format_date(val: str | datetime):
        if not val:
            return ""
        if isinstance(val, str):
            val = datetime.fromisoformat(val)
        if as_string:
            return val.strftime(output_format)
        return val

    return format_date


def get_input_parser(name: str):
    parsers = {
        "italian_dashed_date": parse_date(format="%d-%m-%Y"),
        "italian_slashed_date": parse_date(format="%d/%m/%Y"),
        "italian_slashed_datetime": parse_date(format="%d/%m/%Y %H:%M"),
        "iso_dashed_date": parse_date(format="%Y-%m-%d"),
        "us_date": parse_date(format="%m/%d/%Y"),
        "eu_date": parse_date(format="%d/%m/%Y"),
        "default_currency": parse_float(
            currency_symbols=["€", "$"],
            thousands_sep=".",
            decimal_sep=",",
            default_value=0,
        ),
        "us_currency": parse_float(
            currency_symbols=["€", "$"],
            thousands_sep="",
            decimal_sep=".",
            default_value=0,
        ),
        "italian_pretty_date": parse_date(
            format="%d-%b-%Y", locale_value="it_IT.UTF-8"
        ),
    }
    if name is None:
        return lambda x: x
    if name not in parsers:
        ValueError(f"Parser '{name}' not found in predefined parsers or configuration")

    return parsers[name]


def get_output_formatter(name: str):
    formatters = {
        "float_2dec": formatter_float(
            decimals=2, decimals_sep=",", as_string=True, prefix="", suffix=""
        ),
        "float_4dec": formatter_float(
            decimals=4, decimals_sep=",", as_string=True, prefix="", suffix=""
        ),
        "us_float_4dec": formatter_float(
            decimals=4, decimals_sep=".", as_string=True, prefix="", suffix=""
        ),
        "iso_dashed_date": formatter_date(output_format="%Y-%m-%d", as_string=True),
        "italian_dashed_date": formatter_date(output_format="%d-%m-%Y", as_string=True),
        "str_upper": lambda x: str(x).upper(),
    }
    if name is None:
        return lambda x: x
    if name not in formatters:
        raise ValueError(
            f"Output formatter '{name}' not found in predefined formatters or configuration"
        )
    return formatters[name]
