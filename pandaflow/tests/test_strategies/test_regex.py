import pandas as pd
import pytest
from pandaflow.strategies.regex import RegExStrategy


# Mock formatter
def mock_formatter(value):
    return f"formatted-{value}"


@pytest.fixture
def strategy_cls(monkeypatch):
    monkeypatch.setattr(
        "pandaflow.strategies.regex.get_output_formatter",
        lambda _: mock_formatter,
    )
    return RegExStrategy


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "raw": [
                "Order #12345 confirmed",
                "Order #67890 shipped",
                "Invalid format",
                "Order #00001 delivered",
            ]
        }
    )


def test_valid_regex_extraction(strategy_cls, sample_df):
    rule = {
        "strategy": "regex",
        "field": "__order_id__",
        "source": "raw",
        "regex": r"Order\s+#(\d+)",
        "group_id": 1,
        "formatter": "custom",
    }
    strategy = strategy_cls(rule)
    result = strategy.run(sample_df)
    expected = ["formatted-12345", "formatted-67890", "", "formatted-00001"]
    assert result["__order_id__"].tolist() == expected


def test_missing_source_column_raises(strategy_cls):
    df = pd.DataFrame({"other": ["text"]})
    rule = {
        "strategy": "regex",
        "field": "__out__",
        "source": "missing",
        "regex": r"(.*)",
        "group_id": 1,
    }
    strategy = strategy_cls(rule)
    with pytest.raises(ValueError, match="Columns 'missing' not found"):
        strategy.run(df)


def test_invalid_regex_returns_empty(strategy_cls):
    df = pd.DataFrame({"raw": ["Order #12345"]})
    rule = {
        "strategy": "regex",
        "field": "__out__",
        "source": "raw",
        "regex": r"Order\s+#([",  # Invalid regex
        "group_id": 1,
    }
    strategy = strategy_cls(rule)
    with pytest.raises(Exception):
        strategy.run(df)


def test_group_id_out_of_range_returns_none(strategy_cls):
    df = pd.DataFrame({"raw": ["Order #12345"]})
    rule = {
        "strategy": "regex",
        "field": "__out__",
        "source": "raw",
        "regex": r"Order\s+#(\d+)",
        "group_id": 2,  # Only one group exists
    }
    strategy = strategy_cls(rule)
    result = strategy.run(df)
    assert result["__out__"].tolist() == [""]


def test_validate_rule(strategy_cls):
    rule = {
        "field": "__order_id__",
        "strategy": "regex",
        "source": "raw",
        "regex": r"Order\s+#(\d+)",
        "group_id": 1,
        "formatter": "custom",
    }
    strategy = strategy_cls(rule)
    validated = strategy.validate_rule()
    assert validated.source == "raw"
    assert validated.regex == rule["regex"]
    assert validated.group_id == 1
    assert validated.formatter == "custom"
