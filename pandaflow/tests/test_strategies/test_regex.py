import pandas as pd
import pytest
from pandaflow.strategies.regex import RegExStrategy


# Mock formatter
def mock_formatter(value):
    return f"formatted-{value}"


@pytest.fixture
def strategy(monkeypatch):
    monkeypatch.setattr(
        "pandaflow.strategies.regex.get_output_formatter",
        lambda _: mock_formatter,
    )
    return RegExStrategy()


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


def test_valid_regex_extraction(strategy, sample_df):
    rule = {
        "field": "__order_id__",
        "source": "raw",
        "regex": r"Order\s+#(\d+)",
        "group_id": 1,
        "output_rule": "custom",
    }
    result = strategy.apply(sample_df, rule)
    expected = ["formatted-12345", "formatted-67890", None, "formatted-00001"]
    assert result["__order_id__"].tolist() == expected


def test_missing_source_column_raises(strategy):
    df = pd.DataFrame({"other": ["text"]})
    rule = {"field": "__out__", "source": "missing", "regex": r"(.*)", "group_id": 1}
    with pytest.raises(ValueError, match="Columns 'missing' not found"):
        strategy.apply(df, rule)


def test_invalid_regex_returns_empty(strategy):
    df = pd.DataFrame({"raw": ["Order #12345"]})
    rule = {
        "field": "__out__",
        "source": "raw",
        "regex": r"Order\s+#([",  # Invalid regex
        "group_id": 1,
    }
    with pytest.raises(Exception):
        strategy.apply(df, rule)


def test_group_id_out_of_range_returns_none(strategy):
    df = pd.DataFrame({"raw": ["Order #12345"]})
    rule = {
        "field": "__out__",
        "source": "raw",
        "regex": r"Order\s+#(\d+)",
        "group_id": 2,  # Only one group exists
    }
    result = strategy.apply(df, rule)
    assert result["__out__"].tolist() == [""]


def test_validate_rule(strategy):
    rule_dict = {
        "field": "__order_id__",
        "strategy": "regex",
        "source": "raw",
        "regex": r"Order\s+#(\d+)",
        "group_id": 1,
        "output_rule": "custom",
    }
    validated = strategy.validate_rule(rule_dict)
    assert validated.source == "raw"
    assert validated.regex == rule_dict["regex"]
    assert validated.group_id == 1
    assert validated.output_rule == "custom"
