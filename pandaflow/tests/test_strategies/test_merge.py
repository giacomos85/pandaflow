import pandas as pd
import pytest
from pandaflow.strategies.merge import MergeStrategy


@pytest.fixture
def strategy():
    return MergeStrategy()


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "first": ["Alice", "Bob", "Charlie"],
            "last": ["Smith", "Jones", "Brown"],
            "role": ["Admin", "User", "Guest"],
            "note": ["NaN", "None", "nan"],
        }
    )


def test_merge_multiple_columns_with_separator(strategy, sample_df):
    rule = {"field": "__merged__", "source": ["first", "last"], "separator": " | "}
    result = strategy.apply(sample_df, rule)
    expected = ["Alice | Smith", "Bob | Jones", "Charlie | Brown"]
    assert result["__merged__"].tolist() == expected


def test_merge_with_replacement(strategy, sample_df):
    rule = {
        "field": "__merged__",
        "source": ["first", "role"],
        "replace": {"from": "User", "to": "Member"},
        "separator": "-",
    }
    result = strategy.apply(sample_df, rule)
    expected = ["Alice-Admin", "Bob-Member", "Charlie-Guest"]
    assert result["__merged__"].tolist() == expected


def test_merge_with_nan_like_strings(strategy, sample_df):
    rule = {"field": "__merged__", "source": ["first", "note"], "separator": " "}
    result = strategy.apply(sample_df, rule)
    expected = ["Alice", "Bob", "Charlie"]
    assert result["__merged__"].tolist() == expected


def test_merge_with_source_as_string(strategy, sample_df):
    rule = {"field": "__merged__", "source": "first"}
    result = strategy.apply(sample_df, rule)
    assert result["__merged__"].tolist() == sample_df["first"].tolist()


def test_merge_uses_field_as_fallback_source(strategy, sample_df):
    rule = {"field": "first"}
    result = strategy.apply(sample_df, rule)
    assert result["first"].tolist() == sample_df["first"].tolist()


def test_merge_missing_column_raises(strategy, sample_df):
    rule = {"field": "__merged__", "source": ["first", "missing"]}
    with pytest.raises(ValueError, match="Column 'missing' not found"):
        strategy.apply(sample_df, rule)


def test_validate_rule(strategy):
    rule_dict = {
        "field": "__merged__",
        "strategy": "merge",
        "source": ["first", "last"],
        "separator": "-",
    }
    validated = strategy.validate_rule(rule_dict)
    assert validated.source == ["first", "last"]
    assert validated.separator == "-"
