import pandas as pd
import pytest
from pandaflow.strategies.merge import MergeStrategy


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


def test_merge_multiple_columns_with_separator(sample_df):
    rule = {
        "strategy": "merge",
        "field": "__merged__",
        "source": ["first", "last"],
        "separator": " | ",
    }
    strategy = MergeStrategy(rule)
    result = strategy.run(sample_df)
    expected = ["Alice | Smith", "Bob | Jones", "Charlie | Brown"]
    assert result["__merged__"].tolist() == expected


def test_merge_with_nan_like_strings(sample_df):
    rule = {
        "strategy": "merge",
        "field": "__merged__",
        "source": ["first", "note"],
        "separator": " ",
    }
    strategy = MergeStrategy(rule)
    result = strategy.run(sample_df)
    expected = ["Alice", "Bob", "Charlie"]
    assert result["__merged__"].tolist() == expected


def test_merge_with_source_as_string(sample_df):
    rule = {"strategy": "merge", "field": "__merged__", "source": "first"}
    strategy = MergeStrategy(rule)
    result = strategy.run(sample_df)
    assert result["__merged__"].tolist() == sample_df["first"].tolist()


def test_merge_missing_column_raises(sample_df):
    rule = {"strategy": "merge", "field": "__merged__", "source": ["first", "missing"]}
    with pytest.raises(ValueError, match="Column 'missing' not found"):
        strategy = MergeStrategy(rule)
        strategy.run(sample_df)


def test_validate_rule():
    rule = {
        "field": "__merged__",
        "strategy": "merge",
        "source": ["first", "last"],
        "separator": "-",
    }
    strategy = MergeStrategy(rule)
    validated = strategy.validate_rule()
    assert validated.source == ["first", "last"]
    assert validated.separator == "-"
