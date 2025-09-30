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
    transformation = {
        "strategy": "merge",
        "field": "__merged__",
        "source": ["first", "last"],
        "separator": " | ",
    }
    strategy = MergeStrategy(transformation)
    result = strategy.run(sample_df)
    expected = ["Alice | Smith", "Bob | Jones", "Charlie | Brown"]
    assert result["__merged__"].tolist() == expected


def test_merge_with_nan_like_strings(sample_df):
    transformation = {
        "strategy": "merge",
        "field": "__merged__",
        "source": ["first", "note"],
        "separator": " ",
    }
    strategy = MergeStrategy(transformation)
    result = strategy.run(sample_df)
    expected = ["Alice", "Bob", "Charlie"]
    assert result["__merged__"].tolist() == expected


def test_merge_with_source_as_string(sample_df):
    transformation = {"strategy": "merge", "field": "__merged__", "source": "first"}
    strategy = MergeStrategy(transformation)
    result = strategy.run(sample_df)
    assert result["__merged__"].tolist() == sample_df["first"].tolist()


def test_merge_missing_column_raises(sample_df):
    transformation = {"strategy": "merge", "field": "__merged__", "source": ["first", "missing"]}
    with pytest.raises(ValueError, match="Column 'missing' not found"):
        strategy = MergeStrategy(transformation)
        strategy.run(sample_df)
