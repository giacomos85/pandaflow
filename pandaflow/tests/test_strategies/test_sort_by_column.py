import pytest
import pandas as pd
from pandaflow.strategies.sort_by_column import SortByColumnStrategy


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie", "Dana"],
            "score": [85, 92, 85, None],
            "age": [30, 25, 40, 22],
        }
    )


def test_sort_single_column(sample_df):
    rule = {
        "strategy": "sort_by_column",
        "columns": ["age"],
    }
    result = SortByColumnStrategy().apply(sample_df, rule)
    assert result.age.tolist() == [22, 25, 30, 40]


def test_sort_multiple_columns(sample_df):
    rule = {
        "strategy": "sort_by_column",
        "columns": ["score", "age"],
        "ascending": [False, True],
    }
    result = SortByColumnStrategy().apply(sample_df, rule)
    assert result.name.tolist() == ["Bob", "Alice", "Charlie", "Dana"]


def test_sort_with_na_first(sample_df):
    rule = {
        "strategy": "sort_by_column",
        "columns": ["score"],
        "na_position": "first",
    }
    result = SortByColumnStrategy().apply(sample_df, rule)
    assert pd.isna(result.score.iloc[0])


def test_sort_with_mismatched_ascending(sample_df):
    rule = {
        "strategy": "sort_by_column",
        "columns": ["score", "age"],
        "ascending": [True],  # mismatch
    }
    with pytest.raises(ValueError, match="Length of 'ascending' must match 'columns'"):
        SortByColumnStrategy().apply(sample_df, rule)


def test_sort_with_missing_column(sample_df):
    rule = {
        "strategy": "sort_by_column",
        "columns": ["nonexistent"],
    }
    with pytest.raises(KeyError):
        SortByColumnStrategy().apply(sample_df, rule)


def test_sort_empty_columns(sample_df):
    rule = {
        "strategy": "sort_by_column",
        "columns": [],
    }
    result = SortByColumnStrategy().apply(sample_df, rule)
    pd.testing.assert_frame_equal(result, sample_df)


def test_sort_default_ascending(sample_df):
    rule = {
        "strategy": "sort_by_column",
        "columns": ["score", "age"],
    }
    result = SortByColumnStrategy().apply(sample_df, rule)
    assert result.name.tolist() == ["Alice", "Charlie", "Bob", "Dana"]
