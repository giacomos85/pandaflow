import pytest
import pandas as pd
from pandaflow.strategies.drop_columns import DropColumnsStrategy


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "age": [30, 25],
            "gender": ["F", "M"],
            "country": ["IT", "FR"],
        }
    )


def test_drop_single_column(sample_df):
    rule = {"strategy": "drop_columns", "columns": ["gender"]}
    result = DropColumnsStrategy(rule).apply(sample_df)
    assert "gender" not in result.columns
    assert result.columns.tolist() == ["name", "age", "country"]


def test_drop_multiple_columns(sample_df):
    rule = {"strategy": "drop_columns", "columns": ["gender", "country"]}
    result = DropColumnsStrategy(rule).apply(sample_df)
    assert result.columns.tolist() == ["name", "age"]


def test_drop_missing_column_raise(sample_df):
    rule = {"strategy": "drop_columns", "columns": ["unknown"]}
    with pytest.raises(KeyError):
        DropColumnsStrategy(rule).apply(sample_df)


def test_drop_missing_column_ignore(sample_df):
    rule = {"strategy": "drop_columns", "columns": ["unknown"], "errors": "ignore"}
    result = DropColumnsStrategy(rule).apply(sample_df)
    assert result.equals(sample_df)


def test_drop_mixed_existing_and_missing(sample_df):
    rule = {
        "strategy": "drop_columns",
        "columns": ["age", "unknown"],
        "errors": "ignore",
    }
    result = DropColumnsStrategy(rule).apply(sample_df)
    assert "age" not in result.columns
    assert "unknown" not in result.columns
