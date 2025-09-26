import pytest
import pandas as pd
from pandaflow.strategies.reorder_columns import ReorderColumnsStrategy


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "age": [30, 25],
            "email": ["a@example.com", "b@example.com"],
        }
    )


def test_reorder_columns_valid(sample_df):
    rule = {"strategy": "reorder_columns", "columns": ["email", "name", "age"]}
    result = ReorderColumnsStrategy(rule).apply(sample_df)
    assert result.columns.tolist() == ["email", "name", "age"]
    pd.testing.assert_frame_equal(result, sample_df[["email", "name", "age"]])


def test_reorder_columns_missing_column(sample_df):
    rule = {
        "strategy": "reorder_columns",
        "columns": ["email", "name", "gender"],  # 'gender' is missing
    }
    with pytest.raises(ValueError, match="Missing columns in DataFrame:"):
        ReorderColumnsStrategy(rule).apply(sample_df)


def test_reorder_columns_empty_rule(sample_df):
    rule = {"strategy": "reorder_columns", "columns": []}
    result = ReorderColumnsStrategy(rule).apply(sample_df)
    assert result.empty
    assert result.shape[1] == 0


def test_reorder_columns_partial_order(sample_df):
    rule = {"strategy": "reorder_columns", "columns": ["age", "name"]}
    result = ReorderColumnsStrategy(rule).apply(sample_df)
    assert result.columns.tolist() == ["age", "name"]
    pd.testing.assert_frame_equal(result, sample_df[["age", "name"]])
