import pandas as pd
import pytest
from pandaflow.strategies.mergestring import MergeStringStrategy


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "first_name": ["Alice", "Bob", "Charlie"],
            "last_name": ["Smith", "Jones", "Brown"],
            "role": ["Admin", "User", "Guest"],
        }
    )


def test_merge_with_formula(sample_df):
    transformation = {
        "strategy": "merge_formula",
        "field": "__full_name__",
        "formula": "first_name + ' ' + last_name",
    }
    strategy = MergeStringStrategy(transformation)
    result = strategy.run(sample_df)
    expected = ["Alice Smith", "Bob Jones", "Charlie Brown"]
    assert result["__full_name__"].tolist() == expected


def test_merge_with_source_columns(sample_df):
    transformation = {
        "strategy": "merge_formula",
        "field": "__identity__",
        "source": ["first_name", "role"],
        "separator": " - ",
    }
    strategy = MergeStringStrategy(transformation)
    result = strategy.run(sample_df)
    expected = ["Alice - Admin", "Bob - User", "Charlie - Guest"]
    assert result["__identity__"].tolist() == expected


def test_missing_formula_and_source_raises(sample_df):
    transformation = {
        "field": "__broken__",
        "strategy": "merge_formula",
    }
    with pytest.raises(
        ValueError, match="Either 'formula' or 'source' must be provided"
    ):
        strategy = MergeStringStrategy(transformation)
        strategy.run(sample_df)


def test_merge_with_missing_column_in_formula(sample_df):
    transformation = {
        "strategy": "merge_formula",
        "field": "__broken__",
        "formula": "first_name + ' ' + unknown_column",
    }
    with pytest.raises(NameError):
        strategy = MergeStringStrategy(transformation)
        strategy.run(sample_df)
