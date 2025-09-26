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
    rule = {
        "strategy": "merge_formula",
        "field": "__full_name__",
        "formula": "first_name + ' ' + last_name",
    }
    strategy = MergeStringStrategy(rule)
    result = strategy.apply(sample_df)
    expected = ["Alice Smith", "Bob Jones", "Charlie Brown"]
    assert result["__full_name__"].tolist() == expected


def test_merge_with_source_columns(sample_df):
    rule = {
        "strategy": "merge_formula",
        "field": "__identity__",
        "source": ["first_name", "role"],
        "separator": " - ",
    }
    strategy = MergeStringStrategy(rule)
    result = strategy.apply(sample_df)
    expected = ["Alice - Admin", "Bob - User", "Charlie - Guest"]
    assert result["__identity__"].tolist() == expected


def test_missing_formula_and_source_raises(sample_df):
    rule = {
        "field": "__broken__",
        "strategy": "merge_formula",
    }
    with pytest.raises(
        ValueError, match="Either 'formula' or 'source' must be provided"
    ):
        strategy = MergeStringStrategy(rule)
        strategy.apply(sample_df)


def test_validate_rule():
    rule = {
        "field": "__full_name__",
        "strategy": "merge_formula",
        "formula": "first_name + ' ' + last_name",
        "output_rule": "custom_format",
    }
    strategy = MergeStringStrategy(rule)
    validated = strategy.validate_rule()
    assert validated.formula == rule["formula"]
    assert validated.output_rule == rule["output_rule"]


def test_merge_with_missing_column_in_formula(sample_df):
    rule = {
        "strategy": "merge_formula",
        "field": "__broken__",
        "formula": "first_name + ' ' + unknown_column",
    }
    with pytest.raises(NameError):
        strategy = MergeStringStrategy(rule)
        strategy.apply(sample_df)
