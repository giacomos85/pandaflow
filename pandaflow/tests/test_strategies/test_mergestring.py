import pandas as pd
import pytest
from pandaflow.strategies.mergestring import MergeStringStrategy


@pytest.fixture
def strategy(monkeypatch):
    return MergeStringStrategy()


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "first_name": ["Alice", "Bob", "Charlie"],
            "last_name": ["Smith", "Jones", "Brown"],
            "role": ["Admin", "User", "Guest"],
        }
    )


def test_merge_with_formula(strategy, sample_df):
    rule = {"field": "__full_name__", "formula": "first_name + ' ' + last_name"}
    result = strategy.apply(sample_df, rule)
    expected = ["Alice Smith", "Bob Jones", "Charlie Brown"]
    assert result["__full_name__"].tolist() == expected


def test_merge_with_source_columns(strategy, sample_df):
    rule = {
        "field": "__identity__",
        "source": ["first_name", "role"],
        "separator": " - ",
    }
    result = strategy.apply(sample_df, rule)
    expected = ["Alice - Admin", "Bob - User", "Charlie - Guest"]
    assert result["__identity__"].tolist() == expected


def test_missing_formula_and_source_raises(strategy, sample_df):
    rule = {"field": "__broken__"}
    with pytest.raises(
        ValueError, match="Either 'formula' or 'source' must be provided"
    ):
        strategy.apply(sample_df, rule)


def test_validate_rule(strategy):
    rule_dict = {
        "field": "__full_name__",
        "strategy": "merge_formula",
        "formula": "first_name + ' ' + last_name",
        "output_rule": "custom_format",
    }
    validated = strategy.validate_rule(rule_dict)
    assert validated.formula == rule_dict["formula"]
    assert validated.output_rule == rule_dict["output_rule"]


def test_merge_with_missing_column_in_formula(strategy, sample_df):
    rule = {"field": "__broken__", "formula": "first_name + ' ' + unknown_column"}
    with pytest.raises(NameError):
        strategy.apply(sample_df, rule)
