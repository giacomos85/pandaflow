import pandas as pd
import pytest
from pandaflow.strategies.drop import DropStrategy


@pytest.fixture
def strategy():
    return DropStrategy()


@pytest.fixture
def sample_df():
    return pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]})


def test_drop_single_column(strategy, sample_df):
    rule = {"field": "B", "strategy": "drop"}
    result = strategy.apply(sample_df, rule)
    assert "B" not in result.columns
    assert list(result.columns) == ["A", "C"]


def test_drop_multiple_columns(strategy, sample_df):
    rule = {"field": ["A", "C"], "strategy": "drop"}
    result = strategy.apply(sample_df, rule)
    assert list(result.columns) == ["B"]


def test_drop_missing_column_raises(strategy, sample_df):
    rule = {"field": "Z", "strategy": "drop"}
    with pytest.raises(KeyError):
        strategy.apply(sample_df, rule)


def test_validate_rule(strategy):
    rule_dict = {"field": ["A", "B"], "strategy": "drop"}
    validated = strategy.validate_rule(rule_dict)
    assert validated
