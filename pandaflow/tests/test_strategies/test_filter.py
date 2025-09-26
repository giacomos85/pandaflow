import pandas as pd
import pytest
from pandaflow.strategies.filter import FilterByFormulaStrategy


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "amount": [100, -50, 200],
            "category": ["Sales", "Refund", "Sales"],
            "description": ["A", "B", "C"],
        }
    )


def test_valid_formula_filters_rows(sample_df):
    rule = {
        "field": "description",
        "strategy": "filter",
        "formula": "amount > 0 and category == 'Sales'",
    }
    strategy = FilterByFormulaStrategy(rule)
    result = strategy.apply(sample_df)
    assert len(result) == 2
    assert result["description"].tolist() == ["A", "C"]


def test_non_boolean_formula_raises_error(sample_df):
    rule = {
        "strategy": "filter",
        "field": "description",
        "formula": "amount + 100",
    }  # Not a boolean mask
    with pytest.raises(ValueError, match="Formula must evaluate to a boolean mask"):
        strategy = FilterByFormulaStrategy(rule)
        strategy.apply(sample_df)


def test_field_not_in_df_does_not_format(sample_df):
    rule = {"strategy": "filter", "field": "nonexistent", "formula": "amount > 0"}
    strategy = FilterByFormulaStrategy(rule)
    result = strategy.apply(sample_df)
    assert "nonexistent" not in result.columns


def test_formula_with_multiple_conditions(sample_df):
    rule = {
        "strategy": "filter",
        "field": "description",
        "formula": "(amount > 0) & (category == 'Sales')",
    }
    strategy = FilterByFormulaStrategy(rule)
    result = strategy.apply(sample_df)
    assert len(result) == 2
    assert result["description"].tolist() == ["A", "C"]


def test_validate_rule():
    rule = {
        "strategy": "filter",
        "field": "description",
        "formula": "(amount > 0) & (category == 'Sales')",
    }
    strategy = FilterByFormulaStrategy(rule)
    validated = strategy.validate_rule()
    assert validated
