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
    transformation = {
        "field": "description",
        "strategy": "filter",
        "formula": "amount > 0 and category == 'Sales'",
    }
    strategy = FilterByFormulaStrategy(transformation)
    result = strategy.run(sample_df)
    assert len(result) == 2
    assert result["description"].tolist() == ["A", "C"]


def test_non_boolean_formula_raises_error(sample_df):
    transformation = {
        "strategy": "filter",
        "field": "description",
        "formula": "amount + 100",
    }  # Not a boolean mask
    with pytest.raises(ValueError, match="Formula must evaluate to a boolean mask"):
        strategy = FilterByFormulaStrategy(transformation)
        strategy.run(sample_df)


def test_field_not_in_df_does_not_format(sample_df):
    transformation = {
        "strategy": "filter",
        "field": "nonexistent",
        "formula": "amount > 0",
    }
    strategy = FilterByFormulaStrategy(transformation)
    result = strategy.run(sample_df)
    assert "nonexistent" not in result.columns


def test_formula_with_multiple_conditions(sample_df):
    transformation = {
        "strategy": "filter",
        "field": "description",
        "formula": "(amount > 0) & (category == 'Sales')",
    }
    strategy = FilterByFormulaStrategy(transformation)
    result = strategy.run(sample_df)
    assert len(result) == 2
    assert result["description"].tolist() == ["A", "C"]
