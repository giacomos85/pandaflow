import pandas as pd
import pytest
from pandaflow.strategies.filter import FilterByFormulaStrategy

# # Mock output formatter
# def mock_formatter(value):
#     return f"formatted-{value}"


@pytest.fixture
def strategy(monkeypatch):
    # monkeypatch.setattr(
    #     "pandaflow.strategies.filter.FilterByFormulaStrategy.get_output_formatter",
    #     lambda self, _: mock_formatter
    # )
    return FilterByFormulaStrategy()


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "amount": [100, -50, 200],
            "category": ["Sales", "Refund", "Sales"],
            "description": ["A", "B", "C"],
        }
    )


def test_valid_formula_filters_rows(strategy, sample_df):
    rule = {
        "field": "description",
        "strategy": "filter",
        "formula": "amount > 0 and category == 'Sales'",
    }
    result = strategy.apply(sample_df, rule)
    assert len(result) == 2
    assert result["description"].tolist() == ["A", "C"]


def test_non_boolean_formula_raises_error(strategy, sample_df):
    rule = {"strategy": "filter","field": "description", "formula": "amount + 100"}  # Not a boolean mask
    with pytest.raises(ValueError, match="Formula must evaluate to a boolean mask"):
        strategy.apply(sample_df, rule)


def test_field_not_in_df_does_not_format(strategy, sample_df):
    rule = {"strategy": "filter","field": "nonexistent", "formula": "amount > 0"}
    result = strategy.apply(sample_df, rule)
    assert "nonexistent" not in result.columns


def test_formula_with_multiple_conditions(strategy, sample_df):
    rule = {
        "strategy": "filter",
        "field": "description", "formula": "(amount > 0) & (category == 'Sales')"}
    result = strategy.apply(sample_df, rule)
    assert len(result) == 2
    assert result["description"].tolist() == ["A", "C"]
