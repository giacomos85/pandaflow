import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from pandaflow.strategies.calculate_amount import CalculateAmountStrategy


def test_apply_calculates_and_formats_amount():
    df = pd.DataFrame({"__total__": [100.123, 200.456], "__refund__": [10.0, 20.0]})
    rule = {
        "field": "__amount__",
        "strategy": "calculate_amount",
        "formula": "__total__ - __refund__",
        "output_rule": "float_2dec",
    }
    strategy = CalculateAmountStrategy(rule)
    result = strategy.apply(df)
    expected = pd.DataFrame(
        {
            "__total__": [100.123, 200.456],
            "__refund__": [10.0, 20.0],
            "__amount__": ["90,12", "180,46"],
        }
    )
    assert_frame_equal(result, expected)


def test_apply_without_output_rule():
    df = pd.DataFrame({"__total__": [50, 75], "__refund__": [5, 10]})
    rule = {
        "strategy": "calculate_amount",
        "field": "__amount__",
        "formula": "__total__ - __refund__",
    }
    strategy = CalculateAmountStrategy(rule)
    result = strategy.apply(df)
    expected = pd.DataFrame(
        {"__total__": [50, 75], "__refund__": [5, 10], "__amount__": [45, 65]}
    )
    assert_frame_equal(result, expected)


def test_apply_with_invalid_formula_raises():
    df = pd.DataFrame({"__total__": [100], "__refund__": [10]})
    rule = {
        "strategy": "calculate_amount",
        "field": "__amount__",
        "formula": "__total__ + unknown_field",
        "output_rule": "float_2dec",
    }
    strategy = CalculateAmountStrategy(rule)
    with pytest.raises(pd.errors.UndefinedVariableError):
        strategy.apply(df)


def test_validate_rule():
    rule_dict = {
        "field": "__amount__",
        "strategy": "calculate_amount",
        "formula": "__total__ - __refund__",
    }
    strategy = CalculateAmountStrategy(rule_dict)
    validated = strategy.validate_rule()
    assert validated
