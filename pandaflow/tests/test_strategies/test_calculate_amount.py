import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from pandaflow.strategies.calculate_amount import CalculateAmountStrategy


def test_apply_calculates_and_formats_amount():
    df = pd.DataFrame({"__total__": [100.123, 200.456], "__refund__": [10.0, 20.0]})
    transformation = {
        "field": "__amount__",
        "strategy": "calculate_amount",
        "formula": "__total__ - __refund__",
        "formatter": "float_2dec",
    }
    strategy = CalculateAmountStrategy(transformation)
    result = strategy.run(df)
    expected = pd.DataFrame(
        {
            "__total__": [100.123, 200.456],
            "__refund__": [10.0, 20.0],
            "__amount__": ["90,12", "180,46"],
        }
    )
    assert_frame_equal(result, expected)


def test_apply_without_formatter():
    df = pd.DataFrame({"__total__": [50, 75], "__refund__": [5, 10]})
    transformation = {
        "strategy": "calculate_amount",
        "field": "__amount__",
        "formula": "__total__ - __refund__",
    }
    strategy = CalculateAmountStrategy(transformation)
    result = strategy.run(df)
    expected = pd.DataFrame(
        {"__total__": [50, 75], "__refund__": [5, 10], "__amount__": [45, 65]}
    )
    assert_frame_equal(result, expected)


def test_apply_with_invalid_formula_raises():
    df = pd.DataFrame({"__total__": [100], "__refund__": [10]})
    transformation = {
        "strategy": "calculate_amount",
        "field": "__amount__",
        "formula": "__total__ + unknown_field",
        "formatter": "float_2dec",
    }
    strategy = CalculateAmountStrategy(transformation)
    with pytest.raises(pd.errors.UndefinedVariableError):
        strategy.run(df)
