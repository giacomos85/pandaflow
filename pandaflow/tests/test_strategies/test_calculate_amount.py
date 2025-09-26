import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from pandaflow.strategies.calculate_amount import (
    CalculateAmountStrategy,
    CalculateAmountRule,
)


@pytest.fixture
def strategy():
    return CalculateAmountStrategy()


def test_validate_rule_parses_correctly():
    rule_dict = {
        "field": "__amount__",
        "strategy": "calculate_amount",
        "formula": "__total__ - __refund__",
        "output_rule": "float_2dec",
    }
    rule = CalculateAmountRule(**rule_dict)
    assert isinstance(rule, CalculateAmountRule)
    assert rule.formula == "__total__ - __refund__"
    assert rule.output_rule == "float_2dec"
    assert rule.field == "__amount__"


def test_apply_calculates_and_formats_amount():
    strategy = CalculateAmountStrategy()
    df = pd.DataFrame({"__total__": [100.123, 200.456], "__refund__": [10.0, 20.0]})
    rule = {
        "field": "__amount__",
        "strategy": "calculate_amount",
        "formula": "__total__ - __refund__",
        "output_rule": "float_2dec",
    }
    result = strategy.apply(df, rule)
    expected = pd.DataFrame(
        {
            "__total__": [100.123, 200.456],
            "__refund__": [10.0, 20.0],
            "__amount__": ["90,12", "180,46"],
        }
    )
    assert_frame_equal(result, expected)


def test_apply_without_output_rule():
    strategy = CalculateAmountStrategy()
    df = pd.DataFrame({"__total__": [50, 75], "__refund__": [5, 10]})
    rule = {
        "strategy": "calculate_amount",
        "field": "__amount__",
        "formula": "__total__ - __refund__",
    }
    result = strategy.apply(df, rule)
    expected = pd.DataFrame(
        {"__total__": [50, 75], "__refund__": [5, 10], "__amount__": [45, 65]}
    )
    assert_frame_equal(result, expected)


def test_apply_with_invalid_formula_raises():
    strategy = CalculateAmountStrategy()
    df = pd.DataFrame({"__total__": [100], "__refund__": [10]})
    rules = {"formula": "__total__ + unknown_field", "output_rule": "float_2dec"}
    with pytest.raises(TypeError):
        strategy.apply(df, field="__amount__", rules=rules, output="float_2dec")


def test_validate_rule(strategy):
    rule_dict = {
        "field": "__amount__",
        "strategy": "calculate_amount",
        "formula": "__total__ - __refund__",
    }
    validated = strategy.validate_rule(rule_dict)
    assert validated
