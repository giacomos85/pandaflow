import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from pandaflow.strategies.copy import CopyStrategy, CopyRule


def test_validate_rule_parses_correctly():
    rule_dict = {
        "field": "__amount__",
        "strategy": "copy",
        "source": "Amount",
        "input_rule": "default_currency",
        "output_rule": "float_2dec",
        "fillna": 0.0,
    }
    rule = CopyRule(**rule_dict)
    assert isinstance(rule, CopyRule)
    assert rule.source == "Amount"
    assert rule.input_rule == "default_currency"
    assert rule.output_rule == "float_2dec"
    assert rule.fillna == 0.0


def test_apply_with_input_and_output_rules():
    df = pd.DataFrame({"Amount": ["€1.234,56", "€2.345,67"]})
    rule = {
        "field": "__amount__",
        "strategy": "copy",
        "source": "Amount",
        "input_rule": "default_currency",
        "output_rule": "float_2dec",
    }
    strategy = CopyStrategy(rule)
    result = strategy.apply(df)
    expected = pd.DataFrame(
        {"Amount": ["€1.234,56", "€2.345,67"], "__amount__": ["1234,56", "2345,67"]}
    )
    assert_frame_equal(result, expected)


def test_apply_with_fillna_replaces_empty_and_null():
    df = pd.DataFrame({"Amount": ["", None, "€1.000,00"]})
    rule = {
        "field": "__amount__",
        "strategy": "copy",
        "source": "Amount",
        "input_rule": "default_currency",
        "output_rule": "float_2dec",
        "fillna": "0.00",
    }
    strategy = CopyStrategy(rule)
    result = strategy.apply(df)
    expected = pd.DataFrame(
        {"Amount": ["", None, "€1.000,00"], "__amount__": ["0,00", "0,00", "1000,00"]}
    )
    assert_frame_equal(result, expected)


def test_apply_without_input_or_output_rules():
    df = pd.DataFrame({"Amount": [10, 20]})
    rule = {"field": "__amount__", "strategy": "copy", "source": "Amount"}
    strategy = CopyStrategy(rule)
    result = strategy.apply(df)
    expected = pd.DataFrame({"Amount": [10, 20], "__amount__": [10, 20]})
    assert_frame_equal(result, expected)


def test_apply_raises_if_source_column_missing():
    df = pd.DataFrame({"Price": [100]})
    rule = {"field": "__amount__", "strategy": "copy", "source": "Amount"}
    strategy = CopyStrategy(rule)
    with pytest.raises(ValueError, match="Column 'Amount' not found"):
        strategy.apply(df)


def test_validate_rule():
    rule = {"field": "__amount__", "strategy": "copy", "source": "Amount"}
    strategy = CopyStrategy(rule)
    validated = strategy.validate_rule()
    assert validated
