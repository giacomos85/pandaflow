import pandas as pd
from pandas.testing import assert_frame_equal

from pandaflow.strategies.constant import ConstantStrategy, ConstantRule


def test_validate_rule_parses_correctly():
    rule_dict = {"field": "__source__", "strategy": "constant", "value": "amazon"}
    rule = ConstantRule(**rule_dict)
    assert isinstance(rule, ConstantRule)
    assert rule.value == "amazon"


def test_apply_sets_constant_value():
    strategy = ConstantStrategy()

    df = pd.DataFrame({"order_id": [1, 2, 3]})

    rule = {"field": "__source__", "strategy": "constant", "value": "amazon"}

    result = strategy.apply(df, rule)

    expected = pd.DataFrame(
        {"order_id": [1, 2, 3], "__source__": ["amazon", "amazon", "amazon"]}
    )

    assert_frame_equal(result, expected)


def test_apply_with_missing_value_defaults_to_empty():
    strategy = ConstantStrategy()

    df = pd.DataFrame({"order_id": [1, 2]})

    rule = {
        "field": "__source__",
        "strategy": "constant",
        # no "value" key
    }

    result = strategy.apply(df, rule)

    expected = pd.DataFrame({"order_id": [1, 2], "__source__": ["", ""]})

    assert_frame_equal(result, expected)


def test_run_method_sets_constant_value():
    strategy = ConstantStrategy()

    df = pd.DataFrame({"order_id": [101, 102]})

    rule = {"field": "__source__", "strategy": "constant", "value": "ebay"}

    result = strategy.run(df, rule)

    expected = pd.DataFrame({"order_id": [101, 102], "__source__": ["ebay", "ebay"]})

    assert_frame_equal(result, expected)
