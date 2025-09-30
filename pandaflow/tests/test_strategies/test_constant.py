import pandas as pd
from pandas.testing import assert_frame_equal

from pandaflow.strategies.constant import ConstantStrategy, ConstantTransformation


def test_apply_sets_constant_value():
    df = pd.DataFrame({"order_id": [1, 2, 3]})
    rule = {"field": "__source__", "strategy": "constant", "value": "amazon"}
    strategy = ConstantStrategy(rule)
    result = strategy.run(df)
    expected = pd.DataFrame(
        {"order_id": [1, 2, 3], "__source__": ["amazon", "amazon", "amazon"]}
    )
    assert_frame_equal(result, expected)


def test_apply_with_missing_value_defaults_to_empty():
    df = pd.DataFrame({"order_id": [1, 2]})
    rule = {
        "field": "__source__",
        "strategy": "constant",
        # no "value" key
    }
    strategy = ConstantStrategy(rule)
    result = strategy.run(df)
    expected = pd.DataFrame({"order_id": [1, 2], "__source__": ["", ""]})
    assert_frame_equal(result, expected)


def test_run_method_sets_constant_value():
    df = pd.DataFrame({"order_id": [101, 102]})
    rule = {"field": "__source__", "strategy": "constant", "value": "ebay"}
    strategy = ConstantStrategy(rule)
    result = strategy.run(df)
    expected = pd.DataFrame({"order_id": [101, 102], "__source__": ["ebay", "ebay"]})
    assert_frame_equal(result, expected)
