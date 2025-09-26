import pandas as pd
import pytest
from pandaflow.strategies.base import TransformationStrategy


# Minimal concrete subclass for testing
class DummyStrategy(TransformationStrategy):
    def validate_rule(self, rule_dict):
        if "field" not in rule_dict:
            raise ValueError("Missing 'field'")
        return rule_dict

    def apply(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        df_copy = df.copy()
        df_copy[self.config_dict["field"]] = "debug"
        return df_copy


def test_run_applies_strategy():
    df = pd.DataFrame({"A": [1, 2]})
    rule = {"field": "B"}
    strategy = DummyStrategy(rule)
    result = strategy.run(df)
    assert "B" in result.columns
    assert all(result["B"] == "debug")


def test_check_valid_rule():
    rule = {"field": "X"}
    strategy = DummyStrategy(rule)
    config = {}
    assert strategy.check(config, rule) is True


def test_check_invalid_rule_raises():
    rule = {}  # Missing 'field'
    config = {}
    strategy = DummyStrategy(rule)
    with pytest.raises(ValueError, match="Invalid rule configuration"):
        strategy.check(config, rule)


def test_validate_rule_not_implemented():
    class IncompleteStrategy(TransformationStrategy):
        pass

    strategy = IncompleteStrategy({})
    with pytest.raises(NotImplementedError, match="Must implement validate_rule"):
        strategy.validate_rule({})


def test_apply_not_implemented():
    class IncompleteStrategy(TransformationStrategy):
        def validate_rule(self, rule_dict):
            return rule_dict

    strategy = IncompleteStrategy({})
    df = pd.DataFrame({"A": [1]})
    with pytest.raises(NotImplementedError, match="Must implement apply method"):
        strategy.apply(df, {"field": "B"})
