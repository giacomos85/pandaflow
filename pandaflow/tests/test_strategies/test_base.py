import pandas as pd
import pytest
from pandaflow.strategies.base import TransformationStrategy


# Minimal concrete subclass for testing
class DummyStrategy(TransformationStrategy):

    def run(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
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


def test_apply_not_implemented():
    class IncompleteStrategy(TransformationStrategy):
        pass

    strategy = IncompleteStrategy({})
    df = pd.DataFrame({"A": [1]})
    with pytest.raises(NotImplementedError, match="Must implement apply method"):
        strategy.apply(df, {"field": "B"})
