import pandas as pd
import pytest
from pandaflow.strategies.debug import DebugStrategy


@pytest.fixture
def strategy():
    return DebugStrategy()


@pytest.fixture
def sample_df():
    return pd.DataFrame({"A": [1, 2, 3], "B": ["x", "y", "z"]})


def test_debug_strategy_prints_field_and_head(strategy, sample_df, capsys):
    rule = {"field": "A"}
    strategy.apply(sample_df, rule)

    captured = capsys.readouterr()
    assert "Debugging field: A" in captured.out
    assert "A" in captured.out and "B" in captured.out
    assert "1" in captured.out and "x" in captured.out


def test_debug_strategy_does_not_modify_df(strategy, sample_df):
    rule = {"field": "A"}
    original = sample_df.copy()
    strategy.apply(sample_df, rule)
    pd.testing.assert_frame_equal(sample_df, original)
