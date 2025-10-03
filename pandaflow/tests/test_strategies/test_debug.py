import pandas as pd
import pytest
from pandaflow.strategies.debug import DebugStrategy


@pytest.fixture
def sample_df():
    return pd.DataFrame({"A": [1, 2, 3], "B": ["x", "y", "z"]})


def test_debug_strategy_prints_field_and_head(sample_df, capsys):
    transformation = {"strategy": "debug"}
    strategy = DebugStrategy(transformation)
    strategy.run(sample_df)

    captured = capsys.readouterr()
    assert "A" in captured.out and "B" in captured.out
    assert "1" in captured.out and "x" in captured.out


def test_debug_strategy_does_not_modify_df(sample_df):
    transformation = {"strategy": "debug"}
    strategy = DebugStrategy(transformation)
    original = sample_df.copy()
    strategy.run(sample_df)
    pd.testing.assert_frame_equal(sample_df, original)
