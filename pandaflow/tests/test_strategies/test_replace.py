import pandas as pd
import pytest
from pandaflow.strategies.replace import ReplaceStrategy


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "text": ["hello world", "world peace", "peaceful world"],
            "numeric": [100, 200, 100],
        }
    )


def test_replace_string_occurrence(sample_df):
    transformation = {
        "field": "text",
        "strategy": "replace",
        "find": "world",
        "replace": "planet",
    }
    strategy = ReplaceStrategy(transformation)
    result = strategy.run(sample_df)
    expected = ["hello planet", "planet peace", "peaceful planet"]
    assert result["text"].tolist() == expected


def test_replace_numeric_occurrence(sample_df):
    transformation = {
        "field": "numeric",
        "strategy": "replace",
        "find": "100",
        "replace": "999",
    }
    strategy = ReplaceStrategy(transformation)
    result = strategy.run(sample_df)
    expected = ["999", "200", "999"]
    assert result["numeric"].tolist() == expected


def test_missing_column_raises(sample_df):
    transformation = {
        "field": "missing",
        "strategy": "replace",
        "find": "x",
        "replace": "y",
    }
    strategy = ReplaceStrategy(transformation)
    with pytest.raises(ValueError, match="Column 'missing' not found"):
        strategy.run(sample_df)


def test_replace_with_empty_from(sample_df):
    transformation = {
        "field": "text",
        "strategy": "replace",
        "find": "",
        "replace": "-",
    }
    strategy = ReplaceStrategy(transformation)
    result = strategy.run(sample_df)
    # Every character gets a "-" inserted before it
    assert result["text"].str.startswith("-").all()
