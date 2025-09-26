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
    rule = {
        "field": "text",
        "strategy": "replace",
        "find": "world",
        "replace": "planet",
    }
    strategy = ReplaceStrategy(rule)
    result = strategy.apply(sample_df)
    expected = ["hello planet", "planet peace", "peaceful planet"]
    assert result["text"].tolist() == expected


def test_replace_numeric_occurrence(sample_df):
    rule = {"field": "numeric", "strategy": "replace", "find": "100", "replace": "999"}
    strategy = ReplaceStrategy(rule)
    result = strategy.apply(sample_df)
    expected = ["999", "200", "999"]
    assert result["numeric"].tolist() == expected


def test_missing_column_raises(sample_df):
    rule = {"field": "missing", "strategy": "replace", "find": "x", "replace": "y"}
    strategy = ReplaceStrategy(rule)
    with pytest.raises(ValueError, match="Column 'missing' not found"):
        strategy.apply(sample_df)


def test_replace_with_empty_from(sample_df):
    rule = {"field": "text", "strategy": "replace", "find": "", "replace": "-"}
    strategy = ReplaceStrategy(rule)
    result = strategy.apply(sample_df)
    # Every character gets a "-" inserted before it
    assert result["text"].str.startswith("-").all()


def test_validate_rule_maps_keys():
    rule = {
        "field": "text",
        "strategy": "replace",
        "find": "world",
        "replace": "planet",
    }
    strategy = ReplaceStrategy(rule)
    validated = strategy.validate_rule()
    assert validated.find == "world"
    assert validated.replace == "planet"
