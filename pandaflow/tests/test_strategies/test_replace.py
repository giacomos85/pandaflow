import pandas as pd
import pytest
from pandaflow.strategies.replace import ReplaceStrategy


@pytest.fixture
def strategy():
    return ReplaceStrategy()


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "text": ["hello world", "world peace", "peaceful world"],
            "numeric": [100, 200, 100],
        }
    )


def test_replace_string_occurrence(strategy, sample_df):
    rule = {
        "field": "text",
        "strategy": "replace",
        "find": "world",
        "replace": "planet",
    }
    result = strategy.apply(sample_df, rule)
    expected = ["hello planet", "planet peace", "peaceful planet"]
    assert result["text"].tolist() == expected


def test_replace_numeric_occurrence(strategy, sample_df):
    rule = {"field": "numeric", "strategy": "replace", "find": "100", "replace": "999"}
    result = strategy.apply(sample_df, rule)
    expected = ["999", "200", "999"]
    assert result["numeric"].tolist() == expected


def test_missing_column_raises(strategy, sample_df):
    rule = {"field": "missing", "strategy": "replace", "find": "x", "replace": "y"}
    with pytest.raises(ValueError, match="Column 'missing' not found"):
        strategy.apply(sample_df, rule)


def test_replace_with_empty_from(strategy, sample_df):
    rule = {"field": "text", "strategy": "replace", "find": "", "replace": "-"}
    result = strategy.apply(sample_df, rule)
    # Every character gets a "-" inserted before it
    assert result["text"].str.startswith("-").all()


def test_validate_rule_maps_keys(strategy):
    rule_dict = {
        "field": "text",
        "strategy": "replace",
        "find": "world",
        "replace": "planet",
    }
    validated = strategy.validate_rule(rule_dict)
    assert validated.find == "world"
    assert validated.replace == "planet"
