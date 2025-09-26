import pytest
import pandas as pd
from pandaflow.strategies.split_column import SplitColumnStrategy

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "full_name": ["Alice Smith", "Bob Jones", "Charlie", None]
    })

def test_split_basic(sample_df):
    rule = {
        "strategy": "split_column",
        "column": "full_name",
        "delimiter": " ",
        "prefix": "name"
    }
    result = SplitColumnStrategy().apply(sample_df, rule)
    assert "name_0" in result.columns
    assert "name_1" in result.columns
    assert result.loc[0, "name_0"] == "Alice"
    assert result.loc[0, "name_1"] == "Smith"

def test_split_with_maxsplit(sample_df):
    rule = {
        "strategy": "split_column",
        "column": "full_name",
        "delimiter": " ",
        "maxsplit": 1,
        "prefix": "part"
    }
    result = SplitColumnStrategy().apply(sample_df, rule)
    assert result.columns.tolist()[-2:] == ["part_0", "part_1"]
    assert result.loc[1, "part_0"] == "Bob"
    assert result.loc[1, "part_1"] == "Jones"

def test_split_drop_original(sample_df):
    rule = {
        "strategy": "split_column",
        "column": "full_name",
        "delimiter": " ",
        "drop_original": True
    }
    result = SplitColumnStrategy().apply(sample_df, rule)
    assert "full_name" not in result.columns

def test_split_column_missing(sample_df):
    rule = {
        "strategy": "split_column",
        "column": "nonexistent",
        "delimiter": " "
    }
    with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
        SplitColumnStrategy().apply(sample_df, rule)

def test_split_empty_string():
    df = pd.DataFrame({"text": ["", "a b", None]})
    rule = {
        "strategy": "split_column",
        "column": "text",
        "delimiter": " ",
        "prefix": "split"
    }
    result = SplitColumnStrategy().apply(df, rule)
    assert result.columns.tolist()[-2:] == ["split_0", "split_1"]

def test_split_custom_prefix(sample_df):
    rule = {
        "strategy": "split_column",
        "column": "full_name",
        "delimiter": " ",
        "prefix": "custom"
    }
    result = SplitColumnStrategy().apply(sample_df, rule)
    assert "custom_0" in result.columns
