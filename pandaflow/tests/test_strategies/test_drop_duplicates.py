import pytest
import pandas as pd
from pandaflow.strategies.drop_duplicates import DropDuplicatesStrategy


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Alice", "Charlie", "Bob"],
            "age": [30, 25, 30, 40, 25],
        }
    )


def test_drop_all_duplicates(sample_df):
    rule = {"strategy": "drop_duplicates"}
    result = DropDuplicatesStrategy(rule).apply(sample_df)
    assert len(result) == 3


def test_drop_subset_duplicates(sample_df):
    rule = {"strategy": "drop_duplicates", "subset": ["name", "age"]}
    result = DropDuplicatesStrategy(rule).apply(sample_df)
    assert len(result) == 3
    assert result["name"].tolist() == ["Alice", "Bob", "Charlie"]


def test_keep_last(sample_df):
    rule = {"strategy": "drop_duplicates", "subset": ["name", "age"], "keep": "last"}
    result = DropDuplicatesStrategy(rule).apply(sample_df)
    assert result["name"].tolist() == ["Alice", "Charlie", "Bob"]


# def test_keep_false(sample_df):
#     rule = {
#         "strategy": "drop_duplicates",
#         "subset": ["name", "age"],
#         "keep": False
#     }
#     result = DropDuplicatesStrategy().apply(sample_df, rule)
#     assert result["name"].tolist() == ["Charlie"]


def test_reset_index(sample_df):
    rule = {
        "strategy": "drop_duplicates",
        "subset": ["name", "age"],
        "reset_index": True,
    }
    result = DropDuplicatesStrategy(rule).apply(sample_df)
    assert result.index.tolist() == [0, 1, 2]
