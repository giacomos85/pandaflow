import pandas as pd
import pytest
from pandaflow.strategies.find_duplicates import FindDuplicatesStrategy, FindDuplicatesTransformation


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": [1, 2, 2, 3, 4, 4, 4],
        "name": ["Alice", "Bob", "Bob", "Charlie", "Dave", "Dave", "Gordon"]
    })


def test_find_duplicates_default(sample_df):
    config_dict = {"strategy": "find_duplicates"}
    strategy = FindDuplicatesStrategy(config_dict)
    result = strategy.apply(sample_df)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 4  # 2x "Bob", 2x "Dave" → 4 total


def test_find_duplicates_subset(sample_df):
    config_dict = {"strategy":"find_duplicates", "subset":["id"]}
    strategy = FindDuplicatesStrategy(config_dict)
    result = strategy.apply(sample_df)

    assert all(result["id"].isin([2, 4]))
    assert len(result) == 5 # 2x "2", 3x "4" = 5 total


def test_find_duplicates_reset_index(sample_df):
    config_dict = {"strategy":"find_duplicates", "subset":["id"], "reset_index":True}
    strategy = FindDuplicatesStrategy(config_dict)
    result = strategy.apply(sample_df)

    assert result.index.equals(pd.RangeIndex(start=0, stop=len(result)))
