import pandas as pd
import pytest
from pandaflow.strategies.deduplicate import DeDuplicateStrategy


@pytest.fixture
def strategy():
    return DeDuplicateStrategy()


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "id": [1, 2, 2, 3, 3, 3],
            "name": ["Alice", "Bob", "Bob", "Charlie", "Charlie", "Charlie"],
            "value": [10, 20, 21, 30, 31, 32],
        }
    )


def test_deduplicate_by_field(strategy, sample_df):
    rule = {"field": "id"}
    result = strategy.apply(sample_df, rule)
    expected_ids = [1, 2, 3]  # Keeps last occurrence of each id
    assert result["id"].tolist() == expected_ids
    assert result["value"].tolist() == [10, 21, 32]


def test_deduplicate_by_subset(strategy, sample_df):
    rule = {"field": "id", "subset": ["id", "name"]}
    result = strategy.apply(sample_df, rule)
    expected = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [10, 21, 32]}
    )
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


def test_no_duplicates(strategy):
    df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    rule = {"field": "id"}
    result = strategy.apply(df, rule)
    pd.testing.assert_frame_equal(result, df)


def test_validate_rule(strategy):
    rule_dict = {"field": "id", "strategy": "deduplicate"}
    validated = strategy.validate_rule(rule_dict)
    assert validated.field == "id"
