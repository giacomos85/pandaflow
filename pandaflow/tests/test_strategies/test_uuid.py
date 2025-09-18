import pandas as pd
import pytest
from pandaflow.strategies.uuid import UUIDStrategy
from uuid_extension import uuid7


@pytest.fixture
def strategy():
    return UUIDStrategy()


@pytest.fixture
def sample_df():
    return pd.DataFrame({"name": ["Alice", "Bob", "Charlie"]})


def test_uuid_column_created(strategy, sample_df):
    rule = {"field": "__uuid__"}
    result = strategy.apply(sample_df, rule)
    assert "__uuid__" in result.columns
    assert len(result["__uuid__"]) == len(sample_df)


def test_uuid_values_are_unique(strategy, sample_df):
    rule = {"field": "__uuid__"}
    result = strategy.apply(sample_df, rule)
    uuids = result["__uuid__"].tolist()
    assert len(set(uuids)) == len(uuids)


def test_uuid_format(strategy, sample_df):
    rule = {"field": "__uuid__"}
    result = strategy.apply(sample_df, rule)
    for val in result["__uuid__"]:
        assert isinstance(val, str)
        assert len(val) >= 36  # UUIDv7 string length


def test_missing_field_key(strategy, sample_df):
    rule = {}  # No 'field' key
    result = strategy.apply(sample_df, rule)
    # Should create a column named None
    assert None in result.columns
    assert len(result[None]) == len(sample_df)


def test_validate_rule_(strategy):
    rule_dict = {
        "field": "text",
        "strategy": "replace",
    }
    validated = strategy.validate_rule(rule_dict)
    assert validated
