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


def test_uuid_column_created(sample_df):
    rule = {"strategy": "uuid", "field": "__uuid__"}
    strategy = UUIDStrategy(rule)
    result = strategy.apply(sample_df)
    assert "__uuid__" in result.columns
    assert len(result["__uuid__"]) == len(sample_df)


def test_uuid_values_are_unique(sample_df):
    rule = {"strategy": "uuid", "field": "__uuid__"}
    strategy = UUIDStrategy(rule)
    result = strategy.apply(sample_df)
    uuids = result["__uuid__"].tolist()
    assert len(set(uuids)) == len(uuids)


def test_uuid_format(sample_df):
    rule = {"strategy": "uuid", "field": "__uuid__"}
    strategy = UUIDStrategy(rule)
    result = strategy.apply(sample_df)
    for val in result["__uuid__"]:
        assert isinstance(val, str)
        assert len(val) >= 36  # UUIDv7 string length


def test_validate_rule_():
    rule = {"field": "text", "strategy": "uuid"}
    strategy = UUIDStrategy(rule)
    validated = strategy.validate_rule()
    assert validated
