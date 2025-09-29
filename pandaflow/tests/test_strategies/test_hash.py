import pandas as pd
import pytest
import hashlib
from pandaflow.strategies.hash import HashStrategy


@pytest.fixture
def sample_df():
    return pd.DataFrame({"A": ["foo", "bar", "baz"], "B": ["123", "456", "789"]})


def test_hash_generation(sample_df):
    rule = {
        "field": "__md5__",
        "strategy": "hash",
        "source": ["A", "B"],
        "function": "calculate_md5",
    }
    strategy = HashStrategy(rule)
    result = strategy.run(sample_df)

    expected = [
        hashlib.md5("foo;123".encode("utf-8")).hexdigest(),
        hashlib.md5("bar;456".encode("utf-8")).hexdigest(),
        hashlib.md5("baz;789".encode("utf-8")).hexdigest(),
    ]
    assert result["__md5__"].tolist() == expected


def test_missing_column_raises(sample_df):
    rule = {
        "field": "__md5__",
        "strategy": "hash",
        "source": ["A", "Z"],  # Z is missing
        "function": "calculate_md5",
    }
    with pytest.raises(ValueError, match="Missing columns for hash: Z"):
        strategy = HashStrategy(rule)
        strategy.run(sample_df)


def test_empty_string_handling():
    df = pd.DataFrame({"A": ["", None], "B": ["x", "y"]})
    rule = {
        "field": "__md5__",
        "strategy": "hash",
        "source": ["A", "B"],
        "function": "calculate_md5",
    }
    strategy = HashStrategy(rule)
    result = strategy.run(df)

    expected = [
        hashlib.md5(";x".encode("utf-8")).hexdigest(),
        hashlib.md5(";y".encode("utf-8")).hexdigest(),
    ]
    assert result["__md5__"].tolist() == expected


def test_validate_rule():
    rule_dict = {
        "field": "__md5__",
        "source": ["A", "B"],
        "strategy": "hash",
        "function": "calculate_md5",
    }
    strategy = HashStrategy(rule_dict)
    validated = strategy.validate_rule()
    assert validated.source == ["A", "B"]
    assert validated.function == "calculate_md5"
