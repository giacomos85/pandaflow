import pandas as pd
import pytest
import hashlib
from pandaflow.strategies.hash import HashStrategy


@pytest.fixture
def sample_df():
    return pd.DataFrame({"A": ["foo", "bar", "baz"], "B": ["123", "456", "789"]})


def test_hash_generation(sample_df):
    transformation = {
        "field": "__md5__",
        "strategy": "hash",
        "source": ["A", "B"],
        "function": "calculate_md5",
    }
    strategy = HashStrategy(transformation)
    result = strategy.run(sample_df)

    expected = [
        hashlib.md5("foo;123".encode("utf-8")).hexdigest(),
        hashlib.md5("bar;456".encode("utf-8")).hexdigest(),
        hashlib.md5("baz;789".encode("utf-8")).hexdigest(),
    ]
    assert result["__md5__"].tolist() == expected


def test_missing_column_raises(sample_df):
    transformation = {
        "field": "__md5__",
        "strategy": "hash",
        "source": ["A", "Z"],  # Z is missing
        "function": "calculate_md5",
    }
    with pytest.raises(ValueError, match="Missing columns for hash: Z"):
        strategy = HashStrategy(transformation)
        strategy.run(sample_df)


def test_empty_string_handling():
    df = pd.DataFrame({"A": ["", None], "B": ["x", "y"]})
    transformation = {
        "field": "__md5__",
        "strategy": "hash",
        "source": ["A", "B"],
        "function": "calculate_md5",
    }
    strategy = HashStrategy(transformation)
    result = strategy.run(df)

    expected = [
        hashlib.md5(";x".encode("utf-8")).hexdigest(),
        hashlib.md5(";y".encode("utf-8")).hexdigest(),
    ]
    assert result["__md5__"].tolist() == expected
