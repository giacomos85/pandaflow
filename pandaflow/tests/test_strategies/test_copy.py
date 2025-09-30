import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from pandaflow.strategies.copy import CopyStrategy, CopyTransformation


def test_apply_with_input_and_output_transformations():
    df = pd.DataFrame({"Amount": ["€1.234,56", "€2.345,67"]})
    transformation = {
        "field": "__amount__",
        "strategy": "copy",
        "source": "Amount",
        "parser": "default_currency",
        "formatter": "float_2dec",
    }
    strategy = CopyStrategy(transformation)
    result = strategy.run(df)
    expected = pd.DataFrame(
        {"Amount": ["€1.234,56", "€2.345,67"], "__amount__": ["1234,56", "2345,67"]}
    )
    assert_frame_equal(result, expected)


def test_apply_with_fillna_replaces_empty_and_null():
    df = pd.DataFrame({"Amount": ["", None, "€1.000,00"]})
    transformation = {
        "field": "__amount__",
        "strategy": "copy",
        "source": "Amount",
        "parser": "default_currency",
        "formatter": "float_2dec",
        "fillna": "0.00",
    }
    strategy = CopyStrategy(transformation)
    result = strategy.run(df)
    expected = pd.DataFrame(
        {"Amount": ["", None, "€1.000,00"], "__amount__": ["0,00", "0,00", "1000,00"]}
    )
    assert_frame_equal(result, expected)


def test_apply_without_input_or_output_transformations():
    df = pd.DataFrame({"Amount": [10, 20]})
    transformation = {"field": "__amount__", "strategy": "copy", "source": "Amount"}
    strategy = CopyStrategy(transformation)
    result = strategy.run(df)
    expected = pd.DataFrame({"Amount": [10, 20], "__amount__": [10, 20]})
    assert_frame_equal(result, expected)


def test_apply_raises_if_source_column_missing():
    df = pd.DataFrame({"Price": [100]})
    transformation = {"field": "__amount__", "strategy": "copy", "source": "Amount"}
    strategy = CopyStrategy(transformation)
    with pytest.raises(ValueError, match="Column 'Amount' not found"):
        strategy.run(df)
