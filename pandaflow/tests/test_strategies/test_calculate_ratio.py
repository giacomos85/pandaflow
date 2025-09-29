import numpy as np
import pytest
import pandas as pd
from pandaflow.strategies.calculate_ratio import CalculateRatioStrategy


@pytest.fixture
def sample_df():
    return pd.DataFrame({"sales": [100, 200, 300, None], "cost": [50, 80, 120, 0]})


def test_basic_ratio(sample_df):
    rule = {
        "strategy": "calculate_ratio",
        "numerator": "sales",
        "denominator": "cost",
        "field": "margin",
    }
    result = CalculateRatioStrategy(rule).run(sample_df)
    assert result["margin"].iloc[0] == 2.0
    assert pd.isna(result["margin"].iloc[3])  # None / 0 → NaN


def test_ratio_with_rounding(sample_df):
    rule = {
        "strategy": "calculate_ratio",
        "numerator": "sales",
        "denominator": "cost",
        "field": "margin",
        "round_digits": 1,
    }
    result = CalculateRatioStrategy(rule).run(sample_df)
    assert result["margin"].iloc[1] == 2.5
    assert result["margin"].iloc[2] == 2.5


def test_missing_numerator(sample_df):
    rule = {
        "strategy": "calculate_ratio",
        "numerator": "revenue",
        "denominator": "cost",
        "field": "margin",
    }
    with pytest.raises(ValueError, match="Numerator column 'revenue' not found"):
        CalculateRatioStrategy(rule).run(sample_df)


def test_missing_denominator(sample_df):
    rule = {
        "strategy": "calculate_ratio",
        "numerator": "sales",
        "denominator": "expenses",
        "field": "margin",
    }
    with pytest.raises(ValueError, match="Denominator column 'expenses' not found"):
        CalculateRatioStrategy(rule).run(sample_df)


def test_division_by_zero():
    df = pd.DataFrame({"a": [10, 20], "b": [2, 0]})
    rule = {
        "strategy": "calculate_ratio",
        "numerator": "a",
        "denominator": "b",
        "field": "ratio",
    }
    result = CalculateRatioStrategy(rule).run(df)
    assert result["ratio"].iloc[0] == 5.0
    assert np.isinf(result["ratio"].iloc[1])  # 20 / 0 → inf


def test_custom_field_name(sample_df):
    rule = {
        "strategy": "calculate_ratio",
        "numerator": "sales",
        "denominator": "cost",
        "field": "custom_ratio",
    }
    result = CalculateRatioStrategy(rule).run(sample_df)
    assert "custom_ratio" in result.columns
