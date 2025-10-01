import pandas as pd
import pytest
from pandaflow.strategies.cast_columns import CastColumnsStrategy, CastColumnsTransformation

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "amount": ["10", "20.5", "invalid", None],
        "discount": ["5", "NaN", "15", "bad"]
    })

# def test_cast_float_with_coerce_and_fallback(sample_df):
#     config_dict = {
#         "strategy": "cast_columns",
#         "fields": ["amount", "discount"],
#         "target_type":"float",
#         "errors": "coerce",
#         "fallback": "0"
#     }
#     strategy = CastColumnsStrategy(config_dict=config_dict)
#     result = strategy.apply(sample_df)

#     assert result["amount"].dtype == float
#     assert result["discount"].dtype == float
#     assert result["amount"].iloc[2] == 0  # "invalid" → NaN → fallback
#     assert result["amount"].iloc[3] == 0  # None → NaN → fallback

def test_cast_str(sample_df):
    config_dict = {
        "strategy": "cast_columns",
        "fields": ["amount", "discount"],
        "target_type":"str",
    }
    strategy = CastColumnsStrategy(config_dict=config_dict)
    result = strategy.apply(sample_df)

    assert result["amount"].dtype == object
    assert result["discount"].iloc[3] == "bad"

def test_cast_with_raise_error(sample_df):
    config_dict = {
        "strategy": "cast_columns",
        "fields": ["amount"],
        "target_type":"int",
        "errors": "raise"
    }
    strategy = CastColumnsStrategy(config_dict=config_dict)

    with pytest.raises(TypeError):
        strategy.apply(sample_df)

def test_missing_column_error(sample_df):
    config_dict = {
        "strategy": "cast_columns",
        "fields": ["missing"],
        "target_type":"float"
    }
    strategy = CastColumnsStrategy(config_dict=config_dict)

    with pytest.raises(KeyError):
        strategy.apply(sample_df)
