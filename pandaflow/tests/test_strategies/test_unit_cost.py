import pandas as pd
import pytest
from pandaflow.strategies.calculateunitcost import UnitCostStrategy


# Mock input/output rule functions
def mock_parser(value):
    try:
        return float(str(value).replace(",", "."))
    except Exception:
        return 0.0


def mock_formatter(value):
    return round(value, 2)


@pytest.fixture
def strategy(monkeypatch):
    monkeypatch.setattr("pandaflow.utils.get_input_parser", lambda _: mock_parser)
    monkeypatch.setattr(
        "pandaflow.utils.get_output_formatter", lambda _: mock_formatter
    )
    return UnitCostStrategy()


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {"Importo": ["100.0", "200.0", "0", "300.0"], "Quote": ["2", "4", "0", "3"]}
    )


@pytest.fixture
def valid_rule():
    return {
        "field": "__unitcost__",
        "strategy": "calculate_unitcost",
        "total": "Importo",
        "quantity": "Quote",
        "input_rule": "us_currency",
        "output_rule": "float_2dec",
    }


def test_apply_valid(strategy, sample_df, valid_rule):
    result = strategy.apply(sample_df, valid_rule)
    expected = ["50,00", "50,00", "0,00", "100,00"]
    assert result["__unitcost__"].tolist() == expected


def test_apply_zero_quantity(strategy, valid_rule):
    df = pd.DataFrame({"Importo": ["100"], "Quote": ["0"]})
    result = strategy.apply(df, valid_rule)
    assert result["__unitcost__"].iloc[0] == "0,00"


def test_apply_missing_total_column(strategy, valid_rule):
    df = pd.DataFrame({"Quote": ["2"]})
    with pytest.raises(KeyError):
        strategy.apply(df, valid_rule)


def test_validate_rule(strategy, valid_rule):
    validated = strategy.validate_rule(valid_rule)
    assert validated.total == "Importo"
    assert validated.quantity == "Quote"
