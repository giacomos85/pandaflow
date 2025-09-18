import pandas as pd
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from pandaflow.strategies.csvfile import CsvFileStrategy


@pytest.fixture
def strategy():
    return CsvFileStrategy()


@pytest.fixture
def input_df():
    return pd.DataFrame({"code": ["A", "B", "C"], "other": ["x", "y", "z"]})


def test_validate_rule(strategy):
    rule_dict = {
        "field": "label",
        "strategy": "csvfile",
        "source": "code",
        "file": "lookup.csv",
        "key": "code",
        "value": "label",
    }
    validated = strategy.validate_rule(rule_dict)
    assert validated.file == "lookup.csv"
    assert validated.key == "code"
    assert validated.value == "label"


def test_missing_file_returns_not_found(strategy, input_df):
    rule = {
        "field": "label",
        "source": "code",
        "file": "nonexistent.csv",
        "key": "code",
        "value": "label",
        "not_found": "N/A",
    }
    result = strategy.apply(input_df, rule, output="output.csv")
    assert result["label"].tolist() == ["N/A", "N/A", "N/A"]


def test_missing_key_or_value_raises(strategy, input_df):
    rule = {"field": "label", "file": "lookup.csv"}
    with pytest.raises(ValueError, match="Missing 'file', 'key', or 'value'"):
        strategy.apply(input_df, rule, output="output.csv")


def test_missing_lookup_columns_raises(strategy, input_df):
    with TemporaryDirectory() as tmpdir:
        lookup_path = Path(tmpdir) / "lookup.csv"
        pd.DataFrame({"wrong": ["A", "B"], "data": ["Alpha", "Beta"]}).to_csv(
            lookup_path, index=False
        )

        rule = {
            "field": "label",
            "source": "code",
            "file": str(lookup_path),
            "key": "code",
            "value": "label",
        }
        with pytest.raises(
            ValueError,
            match=f"Key \\[code\\] or value \\[label\\] column not found in CSV for field label. Columns found: wrong, data. {lookup_path}",
        ):
            strategy.apply(input_df, rule, output="output.csv")


def test_missing_source_column_raises(strategy, input_df):
    with TemporaryDirectory() as tmpdir:
        lookup_path = Path(tmpdir) / "lookup.csv"
        pd.DataFrame({"code": ["A", "B"], "label": ["Alpha", "Beta"]}).to_csv(
            lookup_path, index=False
        )

        rule = {
            "field": "label",
            "source": "missing",
            "file": str(lookup_path),
            "key": "code",
            "value": "label",
        }
        with pytest.raises(ValueError, match="Source column 'missing' not found"):
            strategy.apply(input_df, rule, output="output.csv")


def test_successful_lookup(strategy, input_df):
    with TemporaryDirectory() as tmpdir:
        lookup_path = Path(tmpdir) / "lookup.csv"
        pd.DataFrame({"code": ["A", "B"], "label": ["Alpha", "Beta"]}).to_csv(
            lookup_path, index=False
        )

        rule = {
            "field": "label",
            "source": "code",
            "file": str(lookup_path),
            "key": "code",
            "value": "label",
            "not_found": "N/A",
        }
        result = strategy.apply(input_df, rule, output="output.csv")
        assert result["label"].tolist() == ["Alpha", "Beta", "N/A"]


# def test_file_path_substitution(strategy, input_df):
#     with TemporaryDirectory() as tmpdir:
#         year_dir = Path(tmpdir) / "2025"
#         year_dir.mkdir()
#         output_path = year_dir / "output.csv"

#         lookup_path = year_dir / "output.csv"
#         pd.DataFrame({"code": ["A"], "label": ["Alpha"]}).to_csv(lookup_path, index=False)

#         rule = {
#             "field": "label",
#             "source": "code",
#             "file": "${year}/${output}",
#             "key": "code",
#             "value": "label"
#         }
#         result = strategy.apply(input_df, rule, output=str(output_path))
#         assert result["label"].iloc[0] == "Alpha"
