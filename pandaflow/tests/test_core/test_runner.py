import io
import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pandaflow.core.runner import rule_matches_file, transform_csv, transform_csv_batch


@pytest.mark.parametrize(
    "match,file_name,expected",
    [
        ({}, "data.csv", True),
        ({"filename": "data.csv"}, "data.csv", True),
        ({"filename": "data.csv"}, "other.csv", False),
        ({"glob": "*.csv"}, "data.csv", True),
        ({"glob": "*.csv"}, "data.txt", False),
        ({"regex": r".*/data.csv"}, "/tmp/data.csv", True),
        ({"regex": r".*/data.csv"}, "/tmp/other.csv", False),
    ],
)
def test_rule_matches_file(tmp_path, match, file_name, expected):
    file_path = tmp_path / Path(file_name).name
    file_path.write_text("dummy")
    assert rule_matches_file(match, file_path) is expected


def sample_config():
    return {
        "meta": {"skiprows": 0, "csv_separator": ","},
        "match": {},
        "rules": [
            {"field": "name", "strategy": "uppercase"},
            {"field": "age", "strategy": "csvfile", "version": "v1"},
        ],
    }


@pytest.fixture
def sample_csv(tmp_path):
    file = tmp_path / "data.csv"
    file.write_text("name,age\nalice,30\nbob,25")
    return file


@patch("pandaflow.core.runner.StrategyFactory")
def test_transform_csv_with_path(mock_factory, sample_csv):
    config = sample_config()

    # Mock strategies
    strategy1 = MagicMock()
    strategy1.run.side_effect = lambda df, rule: df.assign(name=df["name"].str.upper())

    strategy2 = MagicMock()
    strategy2.run.side_effect = lambda df, rule, output=None: df.assign(age="X")

    mock_factory.return_value.get_strategy.side_effect = [strategy1, strategy2]

    df = transform_csv(sample_csv, config)

    assert isinstance(df, pd.DataFrame)
    assert df.loc[0, "name"] == "ALICE"
    assert df.loc[1, "age"] == "X"


@patch("pandaflow.core.runner.StrategyFactory")
def test_transform_csv_with_filelike(mock_factory):
    config = sample_config()
    csv_data = io.StringIO("name,age\nalice,30\nbob,25")

    strategy1 = MagicMock()
    strategy1.run.side_effect = lambda df, rule: df.assign(name=df["name"].str.upper())

    strategy2 = MagicMock()
    strategy2.run.side_effect = lambda df, rule, output=None: df.assign(age="X")

    mock_factory.return_value.get_strategy.side_effect = [strategy1, strategy2]

    df = transform_csv(csv_data, config)

    assert isinstance(df, pd.DataFrame)
    assert df.loc[0, "name"] == "ALICE"
    assert df.loc[1, "age"] == "X"


@patch("pandaflow.core.runner.StrategyFactory")
def test_transform_csv_skipped_by_match(mock_factory, tmp_path):
    config = sample_config()
    config["match"] = {"filename": "not_this.csv"}

    file = tmp_path / "data.csv"
    file.write_text("name,age\nalice,30")

    df = transform_csv(file, config)
    assert df is None
    mock_factory.assert_not_called()


@patch("pandaflow.core.runner.transform_csv")
def test_transform_csv_batch(mock_transform):
    file1 = Path("/tmp/file1.csv")
    file2 = Path("/tmp/file2.csv")

    mock_transform.side_effect = [pd.DataFrame({"a": [1]}), None]

    config = {}
    result = transform_csv_batch([file1, file2], config)

    assert isinstance(result, dict)
    assert result[file1].equals(pd.DataFrame({"a": [1]}))
    assert result[file2] is None
