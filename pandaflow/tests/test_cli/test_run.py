import io
import sys
import csv
import pytest
from pathlib import Path
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

from pandaflow.cli.main import cli  # adjust import to match your CLI module


@pytest.fixture
def temp_csv(tmp_path):
    file = tmp_path / "data.csv"
    file.write_text("name,age\nalice,30\nbob,25")
    return file


@pytest.fixture
def temp_dir_with_csvs(tmp_path):
    dir_path = tmp_path / "csvs"
    dir_path.mkdir()
    (dir_path / "a.csv").write_text("name,age\nx,1")
    (dir_path / "b.csv").write_text("name,age\ny,2")
    return dir_path


@pytest.fixture
def config_file(tmp_path):
    file = tmp_path / "config.yaml"
    file.write_text("rules: []")
    return file


@patch("pandaflow.cli.run.load_config")
@patch("pandaflow.cli.run.transform_csv_batch")
def test_run_single_file_to_stdout(mock_batch, mock_config, temp_csv, config_file):
    mock_config.return_value = {"rules": []}
    df_mock = MagicMock()
    df_mock.to_csv = lambda *args, **kwargs: print("mocked csv output")
    mock_batch.return_value = {temp_csv: df_mock}

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "run",
            "--input",
            str(temp_csv),
            "--config",
            str(config_file),
            "--output",
            "-",
        ],
    )

    assert result.exit_code == 0
    mock_batch.assert_called_once()
    assert "mocked csv output" in result.output


@patch("pandaflow.cli.run.load_config")
@patch("pandaflow.cli.run.transform_csv_batch")
def test_run_batch_to_directory(
    mock_batch, mock_config, temp_dir_with_csvs, config_file, tmp_path
):
    mock_config.return_value = {"rules": []}
    df_mock = MagicMock()
    df_mock.to_csv = MagicMock()
    mock_batch.return_value = {
        temp_dir_with_csvs / "a.csv": df_mock,
        temp_dir_with_csvs / "b.csv": df_mock,
    }

    output_dir = tmp_path / "out"
    output_dir.mkdir()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "run",
            "--input",
            str(temp_dir_with_csvs),
            "--config",
            str(config_file),
            "--output",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0
    assert df_mock.to_csv.call_count == 2


@patch("pandaflow.cli.run.load_config")
@patch("pandaflow.cli.run.transform_csv_batch")
def test_run_skips_none_results(mock_batch, mock_config, temp_csv, config_file):
    mock_config.return_value = {"rules": []}
    mock_batch.return_value = {temp_csv: None}

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "run",
            "--input",
            str(temp_csv),
            "--config",
            str(config_file),
            "--output",
            "-",
        ],
    )

    assert result.exit_code == 0
    assert result.output == ""


@patch("pandaflow.cli.run.load_config")
@patch("pandaflow.cli.run.transform_csv_batch")
def test_run_output_to_file(mock_batch, mock_config, temp_csv, config_file, tmp_path):
    mock_config.return_value = {"rules": []}
    df_mock = MagicMock()
    df_mock.to_csv = MagicMock()
    mock_batch.return_value = {temp_csv: df_mock}

    output_file = tmp_path / "out.csv"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "run",
            "--input",
            str(temp_csv),
            "--config",
            str(config_file),
            "--output",
            str(output_file),
        ],
    )

    assert result.exit_code == 0
    df_mock.to_csv.assert_called_once_with(
        output_file, sep=",", index=False, quoting=csv.QUOTE_ALL, quotechar='"'
    )
