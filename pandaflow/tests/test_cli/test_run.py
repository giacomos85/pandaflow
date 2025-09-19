import pytest
from click.testing import CliRunner
from unittest.mock import patch

from pandaflow.cli.run import run  # Adjust import to match your CLI module


@pytest.fixture
def config_file(tmp_path):
    file = tmp_path / "config.yaml"
    file.write_text("rules: []")
    return file


@pytest.fixture
def input_file(tmp_path):
    file = tmp_path / "data.csv"
    file.write_text("name,age\nalice,30")
    return file


@pytest.fixture
def input_dir(tmp_path):
    dir_path = tmp_path / "csvs"
    dir_path.mkdir()
    (dir_path / "a.csv").write_text("name,age\nx,1")
    (dir_path / "b.csv").write_text("name,age\ny,2")
    return dir_path


@patch("pandaflow.cli.run.load_config")
@patch("pandaflow.cli.run.read_csvs")
@patch("pandaflow.cli.run.transform_dataframe_mapping")
@patch("pandaflow.cli.run.writer")
def test_run_single_file_to_stdout(
    mock_write, mock_transform, mock_read, mock_config, input_file, config_file
):
    mock_config.return_value = {"rules": []}
    mock_read.return_value = {input_file: "df"}
    mock_transform.return_value = {input_file: "df"}

    runner = CliRunner()
    result = runner.invoke(
        run, ["--input", str(input_file), "--config", str(config_file), "--output", "-"]
    )

    assert result.exit_code == 0
    mock_write.assert_called_once_with({input_file: "df"}, "-")


@patch("pandaflow.cli.run.load_config")
@patch("pandaflow.cli.run.read_csvs")
@patch("pandaflow.cli.run.transform_dataframe_mapping")
@patch("pandaflow.cli.run.writer")
def test_run_directory_to_file(
    mock_write, mock_transform, mock_read, mock_config, input_dir, config_file, tmp_path
):
    mock_config.return_value = {"rules": []}
    mock_read.return_value = {input_dir / "a.csv": "df1", input_dir / "b.csv": "df2"}
    mock_transform.return_value = {
        input_dir / "a.csv": "df1",
        input_dir / "b.csv": "df2",
    }

    output_dir = tmp_path / "out"
    output_dir.mkdir()

    runner = CliRunner()
    result = runner.invoke(
        run,
        [
            "--input",
            str(input_dir),
            "--config",
            str(config_file),
            "--output",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0
    mock_write.assert_called_once_with(mock_transform.return_value, str(output_dir))


@patch("pandaflow.cli.run.load_config")
@patch("pandaflow.cli.run.read_csvs")
@patch("pandaflow.cli.run.transform_dataframe_mapping")
@patch("pandaflow.cli.run.writer")
def test_run_skipped_files(
    mock_write, mock_transform, mock_read, mock_config, input_file, config_file
):
    mock_config.return_value = {"rules": []}
    mock_read.return_value = {input_file: None}
    mock_transform.return_value = {input_file: None}

    runner = CliRunner()
    result = runner.invoke(
        run, ["--input", str(input_file), "--config", str(config_file), "--output", "-"]
    )

    assert result.exit_code == 0
    mock_write.assert_called_once_with({input_file: None}, "-")
