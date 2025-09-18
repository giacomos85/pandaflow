import pytest
from click.testing import CliRunner
from unittest.mock import patch
from pandaflow.cli.main import cli


@pytest.fixture
def runner():
    return CliRunner()


# ---------- run ----------
@patch("pandaflow.cli.run.process_csvs")
def test_run_command_valid(mock_process, runner, tmp_path):
    input_file = tmp_path / "input.csv"
    input_file.write_text("A,B\n1,x")
    config_file = tmp_path / "config.toml"
    config_file.write_text("")

    result = runner.invoke(
        cli,
        [
            "run",
            "-i",
            str(input_file),
            "-o",
            str(tmp_path / "out.csv"),
            "-c",
            str(config_file),
        ],
    )
    assert result.exit_code == 0
    mock_process.assert_called_once()


@patch("pandaflow.cli.run.process_csvs")
def test_run_command_dir_output_mismatch(mock_process, runner, tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_file = tmp_path / "out.csv"
    output_file.write_text("")
    config_file = tmp_path / "config.toml"
    config_file.write_text("")

    result = runner.invoke(
        cli,
        ["run", "-i", str(input_dir), "-o", str(output_file), "-c", str(config_file)],
    )
    assert result.exit_code != 0
    assert "must also be a directory" in result.output


@patch("pandaflow.cli.run.process_csvs")
def test_run_command_file_output_mismatch(mock_process, runner, tmp_path):
    input_file = tmp_path / "input.csv"
    input_file.write_text("A,B\n1,x")
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    config_file = tmp_path / "config.toml"
    config_file.write_text("")

    result = runner.invoke(
        cli,
        ["run", "-i", str(input_file), "-o", str(output_dir), "-c", str(config_file)],
    )
    assert result.exit_code != 0
    assert "must be a file" in result.output
