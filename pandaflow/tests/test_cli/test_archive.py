import pytest
from click.testing import CliRunner
from unittest.mock import patch
from pandaflow.cli.main import cli


@pytest.fixture
def runner():
    return CliRunner()


# ---------- archive ----------
@patch("pandaflow.cli.archive.archive_csv_by_date")
def test_archive_command(mock_archive, runner, tmp_path):
    input_file = tmp_path / "data.csv"
    input_file.write_text("date,value\n2023-01-01,x")
    result = runner.invoke(
        cli,
        [
            "archive",
            "-i",
            str(input_file),
            "-o",
            str(tmp_path / "out"),
            "-d",
            "date",
            "-s",
            "year",
            "--pattern",
            "{year}/{month:02d}.csv",
        ],
    )
    assert result.exit_code == 0
    mock_archive.assert_called_once()
