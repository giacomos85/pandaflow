import pytest
from click.testing import CliRunner
from unittest.mock import patch
from pandaflow.cli.main import cli


@pytest.fixture
def runner():
    return CliRunner()


# ---------- strategies ----------
@patch("pandaflow.cli.strategies.get_registered_strategies")
def test_strategies_success(mock_get, runner):
    mock_get.return_value = {
        "drop": {"version": "1.0.0", "author": "team", "description": "desc"}
    }
    result = runner.invoke(cli, ["strategies"])
    assert result.exit_code == 0
    assert "drop" in result.output


@patch("pandaflow.cli.strategies.get_registered_strategies")
def test_strategies_none(mock_get, runner):
    mock_get.return_value = []
    result = runner.invoke(cli, ["strategies"])
    assert result.exit_code == 0
    assert "No strategies registered" in result.output
