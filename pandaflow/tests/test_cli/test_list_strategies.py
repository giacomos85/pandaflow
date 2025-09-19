import pytest
from click.testing import CliRunner
from unittest.mock import patch
from pandaflow.cli.main import cli


@pytest.fixture
def runner():
    return CliRunner()


# ---------- list_strategies ----------
@patch("pandaflow.cli.list_strategies.get_registered_strategies")
def test_list_strategies_success(mock_get, runner):
    mock_get.return_value = [
        {"name": "drop", "version": "1.0.0", "author": "team", "description": "desc"}
    ]
    result = runner.invoke(cli, ["list-strategies"])
    assert result.exit_code == 0
    assert "drop" in result.output


@patch("pandaflow.cli.list_strategies.get_registered_strategies")
def test_list_strategies_with_error(mock_get, runner):
    mock_get.return_value = [{"name": "bad", "error": "ImportError"}]
    result = runner.invoke(cli, ["list-strategies"])
    assert result.exit_code == 0
    assert "Failed to load" in result.output


@patch("pandaflow.cli.list_strategies.get_registered_strategies")
def test_list_strategies_none(mock_get, runner):
    mock_get.return_value = []
    result = runner.invoke(cli, ["list-strategies"])
    assert result.exit_code == 0
    assert "No strategies registered" in result.output
