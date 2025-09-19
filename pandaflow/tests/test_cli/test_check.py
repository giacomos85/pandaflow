import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from pandaflow.cli.main import cli


@pytest.fixture
def runner():
    return CliRunner()


# ---------- check ----------
@patch("pandaflow.cli.check.load_config")
@patch("pandaflow.cli.check.StrategyFactory")
def test_check_command_valid(mock_factory, mock_load, runner, tmp_path):
    config_file = tmp_path / "config.toml"
    config_file.write_text("")
    mock_load.return_value = {"rules": [{"strategy": "dummy", "version": "1.0"}]}
    mock_strategy = MagicMock()
    mock_strategy.check.return_value = True
    mock_factory.return_value.get_strategy.return_value = mock_strategy

    result = runner.invoke(cli, ["check", "-c", str(config_file)])
    assert result.exit_code == 0
    mock_strategy.check.assert_called_once()


@patch("pandaflow.cli.check.load_config")
@patch("pandaflow.cli.check.StrategyFactory")
def test_check_command_invalid_strategy(mock_factory, mock_load, runner, tmp_path):
    config_file = tmp_path / "config.toml"
    config_file.write_text("")
    mock_load.return_value = {"rules": [{"strategy": "dummy"}]}
    mock_factory.return_value.get_strategy.side_effect = ValueError("bad strategy")

    result = runner.invoke(cli, ["check", "-c", str(config_file)])
    assert result.exit_code != 0
    assert "Exception while validating" in result.output
