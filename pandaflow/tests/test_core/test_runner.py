import pandas as pd
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch, MagicMock
from pandaflow.core.runner import (
    rule_matches_file,
    process_single_csv,
    process_csvs,
)


# ---------- rule_matches_file ----------
def test_rule_matches_filename(tmp_path):
    file = tmp_path / "data.csv"
    file.touch()
    match = {"filename": "data.csv"}
    assert rule_matches_file(match, file)


def test_rule_matches_glob(tmp_path):
    file = tmp_path / "data.csv"
    file.touch()
    match = {"glob": "*.csv"}
    assert rule_matches_file(match, file)


def test_rule_matches_regex(tmp_path):
    file = tmp_path / "data.csv"
    file.touch()
    match = {"regex": r".*data\.csv"}
    assert rule_matches_file(match, file)


def test_rule_matches_filename_mismatch(tmp_path):
    file = tmp_path / "data.csv"
    file.touch()
    match = {"filename": "other.csv"}
    assert not rule_matches_file(match, file)


def test_rule_matches_glob_mismatch(tmp_path):
    file = tmp_path / "data.csv"
    file.touch()
    match = {"glob": "*.txt"}
    assert not rule_matches_file(match, file)


def test_rule_matches_regex_mismatch(tmp_path):
    file = tmp_path / "data.csv"
    file.touch()
    match = {"regex": r".*\.txt"}
    assert not rule_matches_file(match, file)


# ---------- process_single_csv ----------
@patch("pandaflow.core.runner.StrategyFactory")
def test_process_single_csv_applies_strategy(mock_factory, tmp_path):
    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "output.csv"
    pd.DataFrame({"A": ["1", "2"]}).to_csv(input_file, index=False)

    mock_strategy = MagicMock()
    mock_strategy.run.side_effect = lambda df, rule, **kwargs: df.assign(B=["x", "y"])
    mock_factory.return_value.get_strategy.return_value = mock_strategy

    config = {"rules": [{"field": "B", "strategy": "dummy"}], "meta": {}, "match": {}}

    process_single_csv(input_file, output_file, config, verbose=True)
    df = pd.read_csv(output_file)
    assert "B" in df.columns
    assert df["B"].tolist() == ["x", "y"]


@patch("pandaflow.core.runner.StrategyFactory")
def test_process_single_csv_skips_on_match(mock_factory, tmp_path):
    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "output.csv"
    pd.DataFrame({"A": ["1"]}).to_csv(input_file, index=False)

    config = {"rules": [], "meta": {}, "match": {"filename": "not_this.csv"}}

    process_single_csv(input_file, output_file, config)
    assert not output_file.exists()


@patch("pandaflow.core.runner.StrategyFactory")
def test_process_single_csv_fallback_to_none(mock_factory, tmp_path):
    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "output.csv"
    pd.DataFrame({"A": ["1"]}).to_csv(input_file, index=False)

    mock_factory.return_value.get_strategy.return_value = None

    config = {"rules": [{"field": "B", "strategy": "unknown"}], "meta": {}, "match": {}}

    process_single_csv(input_file, output_file, config)
    df = pd.read_csv(output_file)
    assert "B" in df.columns
    assert df["B"].isnull().all()


# ---------- process_csvs ----------
@patch("pandaflow.core.runner.load_config")
@patch("pandaflow.core.runner.process_single_csv")
def test_process_csvs_single_file(mock_process, mock_load, tmp_path):
    input_file = tmp_path / "input.csv"
    input_file.write_text("A,B\n1,x")
    output_file = tmp_path / "output.csv"
    config_file = tmp_path / "config.toml"
    config_file.write_text("")

    mock_load.return_value = {"rules": []}
    process_csvs(input_file, output_file, config_file)
    mock_process.assert_called_once()


@patch("pandaflow.core.runner.load_config")
@patch("pandaflow.core.runner.process_single_csv")
def test_process_csvs_batch_mode(mock_process, mock_load, tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "file.csv").write_text("A,B\n1,x")
    output_dir = tmp_path / "output"
    config_file = tmp_path / "config.toml"
    config_file.write_text("")

    mock_load.return_value = {"rules": []}
    process_csvs(input_dir, output_dir, config_file)
    mock_process.assert_called_once()


@patch("pandaflow.core.runner.load_config")
def test_process_csvs_invalid_output_for_batch(mock_load, tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_file = tmp_path / "output.csv"
    output_file.write_text("")
    config_file = tmp_path / "config.toml"
    config_file.write_text("")

    mock_load.return_value = {"rules": []}
    with pytest.raises(ValueError, match="Expected output to be a folder"):
        process_csvs(input_dir, output_file, config_file)


@patch("pandaflow.core.runner.load_config")
def test_process_csvs_invalid_output_for_file(mock_load, tmp_path):
    input_file = tmp_path / "input.csv"
    input_file.write_text("A,B\n1,x")
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    config_file = tmp_path / "config.toml"
    config_file.write_text("")

    mock_load.return_value = {"rules": []}
    with pytest.raises(ValueError, match="Expected output to be a file"):
        process_csvs(input_file, output_dir, config_file)
