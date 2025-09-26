import json
import toml
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from pandaflow.core.config import load_config, PandaFlowConfig
from pandaflow.strategies.base import TransformationStrategy


class DummyStrategy(TransformationStrategy):
    meta = {"name": "dummy", "version": "1.0.0", "author": "Giacomo"}

    def apply(self, df, rule):
        return df


def make_config_file(ext: str, content: dict):
    tmp = TemporaryDirectory()
    path = Path(tmp.name) / f"config{ext}"
    with open(path, "w", encoding="utf-8") as f:
        if ext == ".json":
            json.dump(content, f)
        elif ext == ".toml":
            toml.dump(content, f)
    return path, tmp


def test_load_json_config_success():
    config_data = {"rules": [{"strategy": "dummy", "version": "1.0.0"}]}
    path, tmp = make_config_file(".json", config_data)

    with patch(
        "pandaflow.core.config.StrategyFactory.get_strategy", return_value=DummyStrategy
    ):
        config = load_config(str(path))
        assert isinstance(config, PandaFlowConfig)
        tmp.cleanup()


def test_load_toml_config_success():
    config_data = {"rules": [{"strategy": "dummy", "version": "1.0.0"}]}
    path, tmp = make_config_file(".toml", config_data)

    with patch(
        "pandaflow.core.config.StrategyFactory.get_strategy", return_value=DummyStrategy
    ):
        config = load_config(str(path))
        assert isinstance(config, PandaFlowConfig)
        tmp.cleanup()


def test_load_config_unknown_strategy():
    config_data = {"rules": [{"strategy": "unknown", "version": "1.0.0"}]}
    path, tmp = make_config_file(".json", config_data)

    with patch("pandaflow.core.config.StrategyFactory.get_strategy", return_value=None):
        with pytest.raises(ValueError, match="Unknown strategy: unknown"):
            load_config(str(path))
        tmp.cleanup()
