import json
from pathlib import Path

from pandaflow.models.config import PandaFlowConfig
import toml

from pandaflow.core.factory import StrategyFactory


def read_config_file(path: str):
    ext = Path(path).suffix.lower()

    raw_config = {}

    with open(path, "rb") as f:
        if ext == ".toml":
            raw_config = toml.load(path)
        elif ext == ".json":
            raw_config = json.load(f)
        else:
            raise ValueError(f"Unsupported config format: {ext}")

    return raw_config


def config_parser(config_dict: dict):
    # Validate top-level config
    config = PandaFlowConfig(**config_dict)

    config.transformations = [
        StrategyFactory.get_strategy(t) for t in config_dict.get("transformations", [])
    ]
    return config


def load_config(path: str):
    raw_config = read_config_file(path)
    cfg = config_parser(raw_config)
    cfg.file_path = path
    return cfg
