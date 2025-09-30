import json
from pathlib import Path

from pandaflow.models.config import PandaFlowConfig
import toml

from pandaflow.core.factory import StrategyFactory


def load_config(path: str):
    ext = Path(path).suffix.lower()

    with open(path, "rb") as f:
        if ext == ".toml":
            raw_config = toml.load(path)
        elif ext == ".json":
            raw_config = json.load(f)
        else:
            raise ValueError(f"Unsupported config format: {ext}")

        # Validate top-level config
        config = PandaFlowConfig(**raw_config)
        config.file_path = path

        typed_transformations = []
        for transformation_dict in raw_config.get("transformations", []):
            strategy = StrategyFactory.get_strategy(transformation_dict)
            typed_transformations.append(strategy)

        config.transformations = typed_transformations
        return config
