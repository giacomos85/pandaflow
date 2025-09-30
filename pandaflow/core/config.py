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
        factory = StrategyFactory(config)

        typed_transformations = []
        for rule_dict in raw_config.get("transformations", []):

            strategy_name = rule_dict.get("strategy")
            version = rule_dict.get("version", None)

            strategy_cls = factory.get_strategy(strategy_name, version=version)

            # Replace raw rule dicts with typed rule objects
            if not strategy_cls:
                raise ValueError(f"Unknown strategy: {strategy_name}")
            strategy = strategy_cls(rule_dict)
            typed_transformations.append(strategy)

        config.transformations = typed_transformations
        return config
