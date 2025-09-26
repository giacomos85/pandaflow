import json
from pathlib import Path
from typing import List, Optional

import toml
from pydantic import BaseModel

from pandaflow.core.factory import StrategyFactory


class BaseRule(BaseModel):
    strategy: str
    version: str | None = None


class ExtractConfig(BaseModel):
    path: Optional[str] = None
    skiprows: Optional[int] = 0
    sep: Optional[str] = ","
    match: Optional[dict] = {}


class LoadConfig(BaseModel):
    output: str | None = None


class PandaFlowConfig(BaseModel):
    meta: Optional[ExtractConfig] = {}
    rules: List[BaseRule]
    load: Optional[LoadConfig] = {}


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
        factory = StrategyFactory(config)

        typed_rules = []
        for rule_dict in raw_config.get("rules", []):

            strategy_name = rule_dict.get("strategy")
            version = rule_dict.get("version", None)

            strategy_cls = factory.get_strategy(strategy_name, version=version)

            # Replace raw rule dicts with typed rule objects
            if not strategy_cls:
                raise ValueError(f"Unknown strategy: {strategy_name}")
            strategy = strategy_cls(rule_dict)
            typed_rules.append(strategy)

        config.rules = typed_rules
        return config
