import json
from pathlib import Path

import toml
from pydantic import BaseModel


class BaseRule(BaseModel):
    strategy: str


class PreprocessConfig(BaseModel):
    meta: dict = {}
    match: dict = {}
    rules: list[BaseRule] = []


def validate_config(config: dict) -> bool:
    return PreprocessConfig(**config)  # Usa Pydantic per una validazione più rigorosa


def load_config(path: str) -> dict:
    ext = Path(path).suffix.lower()

    with open(path, "rb") as f:
        if ext == ".toml":
            return toml.load(path)
        elif ext == ".json":
            return json.load(f)
        else:
            raise ValueError(f"Unsupported config format: {ext}")
