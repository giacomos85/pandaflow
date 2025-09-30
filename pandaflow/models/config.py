from typing import List, Optional
from pandaflow.core.generate_schemas import generate_strategy_schemas
from pydantic import BaseModel


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


    @classmethod
    def model_json_schema(cls):
        base = super().model_json_schema()
        strategy_schemas = generate_strategy_schemas()

        # Inject strategy schemas into components
        base.setdefault("components", {}).setdefault("schemas", {}).update(strategy_schemas)

        # Optionally: add oneOf to "rules" field
        rule_defs = [{"$ref": f"#/components/schemas/{name}:{version}"} for name, version in strategy_schemas.items()]
        base["properties"]["rules"] = {
            "type": "array",
            "items": {"oneOf": rule_defs}
        }

        return base