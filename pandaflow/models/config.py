from typing import List, Literal, Optional
from pandaflow.core.generate_schemas import generate_strategy_schemas
from pydantic import BaseModel, Field


class BaseRule(BaseModel):
    strategy: str
    version: str | None = None

class DataSourceConfig(BaseModel):
    type: Literal["csv", "excel", "json", "sql"]
    path: Optional[str] = Field(None, description="Path to the data file or connection string")
    table: Optional[str] = Field(None, description="Table name for SQL sources")
    query: Optional[str] = Field(None, description="Optional SQL query")
    sep: Optional[str] = Field(",", description="CSV separator")
    skiprows: Optional[int] = Field(0, description="Rows to skip when loading")

class ExtractConfig(BaseModel):
    path: Optional[str] = None
    skiprows: Optional[int] = 0
    sep: Optional[str] = ","
    match: Optional[dict] = {}


class LoadConfig(BaseModel):
    output: str | None = None


class PandaFlowConfig(BaseModel):
    file_path: Optional[str] = Field(None, exclude=True, description="Path to the config file (set at load time)")
    data_sources: Optional[List[DataSourceConfig]] = Field(default_factory=list)
    meta: Optional[ExtractConfig] = {}
    transformations: List[BaseRule]
    load: Optional[LoadConfig] = {}


    @classmethod
    def model_json_schema(cls):
        base = super().model_json_schema()
        strategy_schemas = generate_strategy_schemas()

        # Inject strategy schemas into components
        base.setdefault("components", {}).setdefault("schemas", {}).update(strategy_schemas)

        # Optionally: add oneOf to "transformations" field
        rule_defs = [{"$ref": f"#/components/schemas/{name}:{version}"} for name, version in strategy_schemas.items()]
        base["properties"]["transformations"] = {
            "type": "array",
            "items": {"oneOf": rule_defs}
        }

        return base