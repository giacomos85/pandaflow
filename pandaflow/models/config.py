from typing import List, Optional
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
