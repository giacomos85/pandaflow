from typing import Literal
import pandas as pd
from pydantic import Field

from pandaflow.models.config import PandaFlowTransformation
from pandaflow.strategies.base import TransformationStrategy

class DebugTransformation(PandaFlowTransformation):
    strategy: Literal["debug"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'debug'."
    )

class DebugStrategy(TransformationStrategy):

    meta = {
        "name": "debug",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": "A strategy that prints debug information",
    }

    strategy_model = DebugTransformation

    def apply(self, df: pd.DataFrame):
        print(df.head())
