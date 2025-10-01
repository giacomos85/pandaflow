from typing import Literal
import pandas as pd
from pydantic import Field

from pandaflow.models.config import PandaFlowTransformation
from pandaflow.strategies.base import TransformationStrategy


class ConstantTransformation(PandaFlowTransformation):
    strategy: Literal["constant"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'constant'."
    )
    field: str = Field(
        description="Name of the output column that will receive the constant value."
    )
    value: str = Field(
        default="",
        description="Constant value to assign to the specified output column. Defaults to an empty string."
    )


class ConstantStrategy(TransformationStrategy):

    meta = {
        "name": "constant",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": """The **constant** strategy sets a specified column in a DataFrame to a fixed value across all rows.  
This is useful for injecting default flags, labels, or placeholder values during data transformation.""",
    }

    strategy_model = ConstantTransformation

    def apply(self, df: pd.DataFrame):
        df[self.config.field] = self.config.value
        return df
