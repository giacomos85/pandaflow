import pandas as pd
from pydantic import Field
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import PandaFlowTransformation
from typing import List, Literal


class DropColumnsTransformation(PandaFlowTransformation):
    strategy: Literal["drop_columns"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'drop_columns'."
    )
    columns: List[str] = Field(
        description="List of column names to drop from the DataFrame. All names must be strings."
    )
    errors: str = Field(
        default="raise",
        description="Behavior when a specified column is missing. Use 'raise' to throw an error or 'ignore' to skip silently."
    )


class DropColumnsStrategy(TransformationStrategy):
    
    meta = {
        "name": "drop_columns",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": """Drops one or more columns from the DataFrame."""
    }

    strategy_model = DropColumnsTransformation

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns=self.config.columns, errors=self.config.errors)
