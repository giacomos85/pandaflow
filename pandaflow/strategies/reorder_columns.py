import pandas as pd
from pydantic import Field
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import PandaFlowTransformation
from typing import List, Literal


class ReorderColumnsTransformation(PandaFlowTransformation):
    strategy: Literal["reorder_columns"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'reorder_columns'."
    )
    columns: List[str] = Field(
        description="List of column names specifying the desired order. All listed columns must exist in the DataFrame."
    )


class ReorderColumnsStrategy(TransformationStrategy):

    meta = {
        "name": "reorder_columns",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": """Reorders columns in a DataFrame to match a specified sequence,\ 
ensuring consistent layout for downstream processing or export."""    
    }

    strategy_model = ReorderColumnsTransformation

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [col for col in self.config.columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns in DataFrame: {missing}")
        return df[self.config.columns]
