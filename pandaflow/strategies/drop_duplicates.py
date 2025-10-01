import pandas as pd
from pydantic import Field
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import PandaFlowTransformation
from typing import Literal, Optional, List


class DropDuplicatesTransformation(PandaFlowTransformation):
    strategy: Literal["drop_duplicates"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'drop_duplicates'."
    )
    subset: Optional[List[str]] = Field(
        default=None,
        description="Optional list of column names to consider when identifying duplicates. If None, all columns are used."
    )
    keep: Optional[str] = Field(
        default="first",
        description="Determines which duplicate to keep: 'first', 'last', or False to drop all duplicates."
    )
    reset_index: Optional[bool] = Field(
        default=False,
        description="Whether to reset the DataFrame index after dropping duplicates. Defaults to False."
    )


class DropDuplicatesStrategy(TransformationStrategy):

    meta = {
        "name": "drop_duplicates",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": """Drops duplicate rows from a DataFrame, optionally based on a subset of columns."""
    }

    strategy_model = DropDuplicatesTransformation

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.drop_duplicates(subset=self.config.subset, keep=self.config.keep)
        if self.config.reset_index:
            result = result.reset_index(drop=True)
        return result
