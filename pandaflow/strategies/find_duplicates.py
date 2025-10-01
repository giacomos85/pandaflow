import pandas as pd
from pydantic import Field
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import PandaFlowTransformation
from typing import Literal, Optional, List


class FindDuplicatesTransformation(PandaFlowTransformation):
    strategy: Literal["find_duplicates"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'find_duplicates'."
    )
    subset: Optional[List[str]] = Field(
        default=None,
        description="Optional list of column names to consider when identifying duplicates. If None, all columns are used."
    )
    keep: Optional[str] = Field(
        default="first",
        description="Determines which duplicate to mark as retained: 'first', 'last', or False to mark all duplicates."
    )
    reset_index: Optional[bool] = Field(
        default=False,
        description="Whether to reset the DataFrame index after identifying duplicates. Defaults to False."
    )


class FindDuplicatesStrategy(TransformationStrategy):

    meta = {
        "name": "find_duplicates",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": """Identifies duplicate rows based on specified columns and marks retained entries \
using pandas-style keep semantics. Optionally resets the index for downstream compatibility."""
    }

    strategy_model = FindDuplicatesTransformation

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.drop_duplicates(subset=self.config.subset, keep=self.config.keep)
        duplicates = df[df.duplicated(subset=self.config.subset, keep=False)]
        if self.config.reset_index:
            duplicates = duplicates.reset_index(drop=True)
        return duplicates
