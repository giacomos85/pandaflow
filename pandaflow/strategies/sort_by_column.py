import pandas as pd
from pydantic import Field
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import PandaFlowTransformation
from typing import List, Literal, Optional


class SortByColumnTransformation(PandaFlowTransformation):
    strategy: Literal["sort_by_column"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'sort_by_column'."
    )
    columns: List[str] = Field(
        description="List of column names to sort by, in order of precedence."
    )
    ascending: Optional[List[bool]] = Field(
        default=None,
        description="List of booleans indicating sort direction for each column. True for ascending, False for descending. If None, defaults to ascending for all."
    )
    na_position: Literal["first", "last"] = Field(
        default="last",
        description="Position for NaN values in the sort order. 'first' places NaNs at the top, 'last' at the bottom."
    )


class SortByColumnStrategy(TransformationStrategy):

    meta = {
        "name": "sort_by_column",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": """Sorts a DataFrame by one or more columns, with optional direction and NaN positioning."""
    }

    strategy_model = SortByColumnTransformation

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        asc = (
            self.config.ascending
            if self.config.ascending
            else [True] * len(self.config.columns)
        )
        if len(asc) != len(self.config.columns):
            raise ValueError("Length of 'ascending' must match 'columns'")
        return df.sort_values(
            by=self.config.columns, ascending=asc, na_position=self.config.na_position
        )
