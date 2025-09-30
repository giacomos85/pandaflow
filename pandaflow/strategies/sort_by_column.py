import pandas as pd
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import PandaFlowTransformation
from typing import List, Literal


class SortByColumnTransformation(PandaFlowTransformation):
    strategy: Literal["sort_by_column"]
    columns: List[str]
    ascending: List[bool] = None
    na_position: Literal["first", "last"] = "last"


class SortByColumnStrategy(TransformationStrategy):

    meta = {"name": "sort_by_column", "version": "1.0.0", "author": "pandaflow team"}

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
