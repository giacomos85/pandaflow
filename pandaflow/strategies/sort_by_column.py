import pandas as pd
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.core.config import BaseRule
from typing import List, Literal


class SortByColumnRule(BaseRule):
    columns: List[str]
    ascending: List[bool] = None
    na_position: Literal["first", "last"] = "last"


class SortByColumnStrategy(TransformationStrategy):

    meta = {"name": "sort_by_column", "version": "1.0.0", "author": "pandaflow team"}

    def validate_rule(self, rule_dict):
        return SortByColumnRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict) -> pd.DataFrame:
        config = SortByColumnRule(**rule)
        asc = config.ascending if config.ascending else [True] * len(config.columns)
        if len(asc) != len(config.columns):
            raise ValueError("Length of 'ascending' must match 'columns'")
        return df.sort_values(
            by=config.columns, ascending=asc, na_position=config.na_position
        )
