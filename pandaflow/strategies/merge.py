import pandas as pd
from typing import List, Dict, Literal

from pandaflow.models.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class MergeRule(BaseRule):
    strategy: Literal["merge"]
    field: str
    source: str | List[str]
    replace: Dict[str, str] = None
    separator: str = " "


class MergeStrategy(TransformationStrategy):

    meta = {
        "name": "merge",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Merges values from multiple columns into one",
    }

    strategy_model = MergeRule

    def apply(self, df: pd.DataFrame):
        cols = (
            [
                self.config.source,
            ]
            if isinstance(self.config.source, str)
            else self.config.source
        )

        replaced_cols = []
        for col in cols:
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found in available columns")
            series = df[col].astype(str)
            series = series.replace("nan", "").replace("NaN", "").replace("None", "")
            replaced_cols.append(series)

        # Merge into single column
        df[self.config.field] = pd.Series(
            [
                self.config.separator.join(filter(None, row))
                for row in zip(*replaced_cols)
            ],
            index=df.index,
        )
        return df
