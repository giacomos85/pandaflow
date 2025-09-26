import pandas as pd
from typing import List, Dict

from pandaflow.core.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class MergeRule(BaseRule):
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

    def validate_rule(self, rule_dict):
        return MergeRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict):
        config = MergeRule(**rule)
        cols = (
            [
                config.source,
            ]
            if isinstance(config.source, str)
            else config.source
        )

        replaced_cols = []
        for col in cols:
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found in available columns")
            series = df[col].astype(str)
            series = series.replace("nan", "").replace("NaN", "").replace("None", "")
            replaced_cols.append(series)

        # Merge into single column
        df[config.field] = pd.Series(
            [config.separator.join(filter(None, row)) for row in zip(*replaced_cols)],
            index=df.index,
        )
        return df
