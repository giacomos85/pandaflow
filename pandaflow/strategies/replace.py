import pandas as pd

from pandaflow.strategies.base import TransformationStrategy
from pandaflow.core.config import BaseRule


class ReplaceRule(BaseRule):
    field: str
    find: str | float
    replace: str


class ReplaceStrategy(TransformationStrategy):

    meta = {
        "name": "replace",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Replaces occurrences of a substring in a specified column with another substring",
    }

    def validate_rule(self):
        return ReplaceRule(**self.config_dict)

    def apply(self, df: pd.DataFrame):
        config = ReplaceRule(**self.config_dict)

        if config.field not in df.columns:
            raise ValueError(
                f"Column '{config.field}' not found in input data for replacement"
            )

        find = config.find
        replace = config.replace

        df[config.field] = df[config.field].astype(str).str.replace(find, replace)
        return df
