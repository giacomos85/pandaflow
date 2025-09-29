import pandas as pd

from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import BaseRule


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

    strategy_model = ReplaceRule

    def validate_rule(self):
        return ReplaceRule(**self.config_dict)

    def apply(self, df: pd.DataFrame):

        if self.config.field not in df.columns:
            raise ValueError(
                f"Column '{self.config.field}' not found in input data for replacement"
            )

        find = self.config.find
        replace = self.config.replace

        df[self.config.field] = (
            df[self.config.field].astype(str).str.replace(find, replace)
        )
        return df
