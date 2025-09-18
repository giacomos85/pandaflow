import pandas as pd

from pandaflow.strategies.base import TransformationStrategy
from pandaflow.core.config import BaseRule


class ReplaceRule(BaseRule):
    find: str | float
    replace: str


class ReplaceStrategy(TransformationStrategy):

    meta = {
        "name": "replace",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Replaces occurrences of a substring in a specified column with another substring",
    }

    def validate_rule(self, rule_dict):
        return ReplaceRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict):
        field = rule.get("field")
        if field not in df.columns:
            raise ValueError(
                f"Column '{field}' not found in input data for replacement"
            )

        find = rule.get("find", "")
        replace = rule.get("replace", "")

        df[field] = df[field].astype(str).str.replace(find, replace)
        return df
