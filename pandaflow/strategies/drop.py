import pandas as pd
from typing import List

from pandaflow.core.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class DropRule(BaseRule):
    pass


class DropStrategy(TransformationStrategy):

    meta = {
        "name": "drop",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Drops specified columns from the DataFrame",
    }

    def validate_rule(self, rule_dict):
        return DropRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict):
        field = rule.get("field")

        if isinstance(field, str):
            field = [field]

        return df.copy().drop(columns=field)
