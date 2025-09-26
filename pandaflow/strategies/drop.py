from typing import List
import pandas as pd

from pandaflow.core.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class DropRule(BaseRule):
    field: str|List[str]


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
        config = DropRule(**rule)
        return df.copy().drop(columns=config.field)
