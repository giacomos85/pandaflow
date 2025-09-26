import pandas as pd

from pandaflow.core.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class ConstantRule(BaseRule):
    field: str
    value: str = ""


class ConstantStrategy(TransformationStrategy):

    meta = {
        "name": "constant",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Sets a specified column to a constant value",
    }

    def validate_rule(self, rule_dict):
        return ConstantRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict):
        config = ConstantRule(**rule)
        df[config.field] = config.value
        return df
