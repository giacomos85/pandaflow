from pandaflow.core.config import BaseRule
import pandas as pd
from uuid_extension import uuid7

from pandaflow.strategies.base import TransformationStrategy


class UUIDRule(BaseRule):
    pass


class UUIDStrategy(TransformationStrategy):

    meta = {
        "name": "uuid",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Generates UUIDv7 values for a specified column",
    }

    def validate_rule(self, rule_dict):
        return UUIDRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict):
        field = rule.get("field")

        df[field] = [str(uuid7()) for _ in range(len(df))]
        return df
