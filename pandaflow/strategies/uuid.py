from pandaflow.models.config import BaseRule
import pandas as pd
from uuid_extension import uuid7

from pandaflow.strategies.base import TransformationStrategy


class UUIDRule(BaseRule):
    field: str


class UUIDStrategy(TransformationStrategy):

    meta = {
        "name": "uuid",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Generates UUIDv7 values for a specified column",
    }

    strategy_model = UUIDRule

    def validate_rule(self):
        return UUIDRule(**self.config_dict)

    def apply(self, df: pd.DataFrame):

        df[self.config.field] = [str(uuid7()) for _ in range(len(df))]
        return df
