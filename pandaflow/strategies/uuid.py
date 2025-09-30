from typing import Literal
from pandaflow.models.config import BaseRule
import pandas as pd
from uuid_extension import uuid7

from pandaflow.strategies.base import TransformationStrategy


class UUIDRule(BaseRule):
    strategy: Literal["uuid"]
    field: str


class UUIDStrategy(TransformationStrategy):

    meta = {
        "name": "uuid",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Generates UUIDv7 values for a specified column",
    }

    strategy_model = UUIDRule

    def apply(self, df: pd.DataFrame):

        df[self.config.field] = [str(uuid7()) for _ in range(len(df))]
        return df
