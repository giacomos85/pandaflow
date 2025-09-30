from typing import Literal
import pandas as pd

from pandaflow.models.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class ConstantRule(BaseRule):
    strategy: Literal["constant"]
    field: str
    value: str = ""


class ConstantStrategy(TransformationStrategy):

    meta = {
        "name": "constant",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Sets a specified column to a constant value",
    }

    strategy_model = ConstantRule

    def apply(self, df: pd.DataFrame):
        df[self.config.field] = self.config.value
        return df
