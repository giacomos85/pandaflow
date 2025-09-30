from typing import Literal
from pandaflow.utils import get_output_formatter
import pandas as pd

from pandaflow.models.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class CalculateAmountRule(BaseRule):
    strategy: Literal["calculate_amount"]
    formula: str
    formatter: str = None
    field: str


class CalculateAmountStrategy(TransformationStrategy):

    meta = {
        "name": "calculate_amount",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "",
    }

    strategy_model = CalculateAmountRule

    def apply(self, df: pd.DataFrame):

        format_value = get_output_formatter(self.config.formatter)

        df.eval(f"{self.config.field} = {self.config.formula}", inplace=True)
        format_value = get_output_formatter(self.config.formatter)

        df[self.config.field] = df[self.config.field].apply(format_value)
        return df
