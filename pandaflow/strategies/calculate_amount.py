from typing import Literal
from pandaflow.utils import get_output_formatter
import pandas as pd

from pandaflow.models.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class CalculateAmountRule(BaseRule):
    strategy: Literal["calculate_amount"]
    formula: str
    output_rule: str = None
    field: str


class CalculateAmountStrategy(TransformationStrategy):

    meta = {
        "name": "calculate_amount",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "",
    }

    strategy_model = CalculateAmountRule

    def validate_rule(self):
        return CalculateAmountRule(**self.config_dict)

    def apply(self, df: pd.DataFrame):

        format_value = get_output_formatter(self.config.output_rule)

        df.eval(f"{self.config.field} = {self.config.formula}", inplace=True)
        format_value = get_output_formatter(self.config.output_rule)

        df[self.config.field] = df[self.config.field].apply(format_value)
        return df
