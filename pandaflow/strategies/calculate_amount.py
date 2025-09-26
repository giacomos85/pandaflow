from pandaflow.utils import get_output_formatter
import pandas as pd

from pandaflow.core.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class CalculateAmountRule(BaseRule):
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

    def validate_rule(self):
        return CalculateAmountRule(**self.config_dict)

    def apply(self, df: pd.DataFrame):
        config = CalculateAmountRule(**self.config_dict)

        format_value = get_output_formatter(config.output_rule)

        df.eval(f"{config.field} = {config.formula}", inplace=True)
        format_value = get_output_formatter(config.output_rule)

        df[config.field] = df[config.field].apply(format_value)
        return df
