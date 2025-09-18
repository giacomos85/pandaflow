from pandaflow.utils import get_output_formatter
import pandas as pd

from pandaflow.core.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class CalculateAmountRule(BaseRule):
    formula: str
    output_rule: str = None


class CalculateAmountStrategy(TransformationStrategy):

    meta = {
        "name": "calculate_amount",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "",
    }

    def validate_rule(self, rule_dict):
        return CalculateAmountRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict):
        field = rule.get("field")
        formula = rule.get("formula")

        format_value = get_output_formatter(rule.get("output_rule"))

        df.eval(f"{field} = {formula}", inplace=True)
        format_value = get_output_formatter(rule.get("output_rule"))

        df[field] = df[field].apply(format_value)
        return df
