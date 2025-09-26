import pandas as pd
from typing import Optional

from pandaflow.core.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.utils import get_input_parser
from pandaflow.utils import get_output_formatter


class CalculateUnitCostRule(BaseRule):
    field: str
    total: str
    quantity: str
    input_rule: Optional[str] = None
    output_rule: Optional[str] = None


class UnitCostStrategy(TransformationStrategy):

    meta = {
        "name": "calculate_unitcost",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Calculates unit cost by dividing total by quantity",
    }

    def validate_rule(self, rule_dict):
        return CalculateUnitCostRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict):
        config = CalculateUnitCostRule(**rule)

        if not config.total or not config.quantity:
            raise ValueError(
                f"Invalid configuration for unit cost calculation in field {config.field}"
            )

        parse_number = get_input_parser(rule.get("input_rule"))
        format_value = get_output_formatter(rule.get("output_rule"))

        def calculate_unitcost(row):
            total = parse_number(row[config.total])
            quantity = parse_number(row[config.quantity])
            if quantity == 0:
                return format_value(0.0)
            return format_value(abs(total / quantity))

        df[config.field] = df.apply(calculate_unitcost, axis=1)
        return df
