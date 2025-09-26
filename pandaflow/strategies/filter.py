from pandaflow.core.config import BaseRule
from pandaflow.utils import get_output_formatter
import pandas as pd

from pandaflow.strategies.base import TransformationStrategy


class FilterByFormulaRule(BaseRule):
    field: str
    formula: str


class FilterByFormulaStrategy(TransformationStrategy):

    meta = {
        "name": "filter",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Filters rows based on a formula",
    }

    def validate_rule(self, rule_dict):
        return FilterByFormulaRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict):
        config = FilterByFormulaRule(**rule)

        try:
            # Evaluate the formula as a boolean mask
            mask = df.eval(config.formula)
            if not mask.dtype == bool:
                raise ValueError("Formula must evaluate to a boolean mask")

            # Filter the DataFrame
            df_filtered = df[mask].copy()

            # Optional: format output field if needed
            format_value = get_output_formatter(rule.get("output_rule"))
            if config.field in df_filtered.columns:
                df_filtered[config.field] = df_filtered[config.field].apply(
                    format_value
                )

            return df_filtered

        except Exception as e:
            print(f"Error applying filter formula: {e}")
            raise e
