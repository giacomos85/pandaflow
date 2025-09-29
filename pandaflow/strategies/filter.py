from pandaflow.models.config import BaseRule
from pandaflow.utils import get_output_formatter
import pandas as pd

from pandaflow.strategies.base import TransformationStrategy


class FilterByFormulaRule(BaseRule):
    field: str
    formula: str
    output_rule: str = None


class FilterByFormulaStrategy(TransformationStrategy):

    meta = {
        "name": "filter",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Filters rows based on a formula",
    }

    strategy_model = FilterByFormulaRule

    def validate_rule(self):
        return FilterByFormulaRule(**self.config_dict)

    def apply(self, df: pd.DataFrame):

        try:
            # Evaluate the formula as a boolean mask
            mask = df.eval(self.config.formula)
            if not mask.dtype == bool:
                raise ValueError("Formula must evaluate to a boolean mask")

            # Filter the DataFrame
            df_filtered = df[mask].copy()

            # Optional: format output field if needed
            format_value = get_output_formatter(self.config.output_rule)
            if self.config.field in df_filtered.columns:
                df_filtered[self.config.field] = df_filtered[self.config.field].apply(
                    format_value
                )

            return df_filtered

        except Exception as e:
            print(f"Error applying filter formula: {e}")
            raise e
