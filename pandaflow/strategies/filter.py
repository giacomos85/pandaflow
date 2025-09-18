from pandaflow.utils import get_output_formatter
import pandas as pd

from pandaflow.strategies.base import TransformationStrategy


class FilterByFormulaStrategy(TransformationStrategy):

    meta = {
        "name": "filter",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Filters rows based on a formula",
    }

    def apply(self, df: pd.DataFrame, rule: dict):
        field = rule.get("field")
        formula = rule.get("formula")  # e.g., "amount > 0 and category == 'Sales'"
        if not formula:
            raise ValueError("Missing 'formula' in rule")

        try:
            # Evaluate the formula as a boolean mask
            mask = df.eval(formula)
            if not mask.dtype == bool:
                raise ValueError("Formula must evaluate to a boolean mask")

            # Filter the DataFrame
            df_filtered = df[mask].copy()

            # Optional: format output field if needed
            format_value = get_output_formatter(rule.get("output_rule"))
            if field in df_filtered.columns:
                df_filtered[field] = df_filtered[field].apply(format_value)

            return df_filtered

        except Exception as e:
            print(f"Error applying filter formula: {e}")
            raise e
