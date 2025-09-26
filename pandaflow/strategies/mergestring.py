from pandaflow.utils import get_output_formatter
import pandas as pd
from typing import Optional

from pandaflow.core.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class MergeFormulaRule(BaseRule):
    field: str
    formula: str
    output_rule: Optional[str] = None


class MergeStringStrategy(TransformationStrategy):

    meta = {
        "name": "merge_formula",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Merges values from multiple columns into one using a formula or concatenation.",
    }

    def validate_rule(self, rule_dict):
        return MergeFormulaRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict):
        field = rule.get("field")
        # Get the formula or list of columns to merge
        formula = rule.get("formula")  # e.g., "first_name + ' ' + last_name"
        source_cols = rule.get("source", [])

        # Get formatter if needed
        format_value = get_output_formatter(rule.get("output_rule"))

        if formula:
            # Use apply with eval-like string formatting
            df[field] = df.apply(lambda row: eval(formula, {}, row.to_dict()), axis=1)
        elif source_cols:
            # Merge specified columns with a separator
            separator = rule.get("separator", " ")
            df[field] = (
                df[source_cols].fillna("").astype(str).agg(separator.join, axis=1)
            )
        else:
            raise ValueError("Either 'formula' or 'source' must be provided in rule")

        # Apply formatting if specified
        df[field] = df[field].apply(format_value)

        return df
