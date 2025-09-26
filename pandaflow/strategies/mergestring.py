from pandaflow.utils import get_output_formatter
import pandas as pd
from typing import List, Optional

from pandaflow.core.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class MergeFormulaRule(BaseRule):
    field: str
    formula: Optional[str] = None
    output_rule: Optional[str] = None
    source: Optional[List[str]] = None
    separator: Optional[str] = " "


class MergeStringStrategy(TransformationStrategy):

    meta = {
        "name": "merge_formula",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Merges values from multiple columns into one using a formula or concatenation.",
    }

    def validate_rule(self):
        return MergeFormulaRule(**self.config_dict)

    def apply(self, df: pd.DataFrame):
        config = MergeFormulaRule(**self.config_dict)
        field = config.field
        # Get the formula or list of columns to merge
        formula = config.formula  # e.g., "first_name + ' ' + last_name"
        source_cols = config.source

        # Get formatter if needed
        format_value = get_output_formatter(config.output_rule)

        if formula:
            # Use apply with eval-like string formatting
            df[field] = df.apply(lambda row: eval(formula, {}, row.to_dict()), axis=1)
        elif source_cols:
            # Merge specified columns with a separator
            separator = config.separator
            df[field] = (
                df[source_cols].fillna("").astype(str).agg(separator.join, axis=1)
            )
        else:
            raise ValueError("Either 'formula' or 'source' must be provided in rule")

        # Apply formatting if specified
        df[field] = df[field].apply(format_value)

        return df
