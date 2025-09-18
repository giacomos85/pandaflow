import pandas as pd

from pandaflow.strategies.base import TransformationStrategy
from pandaflow.core.config import BaseRule


class DeDuplicateRule(BaseRule):
    pass


class DeDuplicateStrategy(TransformationStrategy):

    meta = {
        "name": "deduplicate",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Keeps only the last occurrence of duplicate rows based on specified columns",
    }

    def validate_rule(self, rule_dict):
        return DeDuplicateRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict):
        field = rule.get("field")
        # Determine which columns to check for duplicates
        subset = rule.get("subset", [field])

        # Create a mask: True for all but the last occurrence
        mask = df.duplicated(subset=subset, keep="last")

        # Invert the mask to keep only the last occurrence
        df_filtered = df[~mask].copy()

        return df_filtered
