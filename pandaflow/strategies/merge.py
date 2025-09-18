import pandas as pd
from typing import List, Dict

from pandaflow.core.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class MergeRule(BaseRule):
    source: List[str]
    replace: Dict[str, str] = None
    separator: str = None


class MergeStrategy(TransformationStrategy):

    meta = {
        "name": "merge",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Merges values from multiple columns into one",
    }

    def validate_rule(self, rule_dict):
        return MergeRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict):
        field = rule.get("field")
        cols = rule.get("source", field)
        separator = rule.get("separator", " ")

        # Normalize to list
        if isinstance(cols, str):
            cols = [cols]

        # Validate columns
        for col in cols:
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found in available columns")

        # Apply replacements and remove NaNs
        replaced_cols = []
        for col in cols:
            series = df[col].astype(str)

            # Replace NaN strings with empty string
            series = series.replace("nan", "").replace("NaN", "").replace("None", "")

            if "replace" in rule:
                from_str = rule["replace"].get("from", "")
                to_str = rule["replace"].get("to", "")
                series = series.str.replace(from_str, to_str, regex=False)

            replaced_cols.append(series)

        # Merge into single column
        df[field] = pd.Series(
            [separator.join(filter(None, row)) for row in zip(*replaced_cols)],
            index=df.index,
        )
        return df
