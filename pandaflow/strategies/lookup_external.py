import os
from pathlib import Path
import pandas as pd

from pandaflow.core.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class LookupExternalRule(BaseRule):
    field: str
    source: str
    file: str
    key: str
    value: str
    not_found: str = None


class LookupExternalStrategy(TransformationStrategy):

    meta = {
        "name": "lookup_external",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Looks up values from an external CSV file based on a key column",
    }

    def validate_rule(self, rule_dict):
        return LookupExternalRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict, output: str = None):
        field = rule.get("field")
        csv_path = rule.get("file", "").replace("${output}", Path(output).stem)
        csv_path = csv_path.replace("${year}", Path(output).parent.name)
        key_col = rule.get("key")
        value_col = rule.get("value")
        source_col = rule.get("source", field)

        if not csv_path or not key_col or not value_col:
            raise ValueError(
                f"Missing 'file', 'key', or 'value' in rule for field {field}"
            )

        if not os.path.isfile(csv_path):
            df[field] = rule.get("not_found", "")
            return df

        df_lookup = pd.read_csv(csv_path, dtype=str)
        if key_col not in df_lookup.columns or value_col not in df_lookup.columns:
            raise ValueError(
                f"Key [{key_col}] or value [{value_col}] column not found in CSV for field {field}. Columns found: {', '.join(df_lookup.columns)}. {csv_path}"
            )

        lookup_dict = pd.Series(
            df_lookup[value_col].values, index=df_lookup[key_col]
        ).to_dict()
        if source_col not in df.columns:
            raise ValueError(
                f"Source column '{source_col}' not found in input data for field {field}"
            )
        df[field] = df[source_col].map(lookup_dict).fillna(rule.get("not_found", ""))
        return df
