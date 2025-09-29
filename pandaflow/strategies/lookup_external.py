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

    strategy_model = LookupExternalRule

    def validate_rule(self):
        return LookupExternalRule(**self.config_dict)

    def apply(self, df: pd.DataFrame, output: str = None):
        field = self.config.field
        csv_path = self.config.file.replace("${output}", Path(output).stem)
        csv_path = csv_path.replace("${year}", Path(output).parent.name)
        key_col = self.config.key
        value_col = self.config.value
        source_col = self.config.source or self.config.field

        if not csv_path or not key_col or not value_col:
            raise ValueError(
                f"Missing 'file', 'key', or 'value' in rule for field {field}"
            )

        if not os.path.isfile(csv_path):
            df[field] = self.config.not_found
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
        df[field] = df[source_col].map(lookup_dict).fillna(self.config.not_found)
        return df
