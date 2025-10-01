import os
from pathlib import Path
from typing import Literal, Optional
import pandas as pd
from pydantic import Field

from pandaflow.models.config import PandaFlowTransformation
from pandaflow.strategies.base import TransformationStrategy


class LookupExternalTransformation(PandaFlowTransformation):
    strategy: Literal["lookup_external"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'lookup_external'."
    )
    field: str = Field(
        description="Name of the output column that will store the looked-up value."
    )
    source: str = Field(
        description="Name of the column in the current DataFrame whose values will be used as lookup keys."
    )
    file: str = Field(
        description="Path to the external file containing the lookup table (e.g., CSV or JSON)."
    )
    key: str = Field(
        description="Name of the column in the external file that contains the lookup keys."
    )
    value: str = Field(
        description="Name of the column in the external file that contains the values to assign."
    )
    not_found: Optional[str] = Field(
        default=None,
        description="Optional fallback value to assign when a key is not found in the external file. If None, missing keys will result in nulls."
    )


class LookupExternalStrategy(TransformationStrategy):

    meta = {
        "name": "lookup_external",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": "Looks up values from an external CSV file based on a key column",
    }

    strategy_model = LookupExternalTransformation

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
