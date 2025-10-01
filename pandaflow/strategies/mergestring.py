from pydantic import Field
from pandaflow.utils import get_output_formatter
import pandas as pd
from typing import List, Literal, Optional

from pandaflow.models.config import PandaFlowTransformation
from pandaflow.strategies.base import TransformationStrategy


class MergeFormulaTransformation(PandaFlowTransformation):
    strategy: Literal["merge_formula"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'merge_formula'."
    )
    field: str = Field(
        description="Name of the output column that will store the result of the formula or merged values."
    )
    formula: Optional[str] = Field(
        default=None,
        description="Optional pandas-compatible formula to compute the output value. If provided, overrides source/merge behavior."
    )
    formatter: Optional[str] = Field(
        default=None,
        description="Optional formatter function name to apply to the result after formula or merge."
    )
    source: Optional[List[str]] = Field(
        default=None,
        description="Optional list of column names to merge if no formula is provided."
    )
    separator: Optional[str] = Field(
        default=" ",
        description="String used to separate values when merging multiple columns. Defaults to a single space."
    )


class MergeStringStrategy(TransformationStrategy):

    meta = {
        "name": "merge_formula",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": "Merges values from multiple columns into one using a formula or concatenation.",
    }

    strategy_model = MergeFormulaTransformation

    def apply(self, df: pd.DataFrame):
        field = self.config.field
        # Get the formula or list of columns to merge
        formula = self.config.formula  # e.g., "first_name + ' ' + last_name"
        source_cols = self.config.source

        # Get formatter if needed
        format_value = get_output_formatter(self.config.formatter)

        if formula:
            # Use apply with eval-like string formatting
            df[field] = df.apply(lambda row: eval(formula, {}, row.to_dict()), axis=1)
        elif source_cols:
            # Merge specified columns with a separator
            separator = self.config.separator
            df[field] = (
                df[source_cols].fillna("").astype(str).agg(separator.join, axis=1)
            )
        else:
            raise ValueError("Either 'formula' or 'source' must be provided in rule")

        # Apply formatting if specified
        df[field] = df[field].apply(format_value)

        return df
