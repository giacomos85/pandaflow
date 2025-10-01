from typing import Literal, Optional

from pydantic import Field
from pandaflow.models.config import PandaFlowTransformation
from pandaflow.utils import get_output_formatter
import pandas as pd

from pandaflow.strategies.base import TransformationStrategy


class FilterByFormulaTransformation(PandaFlowTransformation):
    strategy: Literal["filter"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'filter'."
    )
    field: str = Field(
        description="Name of the column to apply the filtering formula to."
    )
    formula: str = Field(
        description="Filtering expression to evaluate. Should be a valid pandas-compatible formula (e.g., 'value > 10')."
    )
    formatter: Optional[str] = Field(
        default=None,
        description="Optional formatter function name to apply to the field before evaluating the formula."
    )

class FilterByFormulaStrategy(TransformationStrategy):

    meta = {
        "name": "filter",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": """The **filter** strategy selects rows from a DataFrame based on a user-defined formula.  
It supports logical expressions involving column values and can optionally format the output field.""",
    }

    strategy_model = FilterByFormulaTransformation

    def apply(self, df: pd.DataFrame):

        try:
            # Evaluate the formula as a boolean mask
            mask = df.eval(self.config.formula)
            if not mask.dtype == bool:
                raise ValueError("Formula must evaluate to a boolean mask")

            # Filter the DataFrame
            df_filtered = df[mask].copy()

            # Optional: format output field if needed
            format_value = get_output_formatter(self.config.formatter)
            if self.config.field in df_filtered.columns:
                df_filtered[self.config.field] = df_filtered[self.config.field].apply(
                    format_value
                )

            return df_filtered

        except Exception as e:
            print(f"Error applying filter formula: {e}")
            raise e
