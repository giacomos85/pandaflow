from typing import Literal, Optional
import numpy as np
from pandaflow.utils import get_output_formatter
import pandas as pd
from pydantic import Field
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import PandaFlowTransformation


class CalculateRatioTransformation(PandaFlowTransformation):
    strategy: Literal["calculate_ratio"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'calculate_ratio'."
    )
    field: str = Field(
        description="Name of the output column that will store the calculated ratio."
    )
    numerator: str = Field(
        description="Name of the column to use as the numerator in the ratio calculation."
    )
    denominator: str = Field(
        description="Name of the column to use as the denominator in the ratio calculation."
    )
    round_digits: int = Field(
        default=None,
        description="Optional number of digits to round the result to. If None, no rounding is applied.",
    )
    formatter: Optional[str] = Field(None, description='Optional formatter (e.g. `"float_2dec"`)')


class CalculateRatioStrategy(TransformationStrategy):

    meta = {
        "name": "calculate_ratio",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": """Calculates the ratio between two numeric columns and stores the result in a new column.""",
    }

    strategy_model = CalculateRatioTransformation

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:

        if self.config.numerator not in df.columns:
            raise ValueError(
                f"Numerator column '{self.config.numerator}' not found in DataFrame"
            )
        if self.config.denominator not in df.columns:
            raise ValueError(
                f"Denominator column '{self.config.denominator}' not found in DataFrame"
            )
        df[self.config.numerator] = pd.to_numeric(df[self.config.numerator])
        df[self.config.denominator] = pd.to_numeric(df[self.config.denominator])
        result = df[self.config.numerator] / df[self.config.denominator]
        if self.config.round_digits is not None:
            result = result.round(self.config.round_digits)

        df[self.config.field] = result
        format_value = get_output_formatter(self.config.formatter)

        df = df.replace([np.inf, -np.inf], "")
        df[self.config.field] = df[self.config.field].apply(format_value)
        return df
