from typing import Literal
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

        result = df[self.config.numerator] / df[self.config.denominator]
        if self.config.round_digits is not None:
            result = result.round(self.config.round_digits)

        df[self.config.field] = result
        return df
