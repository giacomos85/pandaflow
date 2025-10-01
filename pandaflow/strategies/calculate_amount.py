from typing import Literal, Optional

from pydantic import Field
from pandaflow.utils import get_output_formatter
import pandas as pd

from pandaflow.models.config import PandaFlowTransformation
from pandaflow.strategies.base import TransformationStrategy


class CalculateAmountTransformation(PandaFlowTransformation):
    strategy: Literal["calculate_amount"] = Field(
        description="strategy name, must be 'calculate_amount'",
        pattern=r"calculate_amount",
    )
    formula: str = Field(
        description='A pandas-compatible expression (e.g. `"price * quantity"`)'
    )
    formatter: Optional[str] = Field(None, description='Optional formatter (e.g. `"float_2dec"`)')
    field: str = Field(None, description="The target column to store the result")


class CalculateAmountStrategy(TransformationStrategy):

    meta = {
        "name": "calculate_amount",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": """The **calculate_amount** strategy computes a new column using a pandas-compatible formula.  
It’s ideal for deriving values from existing columns, applying arithmetic, or generating totals.""",
    }

    strategy_model = CalculateAmountTransformation

    def apply(self, df: pd.DataFrame):

        format_value = get_output_formatter(self.config.formatter)

        df.eval(f"{self.config.field} = {self.config.formula}", inplace=True)
        format_value = get_output_formatter(self.config.formatter)

        df[self.config.field] = df[self.config.field].apply(format_value)
        return df
