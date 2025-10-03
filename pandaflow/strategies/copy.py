import pandas as pd
from typing import Literal, Optional, Union

from pydantic import Field

from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import PandaFlowTransformation
from pandaflow.utils import get_input_parser
from pandaflow.utils import get_output_formatter


class CopyTransformation(PandaFlowTransformation):
    strategy: Literal["copy"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'copy'."
    )
    field: str = Field(
        description="Name of the output column that will receive the copied or transformed value."
    )
    source: Optional[str] = Field(
        default=None,
        description="Name of the source column to copy from. If None, the field will be filled using 'fillna' or left empty."
    )
    fillna: Optional[Union[float, int]] = Field(
        default=None,
        description="Optional fallback value to use if the source column is missing or contains nulls."
    )
    parser: Optional[str] = Field(
        default=None,
        description="Optional parser function name to apply to the source value before copying."
    )
    formatter: Optional[str] = Field(
        default=None,
        description="Optional formatter function name to apply after parsing, before assigning to the output column."
    )


class CopyStrategy(TransformationStrategy):

    meta = {
        "name": "copy",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": """The **copy** strategy transfers values from one column to another, optionally applying input parsing, output formatting, and fill-in logic.  
It is useful for duplicating fields, normalizing formats, or creating derived columns with fallback behavior.""",
    }

    strategy_model = CopyTransformation

    def apply(self, df: pd.DataFrame):
        col = self.config.source or self.config.field
        if col not in df.columns:
            raise ValueError(
                f"Column '{col}' not found in input data for field {self.config.field}"
            )

        parse_number = get_input_parser(self.config.parser)
        format_value = get_output_formatter(self.config.formatter)

        df[self.config.field] = df[col].apply(parse_number).apply(format_value)

        if self.config.fillna != None:
            df[self.config.field] = (
                df[self.config.field]
                .replace("", self.config.fillna)
                .fillna(self.config.fillna)
            )

        return df
