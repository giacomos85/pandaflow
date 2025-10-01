from typing import Literal, Union
import pandas as pd
from pydantic import Field

from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import PandaFlowTransformation


class ReplaceTransformation(PandaFlowTransformation):
    strategy: Literal["replace"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'replace'."
    )
    field: str = Field(
        description="Name of the column in which the replacement will be performed."
    )
    find: Union[str, float] = Field(
        description="Value to search for in the specified column. Can be a string or numeric value."
    )
    replace: str = Field(
        description="Replacement value to assign when a match with 'find' is found."
    )


class ReplaceStrategy(TransformationStrategy):

    meta = {
        "name": "replace",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": """The **replace** strategy searches for a substring or value in a column and replaces it with another string.  
This is useful for cleaning up labels, standardizing values, or removing unwanted characters.""",
    }

    strategy_model = ReplaceTransformation

    def apply(self, df: pd.DataFrame):

        if self.config.field not in df.columns:
            raise ValueError(
                f"Column '{self.config.field}' not found in input data for replacement"
            )

        find = self.config.find
        replace = self.config.replace

        df[self.config.field] = (
            df[self.config.field].astype(str).str.replace(find, replace)
        )
        return df
