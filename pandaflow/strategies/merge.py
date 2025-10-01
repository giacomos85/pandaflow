import pandas as pd
from typing import List, Dict, Literal, Optional, Union

from pydantic import Field

from pandaflow.models.config import PandaFlowTransformation
from pandaflow.strategies.base import TransformationStrategy


class MergeTransformation(PandaFlowTransformation):
    strategy: Literal["merge"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'merge'."
    )
    field: str = Field(
        description="Name of the output column that will store the merged result."
    )
    source: Union[str, List[str]] = Field(
        description="Single column name or list of column names whose values will be merged."
    )
    replace: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional dictionary of string replacements to apply before merging. Keys are substrings to replace; values are their replacements."
    )
    separator: str = Field(
        default=" ",
        description="String used to separate values when merging multiple columns. Defaults to a single space."
    )


class MergeStrategy(TransformationStrategy):

    meta = {
        "name": "merge",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": "Merges values from multiple columns into one",
    }

    strategy_model = MergeTransformation

    def apply(self, df: pd.DataFrame):
        cols = (
            [
                self.config.source,
            ]
            if isinstance(self.config.source, str)
            else self.config.source
        )

        replaced_cols = []
        for col in cols:
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found in available columns")
            series = df[col].astype(str)
            series = series.replace("nan", "").replace("NaN", "").replace("None", "")
            replaced_cols.append(series)

        # Merge into single column
        df[self.config.field] = pd.Series(
            [
                self.config.separator.join(filter(None, row))
                for row in zip(*replaced_cols)
            ],
            index=df.index,
        )
        return df
