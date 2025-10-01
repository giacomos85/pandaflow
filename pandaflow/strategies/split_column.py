from typing import Literal
import pandas as pd
from pydantic import Field
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import PandaFlowTransformation


class SplitColumnTransformation(PandaFlowTransformation):
    strategy: Literal["split_column"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'split_column'."
    )
    column: str = Field(
        description="Name of the column whose string values will be split using the specified delimiter."
    )
    delimiter: str = Field(
        description="Delimiter string used to split the column values (e.g., ',' or '|')."
    )
    maxsplit: int = Field(
        default=-1,
        description="Maximum number of splits to perform. Use -1 for no limit."
    )
    prefix: str = Field(
        default="split",
        description="Prefix to use when naming the new columns created from the split operation."
    )
    drop_original: bool = Field(
        default=False,
        description="Whether to drop the original column after splitting. Defaults to False."
    )


class SplitColumnStrategy(TransformationStrategy):

    meta = {
        "name": "split_column",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": """Splits a string column into multiple columns using a delimiter."""    
    }

    strategy_model = SplitColumnTransformation

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:

        if self.config.column not in df.columns:
            raise ValueError(f"Column '{self.config.column}' not found in DataFrame")

        split_cols = (
            df[self.config.column]
            .astype(str)
            .str.split(
                self.config.delimiter,
                n=self.config.maxsplit if self.config.maxsplit >= 0 else None,
                expand=True,
            )
        )
        split_cols.columns = [
            f"{self.config.prefix}_{i}" for i in range(split_cols.shape[1])
        ]

        result = pd.concat(
            [
                (
                    df.drop(columns=[self.config.column])
                    if self.config.drop_original
                    else df
                ),
                split_cols,
            ],
            axis=1,
        )
        return result
