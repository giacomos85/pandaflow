import hashlib
import pandas as pd
from typing import List, Literal

from pydantic import Field

from pandaflow.models.config import PandaFlowTransformation
from pandaflow.strategies.base import TransformationStrategy


class HashTransformation(PandaFlowTransformation):
    strategy: Literal["hash"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'hash'."
    )
    field: str = Field(
        description="Name of the output column that will store the computed hash value."
    )
    source: List[str] = Field(
        description="List of column names whose values will be combined and hashed."
    )
    function: str = Field(
        description="Name of the hash function to apply (e.g., 'md5', 'sha256'). Must be supported by the hashing backend."
    )


class HashStrategy(TransformationStrategy):

    meta = {
        "name": "hash",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": """The **hash** strategy generates a hash value from one or more columns in a DataFrame.  
This is useful for creating unique identifiers, anonymizing sensitive fields, or tracking row-level changes.""",
    }

    strategy_model = HashTransformation

    def apply(self, df: pd.DataFrame):
        missing = [col for col in self.config.source if col not in df.columns]
        if missing:
            raise ValueError(
                f"Missing columns for hash: {', '.join(missing)}. Found columns: {', '.join(df.columns)}"
            )

        # Generate the hash from selected columns
        df[self.config.field] = (
            df[self.config.source]
            .fillna("")
            .astype(str)
            .agg(";".join, axis=1)
            .apply(lambda x: hashlib.md5(x.encode("utf-8")).hexdigest())
        )

        return df
