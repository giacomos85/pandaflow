import hashlib
import pandas as pd
from typing import List, Literal

from pandaflow.models.config import PandaFlowTransformation
from pandaflow.strategies.base import TransformationStrategy


class HashTransformation(PandaFlowTransformation):
    strategy: Literal["hash"]
    field: str
    source: List[str]
    function: str


class HashStrategy(TransformationStrategy):

    meta = {
        "name": "hash",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Generates a hash from specified columns",
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
