import hashlib
import pandas as pd
from typing import List

from pandaflow.core.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class HashRule(BaseRule):
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

    def validate_rule(self, rule_dict):
        return HashRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict):
        config = HashRule(**rule)
        # field = rule.get("field")
        # cols = rule.get("source", [])
        missing = [col for col in config.source if col not in df.columns]
        if missing:
            raise ValueError(
                f"Missing columns for hash: {', '.join(missing)}. Found columns: {', '.join(df.columns)}"
            )

        # Generate the hash from selected columns
        df[config.field] = (
            df[config.source]
            .fillna("")
            .astype(str)
            .agg(";".join, axis=1)
            .apply(lambda x: hashlib.md5(x.encode("utf-8")).hexdigest())
        )

        return df
