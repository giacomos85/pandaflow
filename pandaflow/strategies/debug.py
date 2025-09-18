import pandas as pd

from pandaflow.strategies.base import TransformationStrategy


class DebugStrategy(TransformationStrategy):

    meta = {
        "name": "debug",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "A strategy that prints debug information",
    }

    def apply(self, df: pd.DataFrame, rule: dict):
        field = rule.get("field")
        print(f"Debugging field: {field}")
        print(df.head())
