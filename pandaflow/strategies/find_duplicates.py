import pandas as pd
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import PandaFlowTransformation
from typing import Literal, Optional, List


class FindDuplicatesTransformation(PandaFlowTransformation):
    strategy: Literal["find_duplicates"]
    subset: Optional[List[str]] = None  # Columns to consider for identifying duplicates
    keep: Optional[str] = "first"  # "first", "last", or False
    reset_index: Optional[bool] = False  # Whether to reset the index after dropping


class FindDuplicatesStrategy(TransformationStrategy):

    meta = {"name": "find_duplicates", "version": "1.0.0", "author": "pandaflow team"}

    strategy_model = FindDuplicatesTransformation

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.drop_duplicates(subset=self.config.subset, keep=self.config.keep)
        duplicates = df[df.duplicated(subset=self.config.subset, keep=False)]
        if self.config.reset_index:
            duplicates = duplicates.reset_index(drop=True)
        return duplicates
