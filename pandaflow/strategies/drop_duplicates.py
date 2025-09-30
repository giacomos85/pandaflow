import pandas as pd
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import BaseRule
from typing import Literal, Optional, List


class DropDuplicatesRule(BaseRule):
    strategy: Literal["drop_duplicates"]
    subset: Optional[List[str]] = None  # Columns to consider for identifying duplicates
    keep: Optional[str] = "first"  # "first", "last", or False
    reset_index: Optional[bool] = False  # Whether to reset the index after dropping


class DropDuplicatesStrategy(TransformationStrategy):
    """
    Strategy: drop_duplicates
    --------------------------

    Drops duplicate rows from a DataFrame, optionally based on a subset of columns.

    Metadata:
        - name: "drop_duplicates"
        - version: "1.0.0"
        - author: "pandaflow team"

    Rule Format:
        - subset: Optional[List[str]] — Columns to consider for identifying duplicates
        - keep: Optional[str] — "first", "last", or False (default: "first")
        - reset_index: Optional[bool] — Whether to reset the index after dropping

    Example:
        >>> import pandas as pd
        >>> from pandaflow.strategies.drop_duplicates import DropDuplicatesStrategy
        >>> df = pd.DataFrame({
        ...     "name": ["Alice", "Bob", "Alice", "Charlie"],
        ...     "age": [30, 25, 30, 40]
        ... })
        >>> rule = {
        ...     "strategy": "drop_duplicates",
        ...     "subset": ["name", "age"],
        ...     "keep": "first",
        ...     "reset_index": True
        ... }
        >>> result = DropDuplicatesStrategy().apply(df, rule)
        >>> print(result)
           name  age
        0  Alice   30
        1   Bob   25
        2 Charlie   40
    """

    meta = {"name": "drop_duplicates", "version": "1.0.0", "author": "pandaflow team"}

    strategy_model = DropDuplicatesRule

    def validate_rule(self):
        return DropDuplicatesRule(**self.config_dict)

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.drop_duplicates(subset=self.config.subset, keep=self.config.keep)
        if self.config.reset_index:
            result = result.reset_index(drop=True)
        return result
