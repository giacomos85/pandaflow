import pandas as pd
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import BaseRule
from typing import List, Literal


class DropColumnsRule(BaseRule):
    strategy: Literal["drop_columns"]
    columns: List[str]  # List of column names to drop
    errors: str = "raise"  # "raise" or "ignore" if column is missing


class DropColumnsStrategy(TransformationStrategy):
    """
    Strategy: dropcolumns
    ----------------------

    Drops one or more columns from the DataFrame.

    Metadata:
        - name: "dropcolumns"
        - version: "1.0.0"
        - author: "pandaflow team"

    Rule Format:
        - columns: List[str] — List of column names to drop
        - errors: Optional[str] — "raise" or "ignore" if column is missing (default: "raise")

    Example:
        >>> import pandas as pd
        >>> from pandaflow.strategies.dropcolumns import DropColumnsStrategy
        >>> df = pd.DataFrame({
        ...     "name": ["Alice", "Bob"],
        ...     "age": [30, 25],
        ...     "gender": ["F", "M"]
        ... })
        >>> rule = {
        ...     "strategy": "dropcolumns",
        ...     "columns": ["gender"]
        ... }
        >>> result = DropColumnsStrategy().apply(df, rule)
        >>> print(result.columns.tolist())
        ['name', 'age']
    """

    meta = {"name": "drop_columns", "version": "1.0.0", "author": "pandaflow team"}

    strategy_model = DropColumnsRule

    def validate_rule(self):
        return DropColumnsRule(**self.config_dict)

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns=self.config.columns, errors=self.config.errors)
