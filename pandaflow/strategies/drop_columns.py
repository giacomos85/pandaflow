import pandas as pd
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.core.config import BaseRule
from typing import List

class DropColumnsRule(BaseRule):
    columns: List[str]           # List of column names to drop
    errors: str = "raise"        # "raise" or "ignore" if column is missing

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

    meta = {
        "name": "drop_columns",
        "version": "1.0.0",
        "author": "pandaflow team"
    }

    def validate_rule(self, rule_dict):
        return DropColumnsRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict) -> pd.DataFrame:
        config = DropColumnsRule(**rule)
        return df.drop(columns=config.columns, errors=config.errors)
