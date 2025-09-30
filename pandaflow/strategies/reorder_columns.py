import pandas as pd
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import PandaFlowTransformation
from typing import List, Literal


class ReorderColumnsTransformation(PandaFlowTransformation):
    strategy: Literal["reorder_columns"]
    columns: List[str]  # Desired column order


class ReorderColumnsStrategy(TransformationStrategy):
    """
    Strategy: reorder_columns
    --------------------------

    Reorders the columns of a DataFrame according to a specified list.

    Metadata:
        - name: "reorder_columns"
        - version: "1.0.0"
        - author: "pandaflow team"

    Transformation Format:
        - columns: List[str] — Desired column order (must match existing columns)

    Example:
        >>> import pandas as pd
        >>> from pandaflow.strategies.reorder_columns import ReorderColumnsStrategy
        >>> df = pd.DataFrame({
        ...     "name": ["Alice", "Bob"],
        ...     "age": [30, 25],
        ...     "email": ["a@example.com", "b@example.com"]
        ... })
        >>> transformation = {
        ...     "strategy": "reorder_columns",
        ...     "columns": ["email", "name", "age"]
        ... }
        >>> result = ReorderColumnsStrategy().apply(df, rule)
        >>> print(result.columns.tolist())
        ['email', 'name', 'age']
    """

    meta = {"name": "reorder_columns", "version": "1.0.0", "author": "pandaflow team"}

    strategy_model = ReorderColumnsTransformation

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [col for col in self.config.columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns in DataFrame: {missing}")
        return df[self.config.columns]
