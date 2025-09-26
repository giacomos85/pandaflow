import pandas as pd
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.core.config import BaseRule


class SplitColumnRule(BaseRule):
    column: str  # Column to split
    delimiter: str  # Delimiter to use
    maxsplit: int = -1  # Optional: max number of splits (-1 = no limit)
    prefix: str = "split"  # Optional: prefix for new columns
    drop_original: bool = False  # Optional: whether to drop the original column


class SplitColumnStrategy(TransformationStrategy):
    """
    Strategy: split_column
    -----------------------

    Splits a string column into multiple columns using a delimiter.

    Metadata:
        - name: "split_column"
        - version: "1.0.0"
        - author: "pandaflow team"

    Rule Format:
        - column: str — Column to split
        - delimiter: str — Delimiter to use
        - maxsplit: Optional[int] — Max number of splits (-1 = no limit)
        - prefix: Optional[str] — Prefix for new columns (default: "split")
        - drop_original: Optional[bool] — Whether to drop the original column

    Example:
        >>> import pandas as pd
        >>> from pandaflow.strategies.split_column import SplitColumnStrategy
        >>> df = pd.DataFrame({
        ...     "full_name": ["Alice Smith", "Bob Jones"]
        ... })
        >>> rule = {
        ...     "strategy": "split_column",
        ...     "column": "full_name",
        ...     "delimiter": " ",
        ...     "prefix": "name"
        ... }
        >>> result = SplitColumnStrategy().apply(df, rule)
        >>> print(result.columns.tolist())
        ['name_0', 'name_1']
    """

    meta = {
        "name": "split_column",
        "version": "1.0.0",
        "author": "pandaflow team"
    }

    def validate_rule(self, rule_dict):
        return SplitColumnRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict) -> pd.DataFrame:
        config = SplitColumnRule(**rule)
        if config.column not in df.columns:
            raise ValueError(f"Column '{config.column}' not found in DataFrame")

        split_cols = df[config.column].astype(str).str.split(
            config.delimiter, n=config.maxsplit if config.maxsplit >= 0 else None, expand=True
        )
        split_cols.columns = [f"{config.prefix}_{i}" for i in range(split_cols.shape[1])]

        result = pd.concat([df.drop(columns=[config.column]) if config.drop_original else df, split_cols], axis=1)
        return result
