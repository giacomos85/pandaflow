import pandas as pd
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.core.config import BaseRule


class CalculateRatioRule(BaseRule):
    field: str
    numerator: str  # Column name for numerator
    denominator: str  # Column name for denominator
    round_digits: int = None  # Optional: number of decimal places to round


class CalculateRatioStrategy(TransformationStrategy):
    """
    Strategy: calculate_ratio
    --------------------------

    Calculates the ratio between two numeric columns and stores the result in a new column.

    Metadata:
        - name: "calculate_ratio"
        - version: "1.0.0"
        - author: "pandaflow team"

    Rule Format:
        - numerator: str — Column name for numerator
        - denominator: str — Column name for denominator
        - result_column: Optional[str] — Name of the output column (default: "ratio")
        - round_digits: Optional[int] — Number of decimal places to round to

    Example:
        >>> import pandas as pd
        >>> from pandaflow.strategies.calculate_ratio import CalculateRatioStrategy
        >>> df = pd.DataFrame({
        ...     "sales": [100, 200, 300],
        ...     "cost": [50, 80, 120]
        ... })
        >>> rule = {
        ...     "strategy": "calculate_ratio",
        ...     "numerator": "sales",
        ...     "denominator": "cost",
        ...     "result_column": "margin",
        ...     "round_digits": 2
        ... }
        >>> result = CalculateRatioStrategy().apply(df, rule)
        >>> print(result["margin"].tolist())
        [2.0, 2.5, 2.5]
    """

    meta = {"name": "calculate_ratio", "version": "1.0.0", "author": "pandaflow team"}

    strategy_model = CalculateRatioRule

    def validate_rule(self):
        return CalculateRatioRule(**self.config_dict)

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:

        if self.config.numerator not in df.columns:
            raise ValueError(
                f"Numerator column '{self.config.numerator}' not found in DataFrame"
            )
        if self.config.denominator not in df.columns:
            raise ValueError(
                f"Denominator column '{self.config.denominator}' not found in DataFrame"
            )

        result = df[self.config.numerator] / df[self.config.denominator]
        if self.config.round_digits is not None:
            result = result.round(self.config.round_digits)

        df[self.config.field] = result
        return df
