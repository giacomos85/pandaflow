import pandas as pd
from pydantic import Field
from typing import List, Literal, Optional
from pandaflow.models.config import PandaFlowTransformation
from pandaflow.strategies.base import TransformationStrategy

class CastColumnsTransformation(PandaFlowTransformation):
    strategy: Literal["cast_columns"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'cast_columns'.",
        example="cast_columns"
    )
    fields: List[str] = Field(
        description="List of column names to cast.",
        example=["amount", "discount"]
    )
    target_type: Literal["int", "float", "str", "bool"] = Field(
        description="Target type to cast each column to.",
        example="float"
    )
    errors: Literal["raise", "coerce", "ignore"] = Field(
        default="raise",
        description="Error handling mode: 'raise' for strict, 'coerce' to convert invalid values to NaN, 'ignore' to skip errors.",
        example="coerce"
    )
    fallback: Optional[str] = Field(
        default=None,
        description="Optional fallback value to fill in for failed conversions (e.g., 0, '').",
        example="0"
    )

class CastColumnsStrategy(TransformationStrategy):

    meta = {
        "name": "cast_columns",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": """The **cast_columns** strategy converts multiple columns to a specified type with optional error handling and fallback values.  
It supports numeric, string, and boolean casting and is useful for preparing data for filtering, merging, or exporting."""
    }

    strategy_model = CastColumnsTransformation

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        for field in self.config.fields:
            if field not in df.columns:
                raise KeyError(f"Column '{field}' not found in DataFrame")

            try:
                if self.config.target_type == "str":
                    casted = df[field].astype(str)
                elif self.config.target_type == "bool":
                    casted = df[field].astype(bool)
                elif self.config.target_type in ["int", "float"]:
                    casted = pd.to_numeric(df[field], errors=self.config.errors)
                    if self.config.fallback is not None:
                        casted = casted.fillna(self.config.fallback)
                else:
                    raise ValueError(f"Unsupported target_type: {self.config.target_type}")

                df[field] = casted

            except Exception as e:
                raise TypeError(f"Failed to cast column '{field}' to {self.config.target_type}: {e}")

        return df