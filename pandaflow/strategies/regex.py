import re

from pydantic import Field
from pandaflow.utils import get_output_formatter
import pandas as pd
from typing import Dict, Literal, Optional

from pandaflow.models.config import PandaFlowTransformation
from pandaflow.strategies.base import TransformationStrategy


class RegexTransformation(PandaFlowTransformation):
    strategy: Literal["regex"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'regex'."
    )
    field: str = Field(
        description="Name of the output column that will store the extracted or transformed value."
    )
    source: str = Field(
        description="Name of the source column whose values will be processed using the regular expression."
    )
    regex: str = Field(
        description="Regular expression pattern to apply to the source column's values."
    )
    group_id: int = Field(
        description="Index of the capturing group to extract from the regex match."
    )
    replace: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional dictionary of string replacements to apply after regex extraction. Keys are substrings to replace; values are their replacements."
    )
    formatter: Optional[str] = Field(
        default=None,
        description="Optional formatter function name to apply after regex and replacement steps."
    )


class RegExStrategy(TransformationStrategy):

    meta = {
        "name": "regex",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": """The **regex** strategy extracts data from a column using a regular expression.  
It’s ideal for parsing structured strings, extracting identifiers, or cleaning up noisy fields.""",
    }

    strategy_model = RegexTransformation

    def apply(self, df: pd.DataFrame):

        source_col = self.config.source

        format_value = get_output_formatter(self.config.formatter)

        if source_col not in df.columns:
            raise ValueError(
                f"Columns '{source_col}' not found in input data for field {self.config.field}"
            )

        def match_regex(row):
            text = str(row[source_col])
            try:
                res = re.match(self.config.regex, text)
                if res:
                    return format_value(res.group(self.config.group_id))
                return ""
            except IndexError:
                return ""

        df[self.config.field] = df.apply(match_regex, axis=1)
        return df
