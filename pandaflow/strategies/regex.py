import re
from pandaflow.utils import get_output_formatter
import pandas as pd
from typing import Dict

from pandaflow.models.config import BaseRule
from pandaflow.strategies.base import TransformationStrategy


class RegexRule(BaseRule):
    field: str
    source: str
    regex: str
    group_id: int
    replace: Dict[str, str] = None
    output_rule: str = None


class RegExStrategy(TransformationStrategy):

    meta = {
        "name": "regex",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Extracts data from a column using a regular expression",
    }

    strategy_model = RegexRule

    def validate_rule(self):
        return RegexRule(**self.config_dict)

    def apply(self, df: pd.DataFrame):

        source_col = self.config.source

        format_value = get_output_formatter(self.config.output_rule)

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
