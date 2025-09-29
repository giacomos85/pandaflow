import pandas as pd
from typing import Optional, Union

from pandaflow.strategies.base import TransformationStrategy
from pandaflow.core.config import BaseRule
from pandaflow.utils import get_input_parser
from pandaflow.utils import get_output_formatter


class CopyRule(BaseRule):
    field: str
    source: str | None
    fillna: Optional[Union[float, int]] = None
    input_rule: Optional[str] = None
    output_rule: Optional[str] = None


class CopyStrategy(TransformationStrategy):

    meta = {
        "name": "copy",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Copies values from one column to another",
    }

    strategy_model = CopyRule

    def validate_rule(self):
        return CopyRule(**self.config_dict)

    def apply(self, df: pd.DataFrame):
        col = self.config.source or self.config.field
        if col not in df.columns:
            raise ValueError(
                f"Column '{col}' not found in input data for field {self.config.field}"
            )

        parse_number = get_input_parser(self.config.input_rule)
        format_value = get_output_formatter(self.config.output_rule)

        df[self.config.field] = df[col].apply(parse_number).apply(format_value)

        if self.config.fillna:
            df[self.config.field] = (
                df[self.config.field].replace("", self.config.fillna).fillna(self.config.fillna)
            )

        return df
