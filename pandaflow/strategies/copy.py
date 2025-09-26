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

    def validate_rule(self, rule_dict):
        return CopyRule(**rule_dict)

    def apply(self, df: pd.DataFrame, rule: dict):
        config = CopyRule(**rule)
        col = config.source or config.field
        if col not in df.columns:
            raise ValueError(
                f"Column '{col}' not found in input data for field {config.field}"
            )

        parse_number = get_input_parser(rule.get("input_rule"))
        format_value = get_output_formatter(rule.get("output_rule"))

        df[config.field] = df[col].apply(parse_number).apply(format_value)

        if "fillna" in rule:
            fillna_value = rule.get("fillna")
            df[config.field] = (
                df[config.field].replace("", fillna_value).fillna(fillna_value)
            )

        return df
