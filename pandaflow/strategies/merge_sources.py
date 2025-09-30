from typing import List, Optional
import pandas as pd
from pydantic import Field
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.models.config import BaseRule


class MergeSourcesRule(BaseRule):
    strategy: str = Field("merge_sources", const=True)
    version: str = Field("1.0.0", const=True)
    sources: Optional[List[int]] = Field(
        None, description="Indices of data_sources to merge. If None, merge all."
    )
    how: str = Field("outer", description="Merge method: inner, outer, left, right")
    on: Optional[List[str]] = Field(
        None, description="Column(s) to join on. If None, use index."
    )


class MergeSourcesStrategy(TransformationStrategy):
    meta = {
        "name": "merge_sources",
        "version": "1.0.0",
        "author": "Giacomo",
        "description": "Merge multiple data sources before applying downstream rules"
    }

    rule_class = MergeSourcesRule

    def apply(self, df: pd.DataFrame, rule: MergeSourcesRule) -> pd.DataFrame:
        sources = self.config.data_sources or []
        selected = (
            [sources[i] for i in rule.sources]
            if rule.sources else sources
        )

        frames = []
        for source in selected:
            if source.type == "csv":
                frame = pd.read_csv(source.path, sep=source.sep, skiprows=source.skiprows)
            elif source.type == "json":
                frame = pd.read_json(source.path)
            elif source.type == "excel":
                frame = pd.read_excel(source.path)
            else:
                raise ValueError(f"Unsupported source type: {source.type}")
            frames.append(frame)

        if not frames:
            raise ValueError("No data sources loaded.")

        merged = frames[0]
        for frame in frames[1:]:
            merged = pd.merge(merged, frame, how=rule.how, on=rule.on)

        return merged
