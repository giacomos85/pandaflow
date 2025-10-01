from typing import Literal

from pydantic import Field
from pandaflow.models.config import PandaFlowTransformation
import pandas as pd
from uuid_extension import uuid7

from pandaflow.strategies.base import TransformationStrategy


class UUIDTransformation(PandaFlowTransformation):
    strategy: Literal["uuid"] = Field(
        description="Strategy identifier used to select this transformation. Must be 'uuid'."
    )
    field: str = Field(
        description="Name of the output column that will store the generated UUID value."
    )


class UUIDStrategy(TransformationStrategy):

    meta = {
        "name": "uuid",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": """The **uuid** strategy generates a unique UUIDv7 value for each row in a DataFrame.  
This is useful for assigning identifiers, anonymizing records, or tracking row-level lineage.""",
    }

    strategy_model = UUIDTransformation

    def apply(self, df: pd.DataFrame):

        df[self.config.field] = [str(uuid7()) for _ in range(len(df))]
        return df
