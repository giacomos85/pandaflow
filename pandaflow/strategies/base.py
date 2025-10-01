import pandas as pd


class TransformationStrategy:

    meta = {
        "name": "base_strategy",
        "version": "1.0.0",
        "author": "PandaFlow Team",
        "description": "Base transformation strategy",
    }

    def __init__(self, config_dict: dict):
        self.config_dict = config_dict
        if hasattr(self, "strategy_model"):
            self.config = self.strategy_model(**self.config_dict)

    def run(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        df_copy = df.copy()
        df_copy = self.apply(df_copy, **kwargs)
        return df_copy

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("Must implement apply method")
