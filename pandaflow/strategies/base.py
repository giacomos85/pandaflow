import pandas as pd


class TransformationStrategy:

    meta = {
        "name": "base_strategy",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Base transformation strategy",
    }

    def __init__(self, config_dict: dict):
        self.config_dict = config_dict

    def run(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        if hasattr(self,"strategy_model"):
            self.config = self.strategy_model(**self.config_dict)
        df_copy = df.copy()
        df_copy = self.apply(df_copy, **kwargs)
        return df_copy

    def check(self, config: dict, rule: dict):
        try:
            self.validate_rule(rule)
        except Exception as e:
            raise ValueError(f"Invalid rule configuration: {rule} {rule}. Error: {e}")
        return True

    def validate_rule(self, rule_dict):
        raise NotImplementedError("Must implement validate_rule method")

    def apply(self, df: pd.DataFrame, rule: dict) -> pd.DataFrame:
        raise NotImplementedError("Must implement apply method")
