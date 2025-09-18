import pandas as pd


class TransformationStrategy:

    meta = {
        "name": "base_strategy",
        "version": "1.0.0",
        "author": "pandaflow team",
        "description": "Base transformation strategy",
    }

    def run(self, df: pd.DataFrame, rule: dict, **kwargs) -> pd.DataFrame:
        df_copy = df.copy()
        self.validate_rule(rule)
        df_copy = self.apply(df_copy, rule, **kwargs)
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
