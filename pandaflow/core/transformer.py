from pathlib import Path
from typing import Mapping
from pandaflow.core.factory import StrategyFactory
import pandas as pd


def transform_dataframe(df: pd.DataFrame, config: dict) -> pd.DataFrame | None:
    """Transform CSV input based on config rules.

    Args:
        input_source: Pandas dataframe.
        config: Dictionary containing meta, match, and rules.

    Returns:
        Transformed DataFrame, or None if skipped due to match rules.
    """
    factory = StrategyFactory(config)

    for rule in config.get("rules", {}):
        field = rule.get("field")
        strategy_name = rule.get("strategy")
        version = rule.get("version", None)

        strategy = factory.get_strategy(strategy_name, version=version)

        if strategy:
            if strategy_name == "csvfile":
                df = strategy.run(df, rule, output=None)
            else:
                df = strategy.run(df, rule)
        else:
            df[field] = None

    return df


def transform_dataframe_mapping(
    input_mapping: Mapping[Path, pd.DataFrame | None], config: dict
) -> Mapping[Path, pd.DataFrame | None]:
    """Pure batch transformation of multiple CSV files.

    Args:
        input_mapping: Dict mapping each input file to its DataFrame.
        config: Dictionary of transformation rules.

    Returns:
        Dict mapping each input file to its transformed DataFrame,
        or None if the file was skipped due to match rules.
    """
    results = {}

    for input_file, df in input_mapping.items():
        df = transform_dataframe(df, config)
        results[input_file] = df  # may be None if skipped

    return results
