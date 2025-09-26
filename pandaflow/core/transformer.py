from pathlib import Path
from typing import Mapping
from pandaflow.core.factory import StrategyFactory
import pandas as pd


def transform_dataframe(df: pd.DataFrame, config: dict, output_path: Path = None) -> pd.DataFrame | None:
    """Transform CSV input based on config rules.

    Args:
        input_source: Pandas dataframe.
        config: Dictionary containing meta, match, and rules.

    Returns:
        Transformed DataFrame, or None if skipped due to match rules.
    """
    factory = StrategyFactory(config)

    for rule in config.get("rules", {}):
        strategy_name = rule.get("strategy")
        version = rule.get("version", None)

        strategy = factory.get_strategy(strategy_name, version=version)

        if strategy:
            if strategy_name == "lookup_external":
                df = strategy.run(df, rule, output=output_path)
            else:
                df = strategy.run(df, rule)
    return df


def transform_dataframe_mapping(
    input_mapping: Mapping[Path, pd.DataFrame | None], config: dict, output_path: Path = None
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
        df = transform_dataframe(df, config, output_path=output_path)
        results[input_file] = df  # may be None if skipped

    return results
