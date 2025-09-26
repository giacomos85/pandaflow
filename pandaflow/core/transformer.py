from pathlib import Path
from typing import Dict, Mapping
import pandas as pd

from pandaflow.core.config import PandaFlowConfig


def transform_dataframe(
    df: pd.DataFrame, config: PandaFlowConfig, output_path: Path = None
) -> pd.DataFrame | None:
    """Transform CSV input based on config rules.

    Args:
        input_source: Pandas dataframe.
        config: Dictionary containing meta, match, and rules.

    Returns:
        Transformed DataFrame, or None if skipped due to match rules.
    """
    for rule in config.rules:
        df = rule.run(df)
    return df


def transform(
    input_mapping: Mapping[Path, pd.DataFrame | None],
    config: dict,
    output_path: Path = None,
) -> Dict[Path, pd.DataFrame | None]:
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
