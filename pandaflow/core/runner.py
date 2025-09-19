import re
from pathlib import Path
from typing import Iterable, Mapping
from pandaflow.core.factory import StrategyFactory
import pandas as pd


def rule_matches_file(match: dict, file_path: Path) -> bool:
    # match = rule.get("match", {})
    filename = file_path.name
    full_path = str(file_path)
    match_filename = match.get("filename", None)
    match_glob = match.get("glob", None)
    match_regex = match.get("regex", None)
    if match_filename and filename != match_filename:
        return False
    elif match_glob and not file_path.match(match_glob):
        return False
    elif match_regex and not re.fullmatch(match_regex, full_path):
        return False

    return True


def transform_csv(input_source: Path, config: dict) -> pd.DataFrame | None:
    """Transform CSV input based on config rules.

    Args:
        input_source: Path to a CSV file or a file-like object (e.g. sys.stdin).
        config: Dictionary containing meta, match, and rules.

    Returns:
        Transformed DataFrame, or None if skipped due to match rules.
    """
    meta = config.get("meta", {})
    skiprows = meta.get("skiprows", 0)
    sep = meta.get("csv_separator", ",")
    match = config.get("match", {})

    # If input is a Path, apply match rules
    if isinstance(input_source, Path):
        if not rule_matches_file(match, input_source):
            return None
        df = pd.read_csv(input_source, dtype=str, skiprows=skiprows, sep=sep)
    else:
        # Assume file-like object (e.g. sys.stdin)
        df = pd.read_csv(input_source, dtype=str, skiprows=skiprows, sep=sep)

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


def transform_csv_batch(
    input_files: Iterable[Path], config: dict
) -> Mapping[Path, pd.DataFrame | None]:
    """Pure batch transformation of multiple CSV files.

    Args:
        input_files: List of CSV file paths.
        config: Dictionary of transformation rules.

    Returns:
        Dict mapping each input file to its transformed DataFrame,
        or None if the file was skipped due to match rules.
    """
    results = {}

    for input_file in input_files:
        df = transform_csv(input_file, config)
        results[input_file] = df  # may be None if skipped

    return results
