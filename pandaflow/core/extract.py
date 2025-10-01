from pathlib import Path
import re
from typing import Dict

import pandas as pd


def resolve_input_path(config_path: str, relative_input: str):
    config_dir = Path(config_path).parent
    input_path = config_dir / relative_input
    return input_path.resolve()


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


def read_csv(input_path: str, config: dict) -> pd.DataFrame | None:
    """Transform CSV input based on config transformations.

    Args:
        input_source: Path to a CSV file or a file-like object (e.g. sys.stdin).
        config: Dictionary containing meta, match, and transformations.

    Returns:
        Transformed DataFrame, or None if skipped due to match transformations.
    """
    meta = config.meta
    skiprows = meta.get("skiprows", 0)
    sep = meta.get("csv_separator", ",")
    match = meta.get("match", {})

    # If input is a Path, apply match transformations
    input_source = Path(input_path)
    if isinstance(input_source, Path) and not rule_matches_file(match, input_source):
        return None
    df = pd.read_csv(input_source, dtype=str, skiprows=skiprows, sep=sep)
    return df


def extract(config: dict, input_path: str = None) -> Dict[Path, pd.DataFrame | None]:
    results = {}
    if input_path:
        input_path = Path(input_path)
        input_files = (
            input_path.glob("*.csv")
            if input_path.is_dir()
            else [
                input_path,
            ]
        )

        for input_file in input_files:
            df = read_csv(input_file, config)
            results[input_path] = df  # may be None if skipped
    else:
        results = {}
        for source in config.data_sources:
            input_path = resolve_input_path(config.file_path, source.path)
            if source.type == "csv":
                frame = pd.read_csv(
                    input_path, sep=source.sep, skiprows=source.skiprows
                )
            elif source.type == "json":
                frame = pd.read_json(input_path)
            elif source.type == "excel":
                frame = pd.read_excel(source.path)
            else:
                raise ValueError(f"Unsupported source type: {source.type}")
            results[input_path] = frame
    return results
