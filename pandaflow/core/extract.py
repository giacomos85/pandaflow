from pathlib import Path
import re
from typing import Dict

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


def read_csv(input_path: str, config: dict) -> pd.DataFrame | None:
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
    input_source = Path(input_path)
    if isinstance(input_source, Path) and not rule_matches_file(match, input_source):
        return None
    df = pd.read_csv(input_source, dtype=str, skiprows=skiprows, sep=sep)
    return df


def extract(input_path: str, config: dict) -> Dict[Path, pd.DataFrame | None]:
    results = {}
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
    return results
