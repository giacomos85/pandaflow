from pathlib import Path
import re
from pandaflow.core.factory import StrategyFactory
import pandas as pd
import csv
from pandaflow.core.config import load_config


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


def process_single_csv(
    input_file: Path, output_file: Path, config: dict, verbose: bool = False
):
    meta = config.get("meta", {})
    skiprows = meta.get("skiprows", 0)
    sep = meta.get("csv_separator", ",")
    match = config.get("match", {})

    if not rule_matches_file(match, input_file):
        # if verbose:
        print(f"Skipping {input_file} due to match rules")
        return

    df = pd.read_csv(input_file, dtype=str, skiprows=skiprows, sep=sep)

    for rule in config.get("rules", {}):
        field = rule.get("field")
        strategy_name = rule.get("strategy")
        version = rule.get("version", None)

        factory = StrategyFactory(config)
        strategy = factory.get_strategy(strategy_name, version=version)

        try:
            if strategy and strategy_name != "csvfile":
                df = strategy.run(df, rule)
            elif strategy and strategy_name == "csvfile":  # pragma: no cover
                df = strategy.run(df, rule, output=str(output_file))
            else:
                df[field] = None
        except Exception as e:
            raise Exception(
                f"Error while processing file {input_file} rule:{rule} {str(e)}"
            )
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, sep=",", index=False, quoting=csv.QUOTE_ALL, quotechar='"')

    if verbose:
        print(f"Saved to {output_file}")


def process_csvs(
    input_path: Path, output_path: Path, config_path: Path, verbose: bool = False
):
    config = load_config(config_path)

    is_batch = input_path.is_dir()
    input_files = list(input_path.rglob("*.csv")) if is_batch else [input_path]

    # Validate output path
    if is_batch:
        if output_path.exists() and not output_path.is_dir():
            raise ValueError(f"Expected output to be a folder, got file: {output_path}")
        output_path.mkdir(parents=True, exist_ok=True)
    else:
        if output_path.exists() and output_path.is_dir():
            raise ValueError(f"Expected output to be a file, got folder: {output_path}")

    for input_file in input_files:
        rel_path = input_file.relative_to(input_path) if is_batch else input_file.name
        out_file = output_path if not is_batch else output_path / rel_path

        process_single_csv(input_file, out_file, config, verbose=verbose)
