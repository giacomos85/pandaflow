import csv
from pathlib import Path
import sys
from typing import Mapping

import pandas as pd


def writer(
    mapping: Mapping[Path, pd.DataFrame | None], output: str, output_format: str = "csv"
):
    # Determine output destination
    output_path = Path(output) if output != "-" else None
    destination = sys.stdout if output_path is None else output_path
    for input_path, df in mapping.items():
        if df is None:
            continue
        if output_path and output_path.exists() and output_path.is_dir():
            filename = input_path.name
            destination = output_path / filename
        if output_format == "csv":
            df.to_csv(
                destination, sep=",", index=False, quoting=csv.QUOTE_ALL, quotechar='"'
            )
        elif output_format == "json":
            if output_path:
                destination = destination.with_suffix(".json")
            df.to_json(destination, orient="records", lines=True)
