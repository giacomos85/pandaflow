import csv
import sys
import click
from pathlib import Path

from pandaflow.core.config import load_config
from pandaflow.core.runner import transform_csv_batch


@click.command()
@click.option(
    "--input",
    "-i",
    required=True,
    type=click.Path(exists=True),
    help="CSV file or folder containing CSVs",
)
@click.option(
    "--output",
    "-o",
    default="-",
    type=click.Path(),
    help="Output file or folder (use '-' for stdout)",
)
@click.option("--config", "-c", required=True, type=click.Path(exists=True))
def run(input, output, config):
    input_path = Path(input)
    input_files = (
        [
            input_path,
        ]
        if input_path.is_dir()
        else sorted(input_path.glob("*.csv"))
    )

    if input_path.is_dir():
        is_batch = True
        input_files = sorted(Path(input).glob("*.csv"))

    # Determine output destination
    if output == "-":
        output_path = None  # signal to use stdout
    else:
        output_path = Path(output)
    destination = sys.stdout if output_path is None else output_path
    results = transform_csv_batch(input_files, load_config(config))
    for input_file, df in results.items():
        if df is None:
            continue
        if output_path and output_path.exists() and output_path.is_dir():
            rel_path = (
                input_file.relative_to(input_path) if is_batch else input_file.name
            )
            destination = output_path if not is_batch else output_path / rel_path
        df.to_csv(
            destination, sep=",", index=False, quoting=csv.QUOTE_ALL, quotechar='"'
        )
