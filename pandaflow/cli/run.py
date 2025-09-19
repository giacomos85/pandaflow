import click

from pandaflow.core.config import load_config

from pandaflow.core.reader import read_csvs
from pandaflow.core.transformer import transform_dataframe_mapping
from pandaflow.core.writer import writer


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
@click.option(
    "--format", "-f",
    type=click.Choice(["csv", "json"], case_sensitive=False),
    default="csv",
    help="Output format: csv or json"
)
def run(input, output, config, format):
    input_files = read_csvs(input, load_config(config))
    results = transform_dataframe_mapping(input_files, load_config(config))
    writer(results, output, output_format=format)
