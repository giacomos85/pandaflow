import click
from pathlib import Path
from pandaflow.core.archiver import archive_csv_by_date


@click.command()
@click.option("--input", "-i", required=True, type=click.Path(exists=True))
@click.option("--output_dir", "-o", required=True, type=click.Path(file_okay=False))
@click.option("--date-col", "-d", required=True, type=click.STRING)
@click.option(
    "--split", "-s", required=True, type=click.Choice(["year", "month"]), default="year"
)
@click.option("--pattern", default="{year}/{month:02d}.csv", show_default=True)
def archive(input, output_dir, date_col, split, pattern):
    """
    Split INPUT_CSV into separate files by year based on DATE_COLUMN.
    Saves results into OUTPUT_DIR.
    """
    archive_csv_by_date(Path(input), Path(output_dir), date_col, split, pattern)
