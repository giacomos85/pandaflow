import click
from pathlib import Path
from pandaflow.core.runner import process_csvs


@click.command()
@click.option("--input", "-i", default="-", type=click.Path(exists=True))
@click.option("--output", "-o", default="-", type=click.Path())
@click.option("--config", "-c", required=True, type=click.Path(exists=True))
@click.option("--verbose", "-v", is_flag=True)
def run(input, output, config, verbose):
    input_path = Path(input)
    output_path = Path(output)

    if input_path.is_dir() and output_path.exists() and not output_path.is_dir():
        raise click.ClickException(
            "When processing a directory, the output must also be a directory."
        )
    elif output_path.exists() and output_path.is_dir():
        raise click.ClickException(
            "When processing a single file, the output must be a file, not a directory."
        )

    process_csvs(input_path, output_path, Path(config), verbose)
