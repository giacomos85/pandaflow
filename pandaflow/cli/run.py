import click
import time
from pathlib import Path
from watchdog.observers import Observer
from pandaflow.core.config import load_config
from pandaflow.core.transformer import transform
from pandaflow.core.load import load
from pandaflow.core.watcher import CsvEventHandler

from pandaflow.core.extract import extract


@click.command()
@click.option(
    "--input",
    "-i",
    required=False,
    type=click.Path(exists=True, file_okay=True, dir_okay=True),
    help="CSV file or folder to process or watch",
)
@click.option(
    "--output",
    "-o",
    default="-",
    type=click.Path(),
    help="Output file or folder (use '-' for stdout)",
)
@click.option(
    "--config",
    "-c",
    required=True,
    type=click.Path(exists=True),
    help="Path to config file",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["csv", "json"], case_sensitive=False),
    default="csv",
    help="Output format: csv or json",
)
@click.option(
    "--watch",
    "-w",
    is_flag=True,
    help="Watch input for changes and process automatically",
)
def run(input, output, config, format, watch):
    config_data = load_config(Path(config))

    if watch:
        if not input:
            raise Exception("In watch mode, the input must be provided via command line")
        input_path = Path(input)
        output_path = Path(output)
        # Watch mode
        output_path.mkdir(parents=True, exist_ok=True) if output != "-" else None

        # If watching a file, filter by filename
        if input_path.is_file():
            watch_path = input_path.parent
            target_file = input_path.name
        else:
            watch_path = input_path
            target_file = None

        event_handler = CsvEventHandler(
            config=config_data,
            output_dir=output_path,
            output_format=format,
            target_file=target_file,
        )

        observer = Observer()
        observer.schedule(event_handler, path=str(watch_path), recursive=False)
        observer.start()

        click.echo(f"👀 Watching: {input_path}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
            click.echo("🛑 Stopped watching.")
        observer.join()
    else:
        # One-shot mode
        input_df_mapping = extract(config_data, input)
        output_df_mapping = transform(input_df_mapping, config_data, output_path=output)
        load(output_df_mapping, output, output_format=format)
