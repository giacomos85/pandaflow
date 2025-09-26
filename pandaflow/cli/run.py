import click
import time
from pathlib import Path
from watchdog.observers import Observer
from pandaflow.core.config import load_config
from pandaflow.core.transformer import transform_dataframe_mapping
from pandaflow.core.writer import writer
from pandaflow.core.watcher import CsvEventHandler

from pandaflow.core.extract import extract


@click.command()
@click.option(
    "--input",
    "-i",
    required=True,
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
    input_path = Path(input)

    if watch:
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
        input_files = extract(input_path, config_data)
        results = transform_dataframe_mapping(
            input_files, config_data, output_path=output
        )
        writer(results, output, output_format=format)
