import click
import time
from pathlib import Path
from watchdog.observers import Observer
from pandaflow.core.config import load_config
from pandaflow.core.watcher import CsvEventHandler


@click.command()
@click.option(
    "--folder", "-f", required=True, type=click.Path(exists=True, file_okay=False)
)
@click.option("--output", "-o", required=True, type=click.Path(file_okay=False))
@click.option("--config", "-c", required=True, type=click.Path(exists=True))
@click.option("--verbose", "-v", is_flag=True)
def watch(folder, output, config, verbose):
    """
    Watch a folder for new or modified CSV files and process them automatically.
    """
    config_data = load_config(Path(config))
    output_dir = Path(output)
    output_dir.mkdir(parents=True, exist_ok=True)

    event_handler = CsvEventHandler(
        config=config_data, output_dir=output_dir, verbose=verbose
    )
    observer = Observer()
    observer.schedule(event_handler, path=folder, recursive=False)
    observer.start()

    click.echo(f"👀 Watching folder: {folder}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        click.echo("🛑 Stopped watching.")
    observer.join()
