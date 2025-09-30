from pandaflow import __version__
import click
from pandaflow.cli.run import run
from pandaflow.cli.archive import archive
from pandaflow.cli.duplicates import duplicates
from pandaflow.cli.strategies import strategies
from pandaflow.cli.schema import dump


@click.group()
@click.version_option(__version__, prog_name="Pandaflow")
def cli():
    """Utility per elaborare file di testo con regex."""
    pass


cli.add_command(run)
cli.add_command(archive)
cli.add_command(duplicates)
cli.add_command(strategies)
cli.add_command(dump)
