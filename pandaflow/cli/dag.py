import click
from pathlib import Path
from pandaflow.core.config import load_config


@click.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to config file")
@click.option("--ascii", is_flag=True, help="Print DAG in terminal-friendly format")
def dag(config, ascii):
    """
    Print the transformation DAG from config.
    """
    cfg = load_config(Path(config))
    steps = []

    for i, tr in enumerate(cfg.transformations):
        node_id = f"step_{i}_{tr.meta.get("name")}"
        depends_on = f"step_{i-1}_{cfg.transformations[i-1].meta.get("name")}" if i > 0 else "None"
        steps.append((node_id, tr.meta.get("name"), depends_on))

    if ascii:
        click.echo("📊 Pandaflow Transformation DAG\n")
        for node_id, strategy, depends_on in steps:
            click.echo(f"{node_id:<25} ← {depends_on:<25} [{strategy}]")
    else:
        click.echo("ℹ️ Use --ascii to print the DAG in terminal format.")
