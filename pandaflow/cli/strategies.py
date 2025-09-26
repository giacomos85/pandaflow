import click
from pandaflow.core.registry import get_registered_strategies


@click.command()
@click.option("--strategy", "-s", default="", show_default=True)
def strategies(strategy):
    """List all registered transformation strategies"""
    strategies = get_registered_strategies()
    if not strategies:
        click.echo("⚠️ No strategies registered under 'pandaflow.strategies'.")
        return
    if strategy and strategy in strategies:
        s = strategies.get(strategy)
        click.echo(f"🔹 {strategy}")
        click.echo(f"   ↳ Version: {s['version']}")
        click.echo(f"   ↳ Author: {s['author']}")
        click.echo(f"   ↳ Description: {s['description']}\n")
    else:
        for name, s in strategies.items():
            if "error" in s:
                click.echo(f" ❌ Failed to load '{s['name']}': {s['error']}")
            else:
                click.echo(f"{name}")
                # click.echo(f"   ↳ Version: {s['version']}")
                # click.echo(f"   ↳ Author: {s['author']}")
                # click.echo(f"   ↳ Description: {s['description']}\n")
