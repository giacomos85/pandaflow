import click
from pandaflow.core.registry import get_registered_strategies


@click.command()
def list_strategies():
    """List all registered transformation strategies"""
    strategies = get_registered_strategies()
    if not strategies:
        click.echo("⚠️ No strategies registered under 'pandaflow.strategies'.")
        return

    click.echo("📦 Registered Strategies:\n")
    for s in strategies:
        if "error" in s:
            click.echo(f" ❌ Failed to load '{s['name']}': {s['error']}")
        else:
            click.echo(f"🔹 {s['name']}")
            click.echo(f"   ↳ Version: {s['version']}")
            click.echo(f"   ↳ Author: {s['author']}")
            click.echo(f"   ↳ Description: {s['description']}\n")
