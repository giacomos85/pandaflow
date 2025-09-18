import click
from pathlib import Path
from pandaflow.core.config import load_config
from pandaflow.core.factory import StrategyFactory


@click.command()
@click.option("--config-path", "-c", required=True, type=click.Path(exists=True))
def check(config_path):
    config = load_config(Path(config_path))
    for rule in config.get("rules", []):
        rule_type = rule.get("strategy")
        version = rule.get("version", None)
        factory = StrategyFactory(config)
        try:
            strategy = factory.get_strategy(rule_type, version=version)
            strategy.check(config_path, rule)
        except ValueError as e:
            raise click.ClickException(
                f"Exception while validating {config_path} - {rule} - {str(e)}"
            )
