import json
import click
from pandaflow.core.registry import load_strategy_classes
from pandaflow.models.config import PandaFlowConfig


@click.command()
@click.option(
    "--config",
    is_flag=True,
    help="Include top-level config schema with injected strategy transformations.",
)
@click.option(
    "--output", type=click.Path(), help="Optional path to save schema as JSON."
)
def dump(config, output):
    """
    Dump strategy rule schemas or full config schema.
    """
    registry = load_strategy_classes()
    schemas = {}

    for name, versions in registry.items():
        for version, strategy_cls in versions.items():
            rule_cls = getattr(strategy_cls, "strategy_model", None)
            if rule_cls:
                key = f"{name}:{version}"
                schemas[key] = rule_cls.model_json_schema()

    if config:
        base = PandaFlowConfig.model_json_schema()
        base.setdefault("components", {}).setdefault("schemas", {}).update(schemas)

        # Inject oneOf into transformations field
        rule_refs = [{"$ref": f"#/components/schemas/{key}"} for key in schemas]
        base["properties"]["transformations"] = {
            "type": "array",
            "items": {"oneOf": rule_refs},
        }
        output_schema = base
    else:
        output_schema = schemas

    if output:
        with open(output, "w") as f:
            json.dump(output_schema, f, indent=2)
        click.echo(f"✅ Schema written to {output}")
    else:
        click.echo(json.dumps(output_schema, indent=2))
