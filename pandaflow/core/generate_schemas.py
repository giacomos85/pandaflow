from pandaflow.core.registry import load_strategy_classes


def generate_strategy_schemas():
    registry = load_strategy_classes()
    schemas = {}

    for name, versions in registry.items():
        for version, strategy_cls in versions.items():
            rule_cls = getattr(strategy_cls, "rule_class", None)
            if rule_cls:
                key = f"{name}:{version}"
                schemas[key] = rule_cls.model_json_schema()
    return schemas
