from importlib.metadata import entry_points

from pandaflow.strategies.base import TransformationStrategy


def get_registered_strategies(group: str = "pandaflow.strategies"):
    try:
        eps = entry_points().select(group=group)
    except AttributeError:
        eps = entry_points().get(group, [])

    strategies = {}
    for ep in sorted(eps, key=lambda e: e.name):
        try:
            cls = ep.load()
            meta = getattr(cls, "meta", {})
            strategies[ep.name] = {
                "version": meta.get("version", "N/A"),
                "author": meta.get("author", "N/A"),
                "description": meta.get("description", "N/A"),
            }
        except Exception as e:
            strategies[ep.name] = {
                "name": ep.name,
                "error": str(e),
            }

    return strategies


def load_strategies():
    registry = {}
    try:
        eps = entry_points().select(group="pandaflow.strategies")
    except AttributeError:
        eps = entry_points().get("pandaflow.strategies", [])

    for ep in sorted(eps, key=lambda e: e.name):
        try:
            strategy_cls = ep.load()
            if not issubclass(strategy_cls, TransformationStrategy):
                raise TypeError(f"{ep.name} is not a valid TransformationStrategy")

            meta = getattr(strategy_cls, "meta", {})
            name = meta.get("name", ep.name)
            version = meta.get("version", "0.0.0")

            # Instantiate strategy
            instance = strategy_cls()

            # Store by name and version
            if name not in registry:  # pragma: no cover
                registry[name] = {}
            registry[name][version] = instance

        except Exception as e:
            print(f"⚠️ Failed to load strategy '{ep.name}': {e}")

    return registry
