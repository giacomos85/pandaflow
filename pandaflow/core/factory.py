from pandaflow.core.registry import load_strategies
from pandaflow.strategies.base import TransformationStrategy


class StrategyFactory:
    def __init__(self, config):
        self.config = config
        self.strategies = load_strategies()

    def get_strategy(
        self, rule_type: str, version: str = None
    ) -> TransformationStrategy:
        versions = self.strategies.get(rule_type)
        if not versions:
            raise ValueError(f"Strategy '{rule_type}' not found.")

        if version:
            strategy = versions.get(version)
            if not strategy:
                raise ValueError(
                    f"Version '{version}' of strategy '{rule_type}' not found."
                )
            return strategy

        # Default to highest version (semantic sort)
        sorted_versions = sorted(versions.keys(), reverse=True)
        return versions[sorted_versions[0]]
