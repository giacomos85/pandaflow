from pandaflow.strategies.base import TransformationStrategy

from pandaflow.core.registry import load_strategy_classes


class StrategyFactory:
    @classmethod
    def get_strategy(self, transformation_dict: dict) -> TransformationStrategy:
        strategies = load_strategy_classes()
        strategy_name = transformation_dict.get("strategy")
        req_version = transformation_dict.get("version", None)
        versions = strategies.get(strategy_name)
        if not versions:
            raise ValueError(f"Strategy '{strategy_name}' not found.")
        if req_version:
            strategy = versions.get(req_version)
            if not strategy:
                raise ValueError(
                    f"Version '{req_version}' of strategy '{strategy_name}' not found."
                )
            return strategy(config_dict=transformation_dict)

        # Default to highest version (semantic sort)
        sorted_versions = sorted(versions.keys(), reverse=True)
        return versions[sorted_versions[0]](config_dict=transformation_dict)
