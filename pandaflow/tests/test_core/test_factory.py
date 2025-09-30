import pytest
from unittest.mock import patch
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.core.factory import StrategyFactory


class DummyStrategy(TransformationStrategy):
    meta = {"name": "dummy", "version": "1.2.3", "author": "Giacomo"}

    def apply(self, df, rule):
        return df


class OlderStrategy(TransformationStrategy):
    meta = {"name": "dummy", "version": "1.0.0", "author": "Giacomo"}

    def apply(self, df, rule):
        return df


def mock_registry():
    return {"dummy": {"1.0.0": OlderStrategy, "1.2.3": DummyStrategy}}


def test_get_strategy_latest_version():
    with patch(
        "pandaflow.core.factory.load_strategy_classes", return_value=mock_registry()
    ):
        strategy = StrategyFactory.get_strategy({"strategy": "dummy"})
        assert isinstance(strategy, DummyStrategy)


def test_get_strategy_specific_version():
    with patch(
        "pandaflow.core.factory.load_strategy_classes", return_value=mock_registry()
    ):
        strategy = StrategyFactory.get_strategy({"strategy":"dummy", "version":"1.0.0"})
        assert isinstance(strategy, OlderStrategy)


def test_get_strategy_missing_type():
    with patch(
        "pandaflow.core.factory.load_strategy_classes", return_value=mock_registry()
    ):
        with pytest.raises(ValueError, match="Strategy 'unknown' not found."):
            StrategyFactory.get_strategy({"strategy":"unknown"})


def test_get_strategy_missing_version():
    with patch(
        "pandaflow.core.factory.load_strategy_classes", return_value=mock_registry()
    ):
        with pytest.raises(
            ValueError, match="Version '9.9.9' of strategy 'dummy' not found."
        ):
            StrategyFactory.get_strategy({"strategy":"dummy", "version":"9.9.9"})
