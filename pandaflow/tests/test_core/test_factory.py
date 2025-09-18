import pytest
from unittest.mock import patch, MagicMock
from pandaflow.core.factory import StrategyFactory
from pandaflow.strategies.base import TransformationStrategy


# Dummy strategy class
class DummyStrategy(TransformationStrategy):
    meta = {
        "name": "dummy",
        "version": "1.2.3",
        "author": "tester",
        "description": "dummy strategy",
    }


@pytest.fixture
def dummy_registry():
    return {
        "dummy": {"1.0.0": DummyStrategy(), "1.2.3": DummyStrategy()},
        "other": {"0.1.0": DummyStrategy()},
    }


@patch("pandaflow.core.factory.load_strategies")
def test_factory_initialization(mock_loader, dummy_registry):
    mock_loader.return_value = dummy_registry
    factory = StrategyFactory(config={"meta": "test"})
    assert factory.config == {"meta": "test"}
    assert "dummy" in factory.strategies


@patch("pandaflow.core.factory.load_strategies")
def test_get_strategy_by_name_and_version(mock_loader, dummy_registry):
    mock_loader.return_value = dummy_registry
    factory = StrategyFactory(config={})
    strategy = factory.get_strategy("dummy", version="1.0.0")
    assert isinstance(strategy, DummyStrategy)


@patch("pandaflow.core.factory.load_strategies")
def test_get_strategy_by_name_highest_version(mock_loader, dummy_registry):
    mock_loader.return_value = dummy_registry
    factory = StrategyFactory(config={})
    strategy = factory.get_strategy("dummy")
    assert isinstance(strategy, DummyStrategy)


@patch("pandaflow.core.factory.load_strategies")
def test_get_strategy_missing_name_raises(mock_loader, dummy_registry):
    mock_loader.return_value = dummy_registry
    factory = StrategyFactory(config={})
    with pytest.raises(ValueError, match="Strategy 'missing' not found"):
        factory.get_strategy("missing")


@patch("pandaflow.core.factory.load_strategies")
def test_get_strategy_missing_version_raises(mock_loader, dummy_registry):
    mock_loader.return_value = dummy_registry
    factory = StrategyFactory(config={})
    with pytest.raises(
        ValueError, match="Version '9.9.9' of strategy 'dummy' not found"
    ):
        factory.get_strategy("dummy", version="9.9.9")
