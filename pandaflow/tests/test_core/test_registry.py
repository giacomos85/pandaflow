from unittest.mock import MagicMock, patch
from pandaflow.strategies.base import TransformationStrategy
from pandaflow.core.registry import (
    get_registered_strategies,
    load_strategy_classes,
)


class DummyStrategy(TransformationStrategy):
    meta = {
        "name": "dummy",
        "version": "1.2.3",
        "author": "Giacomo",
        "description": "Test strategy",
    }

    def apply(self, df):
        return df


def make_entry_point(name, cls=None, error=None):
    ep = MagicMock()
    ep.name = name
    if error:
        ep.load.side_effect = error
    else:
        ep.load.return_value = cls
    return ep


def test_get_registered_strategies_success():
    ep = make_entry_point("dummy", DummyStrategy)
    with patch("pandaflow.core.registry.entry_points") as ep_func:
        ep_func().select.return_value = [ep]
        result = get_registered_strategies()
        assert result["dummy"]["version"] == "1.2.3"
        assert result["dummy"]["author"] == "Giacomo"


def test_get_registered_strategies_failure():
    ep = make_entry_point("broken", error=ImportError("Boom"))
    with patch("pandaflow.core.registry.entry_points") as ep_func:
        ep_func().select.return_value = [ep]
        result = get_registered_strategies()
        assert "error" in result["broken"]
        assert "Boom" in result["broken"]["error"]


def test_load_strategy_classes_success():
    ep = make_entry_point("dummy", DummyStrategy)
    with patch("pandaflow.core.registry.entry_points") as ep_func:
        ep_func().select.return_value = [ep]
        registry = load_strategy_classes()
        assert registry["dummy"]["1.2.3"] is DummyStrategy


def test_load_strategy_classes_invalid_type(capsys):
    class NotAStrategy:
        pass

    ep = make_entry_point("invalid", NotAStrategy)
    with patch("pandaflow.core.registry.entry_points") as ep_func:
        ep_func().select.return_value = [ep]
        registry = load_strategy_classes()
        captured = capsys.readouterr()
        assert "not a valid TransformationStrategy" in captured.out
        assert registry == {}


def test_load_strategy_classes_broken(capsys):
    ep = make_entry_point("broken", error=ImportError("Boom"))
    with patch("pandaflow.core.registry.entry_points") as ep_func:
        ep_func().select.return_value = [ep]
        registry = load_strategy_classes()
        captured = capsys.readouterr()
        assert "Failed to load strategy" in captured.out
        assert registry == {}
