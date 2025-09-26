from unittest.mock import MagicMock, patch
from pandaflow.core.registry import get_registered_strategies, load_strategies
from pandaflow.strategies.base import TransformationStrategy


# Dummy strategy class
class DummyStrategy(TransformationStrategy):
    meta = {
        "name": "dummy",
        "version": "1.2.3",
        "author": "Test Author",
        "description": "A dummy strategy",
    }


# Entry point mock factory
def make_entry_point(name="dummy", cls=DummyStrategy, broken=False):
    ep = MagicMock()
    ep.name = name
    if broken:
        ep.load.side_effect = ImportError("Failed to load")
    else:
        ep.load.return_value = cls
    return ep


# ---------- get_registered_strategies ----------
@patch("pandaflow.core.registry.entry_points")
def test_get_registered_strategies_success(mock_entry_points):
    mock_entry_points.return_value.select.return_value = [make_entry_point()]
    strategies = get_registered_strategies()
    assert len(strategies.items()) == 1
    assert "dummy" in strategies
    assert strategies["dummy"]["version"] == "1.2.3"


# ---------- load_strategies ----------
@patch("pandaflow.core.registry.entry_points")
def test_load_strategies_success(mock_entry_points):
    mock_entry_points.return_value.select.return_value = [make_entry_point()]
    registry = load_strategies()
    assert "dummy" in registry
    assert "1.2.3" in registry["dummy"]
    assert isinstance(registry["dummy"]["1.2.3"], DummyStrategy)


@patch("pandaflow.core.registry.entry_points")
def test_load_strategies_invalid_type(mock_entry_points):
    class NotAStrategy:
        meta = {"name": "bad", "version": "0.0.1"}

    ep = make_entry_point(name="bad", cls=NotAStrategy)
    mock_entry_points.return_value.select.return_value = [ep]

    registry = load_strategies()
    assert "bad" not in registry  # Should be skipped due to TypeError


@patch("pandaflow.core.registry.entry_points")
def test_load_strategies_attribute_error_fallback(mock_entry_points):
    mock_entry_points.return_value.select.side_effect = AttributeError()
    mock_entry_points.return_value.get.return_value = [make_entry_point()]
    registry = load_strategies()
    assert "dummy" in registry
