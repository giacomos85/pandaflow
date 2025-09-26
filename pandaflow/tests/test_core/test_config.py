import pytest
import tempfile
import json
import toml
from pathlib import Path
from pandaflow.core.config import load_config, validate_config


@pytest.fixture
def valid_config_dict():
    return {
        "meta": {"author": "pandaflow team"},
        "match": {"filename": "*.csv"},
        "rules": [
            {"strategy": "drop", "field": "unused_column"},
            {"strategy": "hash", "field": "__md5__", "source": ["A", "B"]},
        ],
    }


def test_validate_config_accepts_valid_schema(valid_config_dict):
    result = validate_config(valid_config_dict)
    assert result.meta["author"] == "pandaflow team"
    assert result.rules[0].strategy == "drop"
    assert isinstance(result.rules[1].strategy, str)


def test_load_json_config(valid_config_dict):
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as f:
        json.dump(valid_config_dict, f)
        f.flush()
        path = f.name

    loaded = load_config(path)
    assert loaded["meta"]["author"] == "pandaflow team"
    Path(path).unlink()


def test_load_toml_config(valid_config_dict):
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".toml", delete=False) as f:
        toml.dump(valid_config_dict, f)
        f.flush()
        path = f.name

    loaded = load_config(path)
    assert loaded["meta"]["author"] == "pandaflow team"
    Path(path).unlink()


def test_load_config_unsupported_extension():
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".yaml", delete=False) as f:
        f.write("meta:\n  author: pandaflow team\n")
        f.flush()
        path = f.name

    with pytest.raises(ValueError, match="Unsupported config format: .yaml"):
        load_config(path)
    Path(path).unlink()
