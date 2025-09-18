import pytest
import pandas as pd
from click.testing import CliRunner
from pandaflow.cli.main import cli


@pytest.fixture
def runner():
    return CliRunner()


# ---------- duplicates ----------
def test_duplicates_found(runner, tmp_path):
    input_file = tmp_path / "data.csv"
    df = pd.DataFrame({"id": [1, 2, 2, 3]})
    df.to_csv(input_file, index=False)
    output_dir = tmp_path / "out"

    result = runner.invoke(
        cli,
        ["duplicates", "-i", str(input_file), "-o", str(output_dir), "-k", "id"],
    )
    assert result.exit_code == 0
    assert "Trovati 2 record duplicati" in result.output
    assert (output_dir / "duplicates.csv").exists()


def test_duplicates_none_found(runner, tmp_path):
    input_file = tmp_path / "data.csv"
    df = pd.DataFrame({"id": [1, 2, 3]})
    df.to_csv(input_file, index=False)
    output_dir = tmp_path / "out"

    result = runner.invoke(
        cli,
        ["duplicates", "-i", str(input_file), "-o", str(output_dir), "-k", "id"],
    )
    assert result.exit_code == 0
    assert "Nessun record duplicato trovato" in result.output


def test_duplicates_missing_column(runner, tmp_path):
    input_file = tmp_path / "data.csv"
    df = pd.DataFrame({"name": ["a", "b"]})
    df.to_csv(input_file, index=False)
    output_dir = tmp_path / "out"

    result = runner.invoke(
        cli,
        ["duplicates", "-i", str(input_file), "-o", str(output_dir), "-k", "id"],
    )
    assert result.exit_code != 0
    assert "Column 'id' not found" in result.output
