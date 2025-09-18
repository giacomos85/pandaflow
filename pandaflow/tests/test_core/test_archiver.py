import pandas as pd
import pytest
from pathlib import Path
from pandaflow.core.archiver import archive_csv_by_date


@pytest.fixture
def sample_csv(tmp_path):
    path = tmp_path / "data.csv"
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "date": ["2023-01-15", "2023-01-20", "2024-03-10", "2024-03-25"],
            "value": ["a", "b", "c", "d"],
        }
    )
    df.to_csv(path, index=False)
    return path


def test_archive_by_month_creates_correct_files(sample_csv, tmp_path):
    output_dir = tmp_path / "out"
    archive_csv_by_date(
        input_path=sample_csv,
        output_dir=output_dir,
        date_col="date",
        split="month",
        pattern="{year}/{month:02d}/archive.csv",
        verbose=True,
    )

    expected_files = [
        output_dir / "2023/01/archive.csv",
        output_dir / "2024/03/archive.csv",
    ]
    for f in expected_files:
        assert f.exists()
        df = pd.read_csv(f)
        assert "date" in df.columns and "value" in df.columns


def test_archive_by_year_creates_correct_structure(sample_csv, tmp_path):
    output_dir = tmp_path / "yearly"
    archive_csv_by_date(
        input_path=sample_csv,
        output_dir=output_dir,
        date_col="date",
        split="year",
        pattern="{year}/archive.csv",
    )

    expected_files = [output_dir / "2023/archive.csv", output_dir / "2024/archive.csv"]
    for f in expected_files:
        assert f.exists()
        df = pd.read_csv(f)
        assert "date" in df.columns and "value" in df.columns


def test_invalid_date_column_raises_error(tmp_path):
    path = tmp_path / "bad.csv"
    df = pd.DataFrame({"id": [1, 2], "wrong_date": ["not-a-date", "still-wrong"]})
    df.to_csv(path, index=False)

    with pytest.raises(ValueError, match="Errore parsing colonna 'date'"):
        archive_csv_by_date(
            input_path=path,
            output_dir=tmp_path / "out",
            date_col="date",
            split="month",
            pattern="{year}/{month}/archive.csv",
        )
