from pathlib import Path
import pandas as pd
import csv


def archive_csv_by_date(
    input_path: Path,
    output_dir: Path,
    date_col: str,
    split: str,
    pattern: str,
    verbose: bool = False,
):
    output_dir.mkdir(parents=True, exist_ok=True)

    # Read CSV with date column as string
    df = pd.read_csv(
        input_path,
        dtype={
            date_col: str,
            **{
                col: str
                for col in pd.read_csv(input_path, nrows=0).columns
                if col != date_col
            },
        },
    )

    try:
        parsed_dates = pd.to_datetime(df[date_col], errors="raise")
    except Exception as e:
        raise ValueError(f"Errore parsing colonna '{date_col}': {e}")

    if split == "year":
        df["__year"] = parsed_dates.dt.year
        df["__month"] = 1
    else:
        df["__year"] = parsed_dates.dt.year
        df["__month"] = parsed_dates.dt.month

    for (year, month), group in df.groupby(["__year", "__month"]):
        relative_path = pattern.format(year=year, month=month)
        out_file = output_dir / relative_path
        out_file.parent.mkdir(parents=True, exist_ok=True)

        group.drop(columns=["__year", "__month"]).to_csv(
            out_file, index=False, quoting=csv.QUOTE_ALL, quotechar='"'
        )

        if verbose:
            print(f"Salvati {len(group)} record in {out_file}")
