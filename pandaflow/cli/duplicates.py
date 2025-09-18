import click
import pandas as pd
import csv
from pathlib import Path


@click.command()
@click.option("--input", "-i", required=True, type=click.Path(exists=True))
@click.option("--output_dir", "-o", required=True, type=click.Path(file_okay=False))
@click.option("--key-col", "-k", required=True, type=click.STRING)
def duplicates(input, output_dir, key_col):
    """
    Check for duplicates in INPUT_CSV based on KEY_COLUMN.
    Saves duplicates into OUTPUT_DIR/duplicates.csv.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input, dtype={key_col: str})
    if key_col not in df.columns:
        raise click.ClickException(
            f"Column '{key_col}' not found. Available columns: {', '.join(df.columns)}"
        )

    duplicates = df[df.duplicated(subset=[key_col], keep=False)]
    if not duplicates.empty:
        out_file = output_path / "duplicates.csv"
        duplicates.to_csv(out_file, index=False, quoting=csv.QUOTE_ALL, quotechar='"')
        click.echo(f"Trovati {len(duplicates)} record duplicati. Salvati in {out_file}")
    else:
        click.echo("Nessun record duplicato trovato.")
