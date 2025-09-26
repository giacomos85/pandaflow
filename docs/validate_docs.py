import json
import pandas as pd
from pathlib import Path
import argparse
from pandaflow.core.transformer import transform_dataframe
from pandaflow.core.config import load_config

RULES_DIR = Path("source/rules")
DATA_DIR = Path("source/data")

def run_strategy(name: str):
    rule_path = RULES_DIR / f"{name}.json"
    input_path = DATA_DIR / f"{name}_input.csv"
    output_path = DATA_DIR / f"{name}_output.csv"

    if not rule_path.exists() or not input_path.exists() or not output_path.exists():
        print(f"❌ Missing files for strategy: {name}")
        return
    config = load_config(rule_path)
    df = pd.read_csv(input_path)
    expected = pd.read_csv(output_path, dtype={**{col: str for col in pd.read_csv(output_path, nrows=0).columns}})

    try:
        result = transform_dataframe(df, config)
        pd.testing.assert_frame_equal(result.reset_index(drop=True).fillna("").astype(str), expected.reset_index(drop=True).fillna("").astype(str))
        print(f"\n✅ [{name}]: output matches expected")
    except Exception as e:
        print(f"\n❌ [{name}]: {str(e)}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", help="Run a single strategy by name")
    args = parser.parse_args()

    if args.name:
        run_strategy(args.name)
    else:
        for rule_file in RULES_DIR.glob("*.json"):
            name = rule_file.stem
            if name in ["index", "uuid"]:
                continue
            run_strategy(name)

if __name__ == "__main__":
    main()
