import csv
import argparse
from datetime import datetime
from random import randint, choice

def generate_csv(filename: str, num_rows: int):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Header
        writer.writerow(['id', 'timestamp', 'value', 'category'])

        for i in range(1, num_rows + 1):
            row = [
                i,
                datetime.utcnow().isoformat(),
                randint(0, 1000000),
                choice(['A', 'B', 'C', 'D'])
            ]
            writer.writerow(row)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a large CSV file.")
    parser.add_argument('--rows', type=int, required=True, help='Number of rows to generate')
    parser.add_argument('--output', type=str, default='output.csv', help='Output CSV filename')
    args = parser.parse_args()

    print(f"Generating {args.rows:,} rows into {args.output}...")
    generate_csv(args.output, args.rows)
    print("Done.")
