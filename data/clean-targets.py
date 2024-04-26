import sys
import csv
from argparse import ArgumentParser

import pandas as pd

def gather(fp, target):
    reader = csv.DictReader(fp)
    for row in reader:
        values = (row
                  .get(target)
                  .strip()
                  .split())
        row[target] = ' '.join(values)

        yield row

if __name__ == "__main__":
    arguments = ArgumentParser()
    arguments.add_argument('--target-column', default='target')
    args = arguments.parse_args()

    records = gather(sys.stdin, args.target_column)
    df = (pd
          .DataFrame
          .from_records(records)
          .dropna(how='all')
          .fillna(pd.NA)
          .drop_duplicates())
    df.to_csv(sys.stdout, index=False)
