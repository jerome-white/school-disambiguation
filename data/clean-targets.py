import sys
import csv
import string
from argparse import ArgumentParser

import pandas as pd

def standardize(word):
    for i in string.punctuation:
        word = word.replace(i, '')
    parts = (word
             .casefold()
             .strip()
             .split())

    return ' '.join(parts)

def gather(fp, target):
    reader = csv.DictReader(fp)
    for row in reader:
        yield { x: standardize(y) for (x, y) in row.items() }

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
