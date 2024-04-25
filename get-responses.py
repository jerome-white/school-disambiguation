import os
import sys
from argparse import ArgumentParser

import pandas as pd

from myutils import sheets

def extract(urls):
    columns = {
        'me_school': 'response',
        'cleaned_school_name': 'target',
    }
    ncol = len(columns)

    for u in urls:
        df = (pd
              .read_csv(u)
              .filter(items=columns))
        if len(df.columns) == ncol:
            yield df.rename(columns=columns)

if __name__ == "__main__":
    arguments = ArgumentParser()
    arguments.add_argument('--sheet-id')
    args = arguments.parse_args()

    objs = extract(sheets(args.sheet_id, os.environ['GOOGLE_API_KEY']))
    df = (pd
          .concat(objs)
          .dropna(how='all')
          .drop_duplicates())
    df.to_csv(sys.stdout, index=False)
