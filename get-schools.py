import os
import sys
from argparse import ArgumentParser

import pandas as pd

from myutils import sheets

if __name__ == "__main__":
    arguments = ArgumentParser()
    arguments.add_argument('--sheet-id')
    args = arguments.parse_args()

    (url, ) = sheets(args.sheet_id, os.environ['GOOGLE_API_KEY'])

    df = pd.read_csv(url)
    view = df['school_name']

    view.to_csv(sys.stdout, header=False, index=False)
