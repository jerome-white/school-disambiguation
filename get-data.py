import os
import sys
import functools as ft
from pathlib import Path
from dataclasses import dataclass
from argparse import ArgumentParser
from urllib.parse import ParseResult, urlunparse

import pandas as pd
from googleapiclient.discovery import build

from myutils import Logger

@dataclass
class SheetLocation:
    name: str
    target: str

    def __str__(self):
        return self.name

#
#
#
@ft.cache
def _url():
    kwargs = { x: None for x in ParseResult._fields }
    p_result = ParseResult(**kwargs)

    return p_result._replace(
        scheme='https',
        netloc='docs.google.com',
    )

def url(sheet_id, doc_id):
    path = Path(
        '/',
        'spreadsheets',
        'd',
        sheet_id,
        'export',
    )
    q_parts = {
        'format': 'csv',
        'gid': doc_id,
    }
    query = '&'.join(map('='.join, q_parts.items()))

    target = _url()
    target = target._replace(path=str(path), query=query)

    return urlunparse(target)

def sheets(sheet_id, token):
    service = build('sheets', 'v4', developerKey=token)
    gsheet = (service
              .spreadsheets()
              .get(spreadsheetId=sheet_id)
              .execute())
    assert sheet_id == gsheet['spreadsheetId']

    for i in gsheet.get('sheets'):
        s = i['properties']
        u = url(sheet_id, str(s['sheetId']))
        yield SheetLocation(s['title'], u)

def extract(locations):
    columns = {
        'me_school': 'response',
        'cleaned_school_name': 'target',
    }

    for i in locations:
        try:
            df = pd.read_csv(i.target, usecols=columns)
        except ValueError as err:
            Logger.error(f'{i}: {err}')
            continue

        Logger.info(i)
        yield (df
               .dropna(how='all')
               .fillna(pd.NA)
               .rename(columns=columns))

if __name__ == "__main__":
    arguments = ArgumentParser()
    arguments.add_argument('--sheet-id')
    args = arguments.parse_args()

    objs = extract(sheets(args.sheet_id, os.environ['GOOGLE_API_KEY']))
    df = (pd
          .concat(objs)
          .drop_duplicates())
    df.to_csv(sys.stdout, index=False)
