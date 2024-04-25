import os
import sys
import functools as ft
from pathlib import Path
from argparse import ArgumentParser
from urllib.parse import ParseResult, urlunparse

import pandas as pd
from googleapiclient.discovery import build

from myutils import Logger

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

    for s in gsheet.get('sheets'):
        doc_id = str(s['properties']['sheetId'])
        yield url(sheet_id, doc_id)

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
            Logger.info(u)
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
