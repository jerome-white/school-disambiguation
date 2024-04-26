import os
import sys
import functools as ft
from pathlib import Path
from dataclasses import dataclass
from argparse import ArgumentParser
from urllib.parse import ParseResult, urlunparse
from multiprocessing import Pool, Queue

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

def func(incoming, outgoing):
    columns = {
        'me_school': 'response',
        'cleaned_school_name': 'target',
    }

    while True:
        location = incoming.get()
        try:
            Logger.info(location)
            df = (pd
                  .read_csv(location.target, usecols=columns)
                  .fillna(pd.NA)
                  .rename(columns=columns))
        except ValueError as err:
            Logger.error(f'{location}: {err}')
            df = None
        outgoing.put(df)

def get(args):
    incoming = Queue()
    outgoing = Queue()
    initargs = (
        outgoing,
        incoming,
    )

    with Pool(args.workers, func, initargs):
        jobs = 0
        for i in sheets(args.sheet_id, args.api_key):
            outgoing.put(i)
            jobs += 1

        for _ in range(jobs):
            df = incoming.get()
            if df is not None:
                yield df

if __name__ == "__main__":
    arguments = ArgumentParser()
    arguments.add_argument('--sheet-id')
    arguments.add_argument(
        '--api-key',
        default=os.environ.get('GOOGLE_API_KEY'),
    )
    arguments.add_argument('--workers', type=int)
    args = arguments.parse_args()

    df = pd.concat(get(args))
    df.to_csv(sys.stdout, index=False)
