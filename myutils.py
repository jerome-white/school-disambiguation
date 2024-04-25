import functools as ft
from pathlib import Path
from urllib.parse import ParseResult, urlunparse

from googleapiclient.discovery import build

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
        yield url(args.sheet_id, doc_id)
