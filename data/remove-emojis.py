import sys
import csv

import emoji

from mylib import Logger

def rm_emo(target):
    for (k, v) in target.items():
        value = emoji.replace_emoji(v, replace='').strip()
        yield (k, value)

if __name__ == "__main__":
    reader = csv.DictReader(sys.stdin)
    writer = csv.DictWriter(sys.stdout, fieldnames=reader.fieldnames)
    writer.writeheader()

    for row in reader:
        record = dict(rm_emo(row))
        if not any(record.values()):
            Logger.warning(f'Excluding: "{row}"')
            continue
        writer.writerow(record)
