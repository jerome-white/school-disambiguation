import sys
from dataclasses import dataclass, asdict

import hmni
import pandas as pd

from myutils import Logger

@dataclass
class Columns:
    left_on: str
    right_on: str

    def __iter__(self):
        yield from (self.left_on, self.right_on)

    def extract(self, df):
        for i in self:
            yield df[i].to_frame()

if __name__ == "__main__":
    columns = Columns('response', 'target')

    df = pd.read_csv(sys.stdin)
    (source, target) = columns.extract(df)
    target = target.drop_duplicates()

    kwargs = asdict(columns)
    matcher = hmni.Matcher(model='latin')
    matched = matcher.fuzzymerge(source,
                                 target,
                                 how='left',
                                 **kwargs)
    Logger.warning(type(matched))
    print(matched)
