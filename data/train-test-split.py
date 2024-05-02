import sys
import itertools as it
import collections as cl
from argparse import ArgumentParser

import pandas as pd
from sklearn.model_selection import train_test_split

from mylib import Logger

class MultiCountGrouper:
    MultiCount = cl.namedtuple('MultiCount', 'df_singles, df_multis')
    _target = 'target'

    def __init__(self, df):
        self.df = df

        counts = df[self._target].value_counts().to_dict()
        self.multi = set(x for (x, y) in counts.items() if y > 1)

    def __call__(self, idx):
        multi = self.df.loc[idx, self._target] in self.multi
        return int(multi)

    @classmethod
    def group(cls, df):
        groups = df.groupby(cls(df), sort=False)
        args = (groups.get_group(x).reset_index(drop=True) for x in range(2))
        return cls.MultiCount(*args)

class SplitManager:
    _train_test = (
        'train',
        'test',
    )

    def __init__(self, train, test):
        values = map(cl.Counter, (train, test))
        self.splits = dict(zip(self._train_test, values))

    def __getitem__(self, key):
        for (k, v) in self.splits.items():
            yield from it.repeat(k, v[key])

#
#
#
class SplitHandler:
    def __init__(self, df, train_ratio):
        self.df = df
        self.train_ratio = train_ratio

    def __iter__(self):
        raise NotImplementedError()

class UniqueSplitHandler(SplitHandler):
    def __init__(self, df, train_ratio):
        super().__init__(df, train_ratio)
        self.n = round(len(self.df) * self.train_ratio)

    def __iter__(self):
        m = len(self.df) - self.n
        splits = ['train'] * self.n + ['test'] * m
        assert len(splits) == len(self.df)

        yield self.df.assign(split=splits)

class StandardSplitHandler(SplitHandler):
    def __init__(self, df, train_ratio, seed):
        super().__init__(df, train_ratio)
        self.kwargs = {
            'train_size': self.train_ratio,
        }
        if args.seed:
            self.kwargs['random_state'] = seed

    def __iter__(self):
        splits = cl.defaultdict(list)
        for (i, j) in self.split():
            splits[i].append(j)

        for (i, g) in self.df.groupby('target', sort=False):
            yield g.assign(split=splits[i])

    def split(self):
        y = self.df['target']
        (*_, y_train, y_test) = train_test_split(self.df['response'],
                                                 y,
                                                 stratify=y,
                                                 **self.kwargs)
        for (i, j) in zip(('train', 'test'), (y_train, y_test)):
            yield from zip(j, it.repeat(i))

if __name__ == "__main__":
    arguments = ArgumentParser()
    arguments.add_argument('--train-ratio', type=float, default=0.8)
    arguments.add_argument('--seed', type=int)
    args = arguments.parse_args()

    df = (pd
          .read_csv(sys.stdin)
          .dropna())
    mc = MultiCountGrouper.group(df)
    handlers = (
        UniqueSplitHandler(mc.df_singles, args.train_ratio),
        StandardSplitHandler(mc.df_multis, args.train_ratio, args.seed),
    )
    view = pd.concat(it.chain.from_iterable(handlers))
    # assert not df.isnull().any().any()
    view.to_csv(sys.stdout, index=False)
