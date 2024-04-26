import sys
import logging

import pandas as pd
import linktransformer as lt

if __name__ == "__main__":
    # arguments = ArgumentParser()
    # arguments.add_argument('--workers', type=int)
    # args = arguments.parse_args()

    df = pd.read_csv(sys.stdin)
    (source, target) = (df[x].to_frame() for x in ('response', 'target'))
    target = target.drop_duplicates()

    matched = lt.merge(source,
                       target,
                       merge_type='1:m',
                       on=None,
                       suffixes=("_x", "_y"),
                       batch_size=128)
    logging.warning(type(matched))
    print(matched)
