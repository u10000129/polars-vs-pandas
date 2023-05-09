import random
import string
from pathlib import Path

import numpy as np
import polars as pl

COL = 50

random.seed(0)
np.random.seed(0)


def gen_fake_data(row: int):
    num_data = np.random.random((row, COL))
    cat_data = np.array(
        [[random.choice(string.ascii_letters) for _ in range(COL)] for _ in range(row)]
    )
    ids = [[str(i)] for i in range(row)]
    data = np.concatenate([ids, num_data, cat_data], axis=1)
    cols = ["ID"] + [f"num_{i}" for i in range(COL)] + [f"cat_{i}" for i in range(COL)]
    df = pl.DataFrame(data, cols)
    Path("data").mkdir(exist_ok=True)
    df.write_csv(f"data/{data.shape[0]}.csv")


if __name__ == "__main__":
    for row in [100_000, 200_000, 500_000, 800_000, 1_000_000]:
        gen_fake_data(row)
