import argparse
import os
from collections import defaultdict
from pathlib import Path
from timeit import default_timer as timer

import polars as pl

import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--module", "-m", type=str, required=True)
parser.add_argument("--test_operation", "-t", type=str, required=True)
args = parser.parse_args()


def timeit(func):
    def inner(*args, **kwargs):
        start = timer()
        func(*args, **kwargs)
        end = timer()
        return end - start

    return inner


def read_csv(row: int) -> pl.DataFrame | pd.DataFrame:
    if args.module == "polars":
        return pl.read_csv(f"data/{row}.csv")
    elif args.module == "pandas2":
        return pd.read_csv(f"data/{row}.csv", dtype_backend="pyarrow", engine="pyarrow")
    else:
        return pd.read_csv(f"data/{row}.csv")


@timeit
def join(df_A: pl.DataFrame | pd.DataFrame, df_B: pl.DataFrame | pd.DataFrame) -> float:
    if args.module == "polars":
        df_A.join(df_B, on="ID", how="left", suffix="A")
    else:
        df_A.join(df_B.set_index("ID"), on="ID", how="left", rsuffix="A")


@timeit
def quintuple_join(
    df_A: pl.DataFrame | pd.DataFrame, df_B: pl.DataFrame | pd.DataFrame
) -> float:
    if args.module == "polars":
        ldf_A, ldf_B = df_A.lazy(), df_B.lazy()
        for i in range(5):
            ldf_A = ldf_A.join(ldf_B, on="ID", how="left", suffix=f"_{i}")
        ldf_A.collect()
    else:
        for i in range(5):
            df_A = df_A.join(df_B.set_index("ID"), on="ID", how="left", rsuffix=f"_{i}")


@timeit
def filter_by_num(df: pl.DataFrame | pd.DataFrame, num_0_median: float) -> float:
    if args.module == "polars":
        df = df.filter(pl.col("num_0") > num_0_median)
    else:
        df = df[df["num_0"] > num_0_median]


@timeit
def filter_by_cat(df: pl.DataFrame | pd.DataFrame) -> float:
    if args.module == "polars":
        df = df.filter(pl.col("cat_0") == "a")
    else:
        df = df[df["cat_0"] == "a"]


@timeit
def agg_by_median(df_num: pl.DataFrame | pd.DataFrame) -> float:
    df_num = df_num.median()


@timeit
def agg_by_mean(df_num: pl.DataFrame | pd.DataFrame) -> float:
    df_num = df_num.mean()


@timeit
def agg_by_nunique(df_cat: pl.DataFrame | pd.DataFrame) -> float:
    if args.module == "polars":
        df_cat = df_cat.select(pl.col("*").n_unique())
    else:
        df_cat = df_cat.nunique()


@timeit
def groupby(df_group: pl.DataFrame | pd.DataFrame) -> float:
    if args.module == "polars":
        df_group = df_group.lazy().groupby("cat_0").mean().collect()
    else:
        df_group = df_group.groupby(["cat_0"]).mean()


if __name__ == "__main__":
    data = defaultdict(list)
    for row in [100_000, 200_000, 500_000, 800_000, 1_000_000]:
        df = df_A = df_B = read_csv(row)
        if args.module == "polars":
            df_num = df.select([col for col in df.columns if col.startswith("num")])
            df_cat = df.select([col for col in df.columns if col.startswith("cat")])
            df_group = df.select(
                ["cat_0"] + [col for col in df.columns if col.startswith("num")]
            )
        else:
            df_num = df[df.columns[df.columns.str.startswith("num")]]
            df_cat = df[df.columns[df.columns.str.startswith("cat")]]
            df_group = df[
                df.columns[
                    df.columns.str.match("cat_0") | df.columns.str.startswith("num")
                ]
            ]
        match args.test_operation:
            case "read_csv":
                start = timer()
                read_csv(row)
                end = timer()
                time = end - start
            case "join":
                time = join(df_A, df_B)
            case "quintuple_join":
                time = quintuple_join(df_A, df_B)
            case "filter_by_num":
                time = filter_by_num(df, df["num_0"].median())
            case "filter_by_cat":
                time = filter_by_cat(df)
            case "agg_by_median":
                time = agg_by_median(df_num)
            case "agg_by_mean":
                time = agg_by_mean(df_num)
            case "agg_by_nunique":
                time = agg_by_nunique(df_cat)
            case "groupby":
                time = groupby(df_group)
            case _:
                raise ValueError()
        data["module"].append(args.module)
        data["elapsed_time"].append(time)
        data["row"].append(row)

    Path("csv").mkdir(exist_ok=True)
    stats_path = f"csv/{args.test_operation}.csv"
    if os.path.isfile(stats_path):
        df = pl.read_csv(stats_path)
        df.extend(pl.DataFrame(data))
    else:
        df = pl.DataFrame(data)

    df.write_csv(stats_path)
