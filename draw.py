import argparse
from pathlib import Path

import polars as pl
import seaborn as sns
from matplotlib import pyplot as plt

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--filename", type=str, required=True)
args = parser.parse_args()


if __name__ == "__main__":
    df = pl.read_csv(f"csv/{args.filename}.csv")
    df = df.with_columns(
        [
            pl.col("elapsed_time").log().alias("elapsed_time (log)"),
            pl.col("row").alias("row_count"),
        ]
    )

    sns.set_palette("Set2")
    fig = plt.figure(figsize=(8, 6))
    sns.lineplot(
        df,
        hue="module",
        hue_order=["polars", "pandas2", "pandas"],
        x="row_count",
        y="elapsed_time (log)",
    )

    Path("fig").mkdir(exist_ok=True)
    fig.savefig(f"fig/{args.filename}.png")
