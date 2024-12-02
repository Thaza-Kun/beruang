# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "polars",
#     "typer",
# ]
# ///

import pathlib
from typing import List
import typer
import polars as pl

@dataclass
class Data:
    category: str
    title: str
    note: str = ""

def main(
    input: pathlib.Path,
    output: pathlib.Path,
    correction: pathlib.Path,
    preview: bool = False,
    typeignore: List[str] = [],
    descignore: List[str] = []
) -> None:
    corr = pl.scan_csv(correction)
    lf = (
        pl.scan_csv(
            input,
            new_columns=["Date", "Type", "Description", "Amount", "Balance", "Flow"],
            schema_overrides=[pl.Date],
        )
        .filter(~pl.col("Type").str.contains_any(typeignore))
        .filter(~pl.col("Description").str.contains_any(descignore))
        .with_columns(
            pl.when(pl.col("Description").eq(""))
            .then(pl.col("Type").alias("Description"))
            .otherwise(pl.col("Description"))
        )
        .select(
            pl.col("Date"),
            pl.when(pl.col("Flow") == "Withdrawal")
            .then(-pl.col("Amount"))
            .otherwise(pl.col("Amount"))
            .alias("Amount"),
            subcol=pl.struct(
                description=pl.col("Description"),
                Title=pl.lit(""),
                Category=pl.lit(""),
                Note=pl.lit(""),
            ),
        )
    )
    for kv in corr.collect().iter_rows(named=True):
        key = kv["name"]
        val = Data(title=kv["title"], category=kv["category"], note=kv["note"])
        lf = lf.with_columns(
            subcol=pl.when(pl.col("subcol").struct["description"].str.contains(key))
            .then(
                pl.struct(
                    Title=pl.lit(val.title),
                    Category=pl.lit(val.category),
                    Note=pl.lit(val.note),
                    description=pl.col("subcol").struct["description"],
                ).alias("subcol")
            )
            .otherwise(pl.col("subcol"))
        )
    lf = (
        lf.unnest("subcol")
        .with_columns(Account=pl.lit("MAYB"))
    )
    if preview:
        print(corr.collect())
        print(lf.filter(pl.col("Category").eq("")).collect())
    lf.select(pl.all().exclude("description")).collect().write_csv(output)


if __name__ == "__main__":
    typer.run(main)
