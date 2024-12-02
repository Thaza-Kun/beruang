# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "polars",
#     "typer",
# ]
# ///

import pathlib
import typer
import polars as pl
from dataclasses import dataclass


@dataclass
class Data:
    category: str
    title: str
    note: str = ""


# TODO: MAP[str, dict()]
description_map = {
    "Dvends Tech": Data(title="Mesin Gedegang", category="Jajan"),
    "AMRANCAFE10": Data(title="Makan", category="Makan"),
    "MEGA LIMITED": Data(title="Storan Awan", category="Khidmat", note="Mega NZ"),
    "VILLAGE GROCER": Data(title="Makan", category="Makan", note="Village Grocer"),
    "WASHUPPTECH": Data(title="Dobi", category="Dobi"),
}


def main(
    input: pathlib.Path,
    output: pathlib.Path,
    correction: pathlib.Path,
    preview: bool = False,
) -> None:
    corr = pl.scan_csv(correction)
    lf = (
        pl.scan_csv(
            input,
            new_columns=["Date", "Type", "Description", "Amount", "Balance", "Flow"],
            schema_overrides=[pl.Date],
        )
        .filter(~pl.col("Type").str.contains_any(["PRE-AUTH", "PREAUTH"]))
        .filter(~pl.col("Description").str.contains("Shopee"))
        .with_columns(
            pl.when(pl.col("Type").eq("CASH DEPOSIT"))
            .then(pl.lit("CASH DEPOSIT").alias("Description"))
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
