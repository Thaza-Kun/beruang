use clap::ValueEnum;
use polars::{lazy::dsl::Expr, prelude::*};

use std::error::Error;

#[derive(Clone, Copy, ValueEnum)]
pub enum TimeGroup {
    Quarterly,
    Monthly,
    Biweekly,
    Weekly,
}

impl TimeGroup {
    pub fn into_dynamic_group_opts(self) -> DynamicGroupOptions {
        match self {
            TimeGroup::Quarterly => DynamicGroupOptions {
                every: Duration::parse("4mo"),
                period: Duration::parse("4mo"),
                offset: Duration::parse("0"),
                ..Default::default()
            },
            TimeGroup::Monthly => DynamicGroupOptions {
                every: Duration::parse("1mo"),
                period: Duration::parse("1mo"),
                offset: Duration::parse("0"),
                ..Default::default()
            },
            TimeGroup::Biweekly => DynamicGroupOptions {
                every: Duration::parse("2w"),
                period: Duration::parse("2w"),
                offset: Duration::parse("0"),
                ..Default::default()
            },
            TimeGroup::Weekly => DynamicGroupOptions {
                every: Duration::parse("1w"),
                period: Duration::parse("1w"),
                offset: Duration::parse("0"),
                ..Default::default()
            },
        }
    }
}

pub struct Ledger<'a> {
    pub columns: Header<'a>,
    frame: LazyFrame,
}

impl<'a> Ledger<'a> {
    pub fn load_parquet(file: &str) -> Result<Self, Box<dyn Error>> {
        let args = ScanArgsParquet::default();
        let lf = LazyFrame::scan_parquet(file, args)?;
        Ok(Self {
            columns: Header::new(),
            frame: lf,
        })
    }

    pub fn summarize(self) -> LazyFrame {
        self.frame
            .clone()
            .group_by([col(self.columns.account), col(self.columns.currency)])
            .agg([
                col(self.columns.cost).sum(),
                col(self.columns.cost)
                    .cast(DataType::Float32)
                    .mean()
                    .alias("Mean")
                    .cast(DataType::Decimal(Some(10), Some(2))),
                col(self.columns.cost)
                    .cast(DataType::Float32)
                    .max()
                    .alias("Max")
                    .cast(DataType::Decimal(Some(10), Some(2))),
                col(self.columns.cost)
                    .cast(DataType::Float32)
                    .min()
                    .alias("Min")
                    .cast(DataType::Decimal(Some(10), Some(2))),
            ])
    }

    fn temporal_group(&self, frame: LazyFrame, group_by: &[Expr], group: TimeGroup) -> LazyGroupBy {
        frame
            .sort(&self.columns.date, Default::default())
            .sort(&self.columns.account, Default::default())
            .sort(&self.columns.currency, Default::default())
            .group_by_dynamic(
                col(&self.columns.date),
                group_by,
                group.into_dynamic_group_opts(),
            )
    }

    pub fn nett(self, group: TimeGroup, ignore_category: &str) -> LazyFrame {
        self.temporal_group(
            self.frame
                .clone()
                .filter((col(self.columns.category) != lit(ignore_category)).into()),
            &[col(self.columns.account), col(self.columns.currency)],
            group,
        )
        .agg([col(self.columns.cost).sum()])
    }
}

pub struct Header<'a> {
    date: &'a str,
    #[allow(dead_code)]
    details: &'a str,
    category: &'a str,
    account: &'a str,
    currency: &'a str,
    cost: &'a str,
}

impl<'a> Header<'a> {
    pub fn new() -> Self {
        Self {
            date: "Tarikh",
            details: "Keterangan",
            category: "Kategori",
            account: "Akaun",
            currency: "Wang",
            cost: "Jumlah",
        }
    }
}
