//! A finance manager for bears and people with money.
mod ledger;
mod read_excel;

use crate::ledger::Ledger;
use itertools::Itertools;
use ledger::TimeGroup;
use polars::prelude::*;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Combines multiple excel sheets into a single dataframe. Optionally converts to Parquet or CSV.
    Combine(ReadExcel),
    /// Do some accounting
    #[command(subcommand)]
    Ledger(LedgerAction),
}

#[derive(Args)]
struct LedgerOpts {
    /// Filename (Parquet format)
    file: String,
    /// Choose time blocks for aggregation
    #[arg(value_enum, short, long)]
    temporal: TimeGroup,
}

#[derive(Subcommand)]
enum LedgerAction {
    /// Summarizes accounts for the whole duration.
    Summary(LedgerOpts),
    /// Sums the total for the selected time blocks.
    Nett(LedgerOpts),
}

#[derive(Clone, ValueEnum)]
enum DataIOFormat {
    Parquet,
    Csv,
}

#[derive(Args)]
struct ReadExcel {
    /// Path to file.
    #[arg(short)]
    file: String,
    /// Sheets to read. Defaults to all months.
    #[arg(short)]
    sheets: Vec<String>,
    /// Parquet file to convert. Leave empty to display dataframe.
    #[arg(short)]
    output: Option<String>,
    /// Format for export.
    #[arg(short, long)]
    export: DataIOFormat,
}

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mac", "Apr", "Mei", "Jun", "Jul", "Ogo", "Sep", "Okt", "Nov", "Dis",
];

fn main() {
    let app = Cli::parse();

    match &app.command {
        Commands::Combine(ReadExcel {
            file,
            sheets,
            output,
            export,
        }) => {
            let sheets = if sheets.is_empty() {
                MONTHS.into_iter().map(|s| String::from(s)).collect_vec()
            } else {
                sheets.to_vec()
            };
            let mut df = read_excel::timeseries_from_excel(
                &file,
                sheets.iter().map(|a| a.as_str()).collect_vec().as_slice(),
            )
            .expect(&format!("Unable to read file {}", &file));
            match output {
                Some(file) => match export {
                    DataIOFormat::Parquet => {
                        let mut file = std::fs::File::create(file).unwrap();
                        ParquetWriter::new(&mut file).finish(&mut df).unwrap();
                    }
                    DataIOFormat::Csv => read_excel::timeseries_to_csv(df.lazy(), file).unwrap(),
                },
                None => println!("{}", df),
            }
        }
        Commands::Ledger(action) => match action {
            LedgerAction::Summary(opts) => {
                println!(
                    "{}",
                    Ledger::load_parquet(&opts.file)
                        .expect(&format!("Unable to read file {}", &opts.file))
                        .summarize()
                        .collect()
                        .expect("Unable to collect LazyFrame.")
                )
            }
            LedgerAction::Nett(opts) => println!(
                "{}",
                Ledger::load_parquet(&opts.file)
                    .expect(&format!("Unable to read file {}", &opts.file))
                    .nett(opts.temporal, "Pertukaran")
                    .collect()
                    .expect("Unable to collect LazyFrame")
            ),
        },
    };
}
