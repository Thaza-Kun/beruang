//! A finance manager for bears and people with money.

#![warn(missing_docs)]

use std::{error::Error, fs::OpenOptions, io::Write, path::Path};

use chrono::NaiveDate;
use clap::{Parser, ValueEnum};
use csv;
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug, Serialize, Deserialize)]
struct TransactionParser {
    #[arg(allow_hyphen_values = true)]
    total: i64,
    category: Category,
    participant: String,
    #[arg(short, long, default_value_t = String::from("MAYB"))]
    account: String,
    #[arg(long, default_value_t = String::from("MYR"))]
    currency: String,
    #[arg(short, long)]
    details: String,
    #[arg(long)]
    date: NaiveDate,
    #[serde(skip)]
    #[arg(long, default_value_t = String::from("transactions.csv"))]
    file: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Transaction {
    date: NaiveDate,
    details: String,
    account: String,
    category: Category,
    participant: String,
    currency: String,
    total: String,
}

impl Transaction {
    fn from_parser(parser: &TransactionParser) -> Transaction {
        let mut str_total = parser.total.clone().to_string();
        let length = str_total.chars().count();
        str_total.insert(length - 2, '.');

        Transaction {
            date: parser.date.clone(),
            details: parser.details.clone(),
            account: parser.account.clone(),
            category: parser.category.clone(),
            participant: parser.participant.clone(),
            currency: parser.currency.clone(),
            total: str_total,
        }
    }
}

#[derive(Clone, ValueEnum, Debug, Serialize, Deserialize)]
enum Category {
    Makan,
    Kebersihan,
    Keluarga,
    Kesihatan,
    Khidmat,
    Pelaburan,
    Pengangkutan,
    Rencam,
    Pendapatan,
    Upah,
    Hadiah,
    Perbelanjaan,
    Hutang,
    Hiburan,
    #[serde(rename = "Alat Kerja")]
    AlatKerja,
    Pendidikan,
    Simpanan,
}

fn main() -> Result<(), Box<dyn Error>> {
    let app = TransactionParser::parse();
    let file_existed = dbg!(Path::new(&app.file).exists());
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(&app.file)?;
    let mut writer = csv::WriterBuilder::new()
        .has_headers(!file_existed)
        .from_writer(vec![]);
    writer.serialize(Transaction::from_parser(&app))?;
    let data = String::from_utf8(writer.into_inner()?)?;
    file.write(dbg!(data).as_bytes())?;
    Ok(())
}
