use calamine::{open_workbook, Reader, Xlsx};
use itertools::Itertools;
use polars::datatypes::{AnyValue, ArrowDataType, DataType, TimeUnit};
use polars::frame::row::Row;
use polars::frame::DataFrame;
use polars::io::csv::CsvWriter;
use polars::io::SerWriter;
use polars::lazy::dsl::col;
use polars::lazy::frame::LazyFrame;
use polars::prelude::{ArrowField, ArrowSchema, Schema};
use std::error::Error;

fn to_polars_value<'a>(data: &'a calamine::DataType) -> AnyValue<'a> {
    match data {
        calamine::DataType::Int(i) => AnyValue::Int64(*i),
        // Since all floats in this timeseries is monetary, it is just a decimal with precision 2.
        // Example: 1.50 * 100. :-> 150 --> Decimal(1[.]50)
        calamine::DataType::Float(f) => AnyValue::Decimal((f * 100.) as i128, 2),
        calamine::DataType::String(s) => AnyValue::String(s),
        calamine::DataType::Bool(tf) => AnyValue::Boolean(*tf),
        calamine::DataType::DateTime(t) => {
            // Excel counts days from the year 1900
            // Polars counts days from the year 1970 (UNIX EPOCH)*
            // Therefore: POLARS_DATE = EXCEL_DATE - 70 years.
            // P.S. It seems that the minus 1 offset is also necessary.
            //
            // ---
            // *Thank god Polars' AnyValue::Date counts date in days.
            //  Otherwise, we would need to convert it to seconds
            //  because UNIX time counts _SECONDS_ from the epoch.
            AnyValue::Date((t - 1. - (70. * (365.25))).floor() as i32)
        }
        calamine::DataType::Duration(d) => {
            AnyValue::Duration(d.round() as i64, TimeUnit::Milliseconds)
        }
        _ => AnyValue::Null,
    }
}
fn type_of(name: String) -> ArrowDataType {
    if name == "Tarikh".to_string() {
        ArrowDataType::Date32
    } else if name == "Jumlah".to_string() {
        ArrowDataType::Decimal(10, 2)
    } else {
        ArrowDataType::Utf8
    }
}

pub fn timeseries_from_excel(file: &str, sheets: &[&str]) -> Result<DataFrame, Box<dyn Error>> {
    let mut workbook: Xlsx<_> = open_workbook(&file)?;
    if sheets.is_empty() {
        return Ok(DataFrame::empty());
    }
    let mut sheets = sheets.iter();
    let r = workbook.worksheet_range(sheets.next().ok_or("No Sheets")?)?;
    let mut rows = r.rows();
    let header = rows.next().ok_or("No first line")?;

    let schema = Schema::from(ArrowSchema::from(
        header
            .iter()
            .map(|a| ArrowField {
                name: a.to_string().into(),
                data_type: type_of(a.to_string()),
                is_nullable: false,
                metadata: Default::default(),
            })
            .collect_vec(),
    ));

    let ro = rows
        .map(|a| Row::new(a.iter().map(|b| to_polars_value(b)).collect_vec()))
        .collect_vec();
    let mut df = DataFrame::from_rows_and_schema(&ro, &schema)?;
    for sh in sheets {
        let r = workbook.worksheet_range(sh)?;
        // Skip headers
        let ro = r
            .rows()
            .skip(1)
            .map(|a| Row::new(a.iter().map(|b| to_polars_value(b)).collect_vec()))
            .collect_vec();
        df = df.vstack(&DataFrame::from_rows_and_schema(&ro, &schema)?)?;
    }
    Ok(df.drop_nulls::<String>(None)?)
}

pub fn timeseries_to_csv(frame: LazyFrame, output: &str) -> Result<(), Box<dyn Error>> {
    let mut file = std::fs::File::create(output).unwrap();
    CsvWriter::new(&mut file)
        .finish(
            &mut frame
                .with_column(col("Jumlah").cast(DataType::Float32))
                .collect()?,
        )
        .unwrap();
    Ok(())
}
