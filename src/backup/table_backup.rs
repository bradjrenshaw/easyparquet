use crate::backup::columns::ColumnData;
use crate::backup::queries::DataReader;
use crate::backup::queries::DataWriter;
use crate::backup::queries::DataWriterFactory;

//This is a prototype and will be significantly optimized
// Todo: Needs to read data in chunks and better abstract out various functions for testing and better structure
// Todo: Better handling of error states (graceful recovery) and fewer unchecked .unwrap() calls
use super::Column;
use anyhow::Result;
use arrow::array::Array;
use arrow::datatypes::Schema;
use futures::StreamExt;
use mysql_async::Row;
use mysql_async::prelude::*;
use parquet::arrow::ArrowWriter;
use std::sync::Arc;

///Reads a table from the specified database and writes it to a parquet file.
pub struct TableBackup {
    pub table_name: String,
    pub output_file_path: String,
}

impl TableBackup {
    pub fn new(table_name: String, output_file_path: String) -> TableBackup {
        TableBackup {
            table_name,
            output_file_path,
        }
    }

    pub async fn execute(&self, reader: Box<dyn DataReader>, writer: Box<dyn DataWriterFactory>) -> Result<()> {
        reader.read(writer).await?;
        Ok(())
    }
}
