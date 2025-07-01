use anyhow::{bail, Result};
use arrow::array::Array;
use arrow::{array::RecordBatch, datatypes::Schema};
use async_trait::async_trait;
use mysql_async::{Conn, Pool, ResultSetStream, Row};
use mysql_async::prelude::*;
use parquet::arrow::ArrowWriter;
use std::pin::Pin;
use std::sync::Arc;
use std::fs::File;
use futures::{Stream, StreamExt};
use crate::backup::columns::ColumnData;

use super::Column;

#[async_trait]
pub trait DataReader: Send + Sync {
    async fn read(&self, writerFactory: Box<dyn DataWriterFactory>) -> Result<()>;
}

pub trait DataWriter {
    fn setup(&mut self, schema: Arc<Schema>) -> Result<()>;
    fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

pub trait DataWriterFactory: Send + Sync {
    fn create(&self) -> Box<dyn DataWriter>;
}

pub struct MysqlReader {
    pool: mysql_async::Pool,
    table_name: String,
}

impl MysqlReader {

    pub fn new(pool: Pool, table_name: String) -> MysqlReader {
        MysqlReader {
            pool: pool,
            table_name: table_name,
        }
    }
}

#[async_trait]
impl DataReader for MysqlReader {

    async fn read(&self, mut writerFactory: Box<dyn DataWriterFactory>) -> Result<()> {
        let mut conn = self.pool.get_conn().await?;
        let query = format!("SELECT * FROM {}", self.table_name);
                let mut stream = conn.exec_stream(query, mysql_async::Params::Empty).await?;
        let mut columns = Vec::new();

        let mut schema_vec = Vec::new();
        for column in stream.columns().iter() {
            let name = column.name_str().into_owned();
            let nullable = !column
                .flags()
                .contains(mysql_async::consts::ColumnFlags::NOT_NULL_FLAG);
            let column_type = column.column_type();
            let data = Arc::new(ColumnData::new(name, nullable, column_type)?);
            let col = Column::from_data(data.clone())?;
            schema_vec.push(data.get_schema_field());
            columns.push(col);
        }

        let schema = Arc::new(Schema::new(schema_vec));

        while let Some(row_result) = stream.next().await {
            let row: Row = row_result?;
            for (i, v) in row.unwrap().into_iter().enumerate() {
                Column::push(&mut columns[i], v)?;
            }
        }

        let batch_vec: Vec<Arc<dyn Array>> = columns.into_iter().map(|col| col.finish()).collect();

        let batch = arrow::array::RecordBatch::try_new(schema.clone(), batch_vec)?;

    tokio::task::spawn_blocking(move || {
                let mut writer = writerFactory.create();
        writer.setup(schema.clone())?;
        writer.write(&batch)?;
        writer.finish()
    }).await??;
        Ok(())
    }
}

pub struct ParquetWriter {
    file_path: String,
    writer: Option<ArrowWriter<File>>,
    schema: Option<Arc<Schema>>,
}

impl ParquetWriter {
    pub fn new(file_path: String) -> ParquetWriter {
        ParquetWriter {
            file_path: file_path,
            writer: None,
            schema: None
        }
    }
}

impl DataWriter for ParquetWriter {

    fn setup(&mut self, schema: Arc<Schema>) -> Result<()> {
        let file = File::create(&self.file_path).unwrap();
        self.writer = Some(ArrowWriter::try_new(file, schema.clone(), None).unwrap());
                self.schema = Some(schema);
        Ok(())
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if let Some(ref mut writer) = self.writer {
            writer.write(batch).unwrap();
        } else {
            bail!("No file handle.")
        }
        Ok(())
    }

     fn finish(&mut self) -> Result<()> {
        if let Some(ref mut writer) = self.writer {
            writer.finish()?;
        } else {
            bail!("Invalid Parquet writer.");
        }
        Ok(())
     }
}

pub struct ParquetWriterFactory {
    file_path: String,
}

impl ParquetWriterFactory {

    pub fn new(file_path: String) -> Self {
        Self {file_path}
    }
}

impl DataWriterFactory for ParquetWriterFactory {

    fn create(&self) -> Box<dyn DataWriter> {
        Box::new(ParquetWriter::new(self.file_path.clone()))
    }

}