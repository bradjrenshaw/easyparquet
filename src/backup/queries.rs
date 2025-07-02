use crate::backup::columns::ColumnData;
use anyhow::{Result, bail};
use arrow::array::Array;
use arrow::{array::RecordBatch, datatypes::Schema};
use async_trait::async_trait;
use futures::StreamExt;
use mysql_async::prelude::*;
use mysql_async::{Pool, Row};
use parquet::arrow::ArrowWriter;
use tokio::sync::mpsc;
use std::fs::File;
use std::sync::Arc;

use super::Column;

#[async_trait]
pub trait DataReader: Send + Sync {
    async fn read(&self, writer_factory: Box<dyn DataWriterFactory>) -> Result<()>;
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
    chunk_size: usize,
}

impl MysqlReader {
    pub fn new(pool: Pool, table_name: String, chunk_size: usize) -> MysqlReader {
        MysqlReader {
            pool: pool,
            table_name: table_name,
            chunk_size: chunk_size,
        }
    }
}

enum WriteMessage {
    Chunk(RecordBatch),
    Finish,
    Error,
}

impl MysqlReader {

    fn get_columns(columns: &Vec<Arc<ColumnData>>) -> Vec<Column> {
        columns.iter().map(|data| Column::from_data(data.clone()).unwrap()).collect()
    }
}

#[async_trait]
impl DataReader for MysqlReader {
    async fn read(&self, writer_factory: Box<dyn DataWriterFactory>) -> Result<()> {
        let mut conn = self.pool.get_conn().await?;
        let query = format!("SELECT * FROM {}", self.table_name);
        let mut stream = conn.exec_stream(query, mysql_async::Params::Empty).await?;
        let mut column_data = Vec::new();

        let mut schema_vec = Vec::new();
        for column in stream.columns().iter() {
            let name = column.name_str().into_owned();
                        //Note that the not_null flag means null is not allowed (mysql NOT NULL) so it needs to be inverted for Arrow nullable
            let nullable = !column
                .flags()
                .contains(mysql_async::consts::ColumnFlags::NOT_NULL_FLAG);
            let column_type = column.column_type();
            let data = Arc::new(ColumnData::new(name, nullable, column_type)?);
            schema_vec.push(data.get_schema_field());
            column_data.push(data);
        }

        let schema = Arc::new(Schema::new(schema_vec));

        let (tx, mut rx) = mpsc::channel(100);

        let writer_schema = schema.clone();
        let join_handle = tokio::task::spawn_blocking(move || {
            let mut writer = writer_factory.create();
            writer.setup(writer_schema)?;
            while let Some(message) = rx.blocking_recv() {
                match message {
            WriteMessage::Chunk(batch) => writer.write(&batch)?,
            WriteMessage::Finish => break,
            WriteMessage::Error => bail!("Error in another task")
                }
            }
            writer.finish()
        });

        loop{
                    let mut columns = MysqlReader::get_columns(&column_data);

                    let mut rows: usize = 0;
        while let Some(row_result) = stream.next().await {
            let row: Row = row_result?;
            for (i, v) in row.unwrap().into_iter().enumerate() {
                Column::push(&mut columns[i], v)?;
            }

            rows = rows + 1;
            //If chunk size is > 0, and we have reached allocated chunk size (in rows) then break and the outer loop will send the chunk and then reiterate to find more chunks
            if self.chunk_size > 0 && rows >= self.chunk_size {
                break;
            }
        }

        if rows > 0 {
            //We have gathered either all data or a chunk
        let batch_vec: Vec<Arc<dyn Array>> = columns.into_iter().map(|col| col.finish()).collect();
        let batch = arrow::array::RecordBatch::try_new(schema.clone(), batch_vec)?;
        tx.send(WriteMessage::Chunk(batch)).await?;
        } else {
            //end of data stream reached
            break;
        }
    }

        tx.send(WriteMessage::Finish).await?;
        join_handle.await??;
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
            schema: None,
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
        Self { file_path }
    }
}

impl DataWriterFactory for ParquetWriterFactory {
    fn create(&self) -> Box<dyn DataWriter> {
        Box::new(ParquetWriter::new(self.file_path.clone()))
    }
}
