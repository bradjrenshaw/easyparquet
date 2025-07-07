use crate::data::columns::{Column, ColumnData};
use crate::readers::DataReader;
use crate::writers::DataWriterFactory;
use anyhow::{Result, bail};
use arrow::array::Array;
use arrow::{array::RecordBatch, datatypes::Schema};
use async_trait::async_trait;
use futures::StreamExt;
use mysql_async::prelude::*;
use mysql_async::{Pool, Row};
use std::sync::Arc;
use tokio::sync::mpsc;

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
        columns
            .iter()
            .map(|data| Column::from_data(data.clone()).unwrap())
            .collect()
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
            let flags = column.flags();
            //Note that the not_null flag means null is not allowed (mysql NOT NULL) so it needs to be inverted for Arrow nullable
            let nullable = !flags.contains(mysql_async::consts::ColumnFlags::NOT_NULL_FLAG);
            let unsigned = flags.contains(mysql_async::consts::ColumnFlags::UNSIGNED_FLAG);
            let column_type = column.column_type();
            let data = Arc::new(ColumnData::new(name, unsigned, nullable, column_type)?);
            schema_vec.push(data.get_schema_field());
            column_data.push(data);
        }

        let schema = Arc::new(Schema::new(schema_vec));

        let (tx, mut rx) = mpsc::channel(self.chunk_size * 2);

        let writer_schema = schema.clone();
        let write_task = tokio::task::spawn_blocking(move || {
            let mut writer = writer_factory.create();
            writer.setup(writer_schema)?;
            while let Some(message) = rx.blocking_recv() {
                match message {
                    WriteMessage::Chunk(batch) => writer.write(&batch)?,
                    WriteMessage::Finish => {
                        writer.finish()?;
                        return Ok(());
                    }
                    WriteMessage::Error => {
                        writer.abort()?;
                        return Ok(());
                    }
                }
            }
            //If this point is reached, sender channel closed too early, no Finish message was received, thus the end of the data stream was not reached and the database table cannot be properly backed up
            bail!("End of data stream too early; improper backup");
        });

        loop {
            let mut columns = MysqlReader::get_columns(&column_data);

            let mut rows: usize = 0;
            while let Some(row_result) = stream.next().await {
                let row: Row = match row_result {
                    Ok(row) => row,
                    Err(e) => {
                        tx.send(WriteMessage::Error).await?;
                        bail!(e);
                    }
                };

                for (i, v) in row.unwrap().into_iter().enumerate() {
                    if let Err(e) = Column::push(&mut columns[i], v) {
                        tx.send(WriteMessage::Error).await?;
                        bail!(e);
                    }
                }

                rows = rows + 1;
                //If chunk size is > 0, and we have reached allocated chunk size (in rows) then break and the outer loop will send the chunk and then reiterate to find more chunks
                if self.chunk_size > 0 && rows >= self.chunk_size {
                    break;
                }
            }

            if rows > 0 {
                //We have gathered either all data or a chunk
                let batch_vec: Vec<Arc<dyn Array>> =
                    columns.into_iter().map(|col| col.finish()).collect();
                let batch = arrow::array::RecordBatch::try_new(schema.clone(), batch_vec)?;
                tx.send(WriteMessage::Chunk(batch)).await?;
            } else {
                //end of data stream reached
                break;
            }
        }

        tx.send(WriteMessage::Finish).await?;

        match write_task.await {
            Ok(Ok(())) => {},
            Ok(Err(e)) => bail!(e),
            Err(e) => bail!("Writer task failed: {e}."),
        };
        Ok(())
    }
}
