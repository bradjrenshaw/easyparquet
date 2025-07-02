use std::sync::Arc;
use anyhow::Result;
use arrow::{array::RecordBatch, datatypes::Schema};

pub trait DataWriter {
    fn setup(&mut self, schema: Arc<Schema>) -> Result<()>;
    fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
    fn abort(&mut self) -> Result<()>;
}

pub trait DataWriterFactory: Send + Sync {
    fn create(&self) -> Box<dyn DataWriter>;
}

mod parquet_writer;
pub use parquet_writer::ParquetWriterFactory;