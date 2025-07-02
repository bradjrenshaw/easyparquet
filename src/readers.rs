use crate::writers::DataWriterFactory;
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait DataReader: Send + Sync {
    async fn read(&self, writer_factory: Box<dyn DataWriterFactory>) -> Result<()>;
}

mod mysql_reader;
pub use mysql_reader::MysqlReader;
