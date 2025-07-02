    use async_trait::async_trait;
use anyhow::Result;
    use crate::writers::DataWriterFactory;

#[async_trait]
pub trait DataReader: Send + Sync {
    async fn read(&self, writer_factory: Box<dyn DataWriterFactory>) -> Result<()>;
}

mod mysql_reader;
pub use mysql_reader::MysqlReader;