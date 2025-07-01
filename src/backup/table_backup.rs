use crate::backup::queries::DataReader;
use crate::backup::queries::DataWriterFactory;

//This is a prototype and will be significantly optimized
// Todo: Needs to read data in chunks and better abstract out various functions for testing and better structure
// Todo: Better handling of error states (graceful recovery) and fewer unchecked .unwrap() calls
use anyhow::Result;

///Reads a table from the specified database and writes it to a parquet file.
pub struct TableBackup;

impl TableBackup {
    pub fn new() -> TableBackup {
        TableBackup
    }

    pub async fn execute(
        &self,
        reader: Box<dyn DataReader>,
        writer: Box<dyn DataWriterFactory>,
    ) -> Result<()> {
        reader.read(writer).await?;
        Ok(())
    }
}
