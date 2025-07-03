use std::fs;
use std::io::ErrorKind;

use crate::readers::DataReader;
use crate::writers::DataWriterFactory;

//This is a prototype and will be significantly optimized
// Todo: Needs to read data in chunks and better abstract out various functions for testing and better structure
// Todo: Better handling of error states (graceful recovery) and fewer unchecked .unwrap() calls
use anyhow::{Result, bail};

///Reads a table from the specified database and writes it to a parquet file.
pub struct TableBackup {
    file_path: String,
    temp_path: String,
}

impl TableBackup {
    pub fn new(file_path: String) -> TableBackup {
        TableBackup {
            temp_path: format!("{}.temp", file_path),
            file_path,
        }
    }

    pub fn abort(&mut self) -> Result<()> {
        if let Err(e) = fs::remove_file(&self.temp_path) {
            if e.kind() != ErrorKind::NotFound {
                bail!(e);
            }
        }
        if let Err(e) = fs::remove_file(&self.file_path) {
            if e.kind() != ErrorKind::NotFound {
                bail!(e);
            }
        }
        Ok(())
    }

    pub async fn execute(
        &mut self,
        reader: Box<dyn DataReader>,
        writer: Box<dyn DataWriterFactory>,
    ) -> Result<()> {
        if let Err(e) = reader.read(writer).await {
            self.abort()?;
            bail!(e);
        }
        Ok(())
    }
}
