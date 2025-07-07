use std::{fs, path::PathBuf};
use std::io::ErrorKind;

use crate::readers::DataReader;
use crate::writers::DataWriterFactory;

use anyhow::{Result, bail};

///Reads a table from the specified database and writes it to a parquet file.
pub struct TableBackup {
    file_path: PathBuf,
    temp_path: PathBuf,
}

impl TableBackup {
    pub fn new(file_path: PathBuf) -> TableBackup {
        let mut temp_path = file_path.clone();
        temp_path.set_extension("temp");
        TableBackup {
            temp_path,
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
