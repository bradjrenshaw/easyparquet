use std::io::ErrorKind;
use std::{fs::File, sync::Arc};
use std::fs;
use arrow::{array::RecordBatch, datatypes::Schema};
use parquet::arrow::ArrowWriter;
use crate::writers::{DataWriter, DataWriterFactory};
use anyhow::{bail, Result};

pub struct ParquetWriter {
    file_path: String,
    temp_path: String,
    writer: Option<ArrowWriter<File>>,
    schema: Option<Arc<Schema>>,
}

impl ParquetWriter {
    pub fn new(file_path: String) -> ParquetWriter {
        ParquetWriter {
                        temp_path: format!("{}.temp", file_path),
            file_path: file_path,
            writer: None,
            schema: None,
        }
    }
}

impl DataWriter for ParquetWriter {
    fn setup(&mut self, schema: Arc<Schema>) -> Result<()> {
        let file = File::create(&self.temp_path).unwrap();

        self.writer = Some(ArrowWriter::try_new(file, schema.clone(), None).unwrap());
        self.schema = Some(schema);
        Ok(())
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if let Some(ref mut writer) = self.writer {
            writer.write(batch)?;
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
        fs::rename(&self.temp_path, &self.file_path)?;
        Ok(())
    }

    fn abort(&mut self) -> Result<()> {
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
