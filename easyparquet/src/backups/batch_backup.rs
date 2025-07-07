use crate::backups::TableBackup;
use crate::readers::MysqlReader;
use crate::writers::ParquetWriterFactory;
use anyhow::{Result, bail};
use std::{collections::HashSet, path::PathBuf};

///Simultaneously backs up multiple databases to their associated. parquet files.
pub struct BatchBackup {
    root_directory: PathBuf,
    tables: HashSet<String>,
}

impl BatchBackup {
    pub fn new(root_directory: PathBuf) -> BatchBackup {
        BatchBackup {
            tables: HashSet::new(),
            root_directory
        }
    }

    pub fn add_table(&mut self, name: String) {
        self.tables.insert(name);
    }

    pub async fn execute(&self, pool: mysql_async::Pool) -> Result<()> {
        let mut task_set = tokio::task::JoinSet::new();

        for name in self.tables.iter() {
            let table_name = name.clone();
            let mut path = self.root_directory.clone();
            let output_file_name = format!("{}.parquet", table_name);
            path.push(output_file_name);
            let pool = pool.clone();
            task_set.spawn(async move {
                let mut backup = TableBackup::new(path.clone());
                let reader = Box::new(MysqlReader::new(pool, table_name.clone(), 1000));
                let writer = Box::new(ParquetWriterFactory::new(path.clone()));
                backup.execute(reader, writer).await
            });
        }

        while let Some(result) = task_set.join_next().await {
            match result {
                Ok(result) => match result {
                    Ok(()) => continue,
                    Err(e) => {
                        task_set.abort_all();
                        bail!("{e}")
                    }
                },
                Err(e) => {
                    task_set.abort_all();
                    bail!("Unrecoverable error: {e}")
                }
            }
        }
        Ok(())
    }
}
