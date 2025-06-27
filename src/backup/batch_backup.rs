use std::{collections::HashSet};
use super::TableBackup;
use anyhow::{Result, bail};

pub struct BatchBackup {
    tables: HashSet<String>
}

impl BatchBackup {

    pub fn new() -> BatchBackup {
        BatchBackup {tables: HashSet::new()}
    }

    pub fn add_table(&mut self, name: String) {
        self.tables.insert(name);
    }

    pub async fn execute(&self, pool: mysql_async::Pool) -> Result<()> {
        let mut task_set = tokio::task::JoinSet::new();

        for name in self.tables.iter() {
            let table_name = name.clone();
            let output_file_path = format!("{}.parquet", table_name);
            let backup = TableBackup::new(table_name, output_file_path);
            let pool = pool.clone();
            task_set.spawn(async move {
                backup.execute(pool).await
            });
        }

        while let Some(result) = task_set.join_next().await {
            match result {
                Ok(result) => {
                    match result {
                        Ok(()) => continue,
                        Err(e) => {
                            task_set.abort_all();
                            bail!("{e}")
                        }
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
