use anyhow::Result;
use std::{error::Error, path::PathBuf};
mod config;
pub use config::Config;

use crate::backups::BatchBackup;

pub mod backups;
mod data;
pub mod readers;
pub mod writers;

pub async fn run(config: Config) -> Result<(), Box<dyn Error>> {
    let url = config.get_uri();
    let pool = mysql_async::Pool::new(url.as_str());
    let root_directory = PathBuf::from(&config.backup_directory);

    let mut backup = BatchBackup::new(root_directory);
    for table in config.database_tables.into_iter() {
        backup.add_table(table);
    }
    backup.execute(pool).await?;
    Ok(())
}
