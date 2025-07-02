use anyhow::Result;
use std::error::Error;
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
    let mut backup = BatchBackup::new();
    for table in config.database_tables.into_iter() {
        backup.add_table(table);
    }
    backup.execute(pool).await?;
    Ok(())
}
