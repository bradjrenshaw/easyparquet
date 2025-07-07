use std::path::PathBuf;

use anyhow::Result;
use crate::Config;
use easyparquet::backups::BatchBackup;
use mysql_async::Pool;

pub async fn backup(config: &Config) -> Result<()> {
    let pool = Pool::new(config.get_uri().as_str());
    let root_directory = PathBuf::from(&config.backup_directory);
    let mut backup = BatchBackup::new(root_directory);
    backup.add_table("users".to_string());
        backup.add_table("accounts".to_string());
    backup.execute(pool).await?;
    Ok(())
}
