use anyhow::Result;
use crate::Config;
use easyparquet::backups::BatchBackup;
use mysql_async::Pool;

pub async fn backup(config: &Config) -> Result<()> {
    let pool = Pool::new(config.get_uri().as_str());
    let mut backup = BatchBackup::new();
    backup.add_table("users".to_string());
        backup.add_table("accounts".to_string());
    backup.execute(pool).await?;
    Ok(())
}
