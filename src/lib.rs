use std::error::Error;
mod backup;
mod config;
pub use backup::BatchBackup;
pub use config::Config;

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
