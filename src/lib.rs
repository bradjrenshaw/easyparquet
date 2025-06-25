use std::error::Error;
mod config;
mod backup;
pub use config::Config;
pub use backup::TableBackup;

pub async fn run(config: Config) -> Result<(), Box<dyn Error>> {
    let url = config.get_uri();
    let pool = mysql_async::Pool::new(url.as_str());
    if config.database_tables.len() > 0 {
        let table_name = config.database_tables.get(0).unwrap().clone();
        let output_file_path = format!("{table_name}.parquet");
        let op = TableBackup {
            table_name,
            output_file_path,
        };
        println!("{:?}", op);
        let new_pool = pool.clone();
        op.execute(new_pool).await?;
    }
    Ok(())
}
