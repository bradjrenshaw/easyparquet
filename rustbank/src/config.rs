use dotenvy;

#[derive(Debug)]
pub struct Config {
    pub database_uri: String,
    pub backup_directory: String,
}

fn get_env(key: &str) -> Result<String, String> {
    match dotenvy::var(key) {
        Ok(s) => Ok(s),
        Err(_) => Err(format!("Environment variable {key} is required.")),
    }
}

impl Config {
    pub fn build() -> Result<Config, String> {
        let database_uri = get_env("database_uri")?;
        let backup_directory = get_env("backup_directory")?;
        Ok(Config {
            database_uri,
            backup_directory
        })
    }

    pub fn get_uri(&self) -> String {
        self.database_uri.clone()
    }
}
