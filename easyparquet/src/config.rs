use dotenvy;

#[derive(Debug)]
pub struct Config {
    pub database_uri: String,
    pub backup_directory: String,
    pub database_tables: Vec<String>,
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
        let database_tables = get_env("database_tables")?
            .split(";")
            .filter_map(|s| {
                let s_str = s.trim().to_string();
                if !s_str.is_empty() { Some(s_str) } else { None }
            })
            .collect();
        Ok(Config {
            database_uri,
            backup_directory,
            database_tables,
        })
    }

    pub fn get_uri(&self) -> &String {
        &self.database_uri
    }
}
