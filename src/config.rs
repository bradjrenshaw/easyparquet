use dotenvy;

#[derive(Debug)]
pub struct Config {
    database_uri: String,
    database_user: String,
    database_password: String,
    database_tables: Vec<String>,
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
        let database_user = get_env("database_user")?;
        let database_password = get_env("database_password")?;
        let database_tables_string = get_env("database_tables")?;
        let database_tables = database_tables_string
            .split(";")
            .filter_map(|s| {
                let s_str = s.trim().to_string();
                if !s_str.is_empty() { Some(s_str) } else { None }
            })
            .collect();
        Ok(Config {
            database_uri,
            database_user,
            database_password,
            database_tables,
        })
    }
}
