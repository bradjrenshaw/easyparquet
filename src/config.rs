use dotenvy;

#[derive(Debug)]
pub struct Config {
    database_host: String,
    database_port: u16,
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
        let database_host = get_env("database_host")?;
        let database_port: u16 = match get_env("database_port") {
            Ok(e) => match e.parse() {
                Ok(f) => f,
                Err(e) => return Err("Server port must be a valid port.".to_string()),
            },
            Err(_) => 3306, //Default port
        };
        let database_user = get_env("database_user")?;
        let database_password = get_env("database_password")?;
        let database_tables = get_env("database_tables")?
            .split(";")
            .filter_map(|s| {
                let s_str = s.trim().to_string();
                if !s_str.is_empty() { Some(s_str) } else { None }
            })
            .collect();
        Ok(Config {
            database_host,
            database_port,
            database_user,
            database_password,
            database_tables,
        })
    }
}
