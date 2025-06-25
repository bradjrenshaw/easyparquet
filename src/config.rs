use dotenvy;

#[derive(Debug)]
pub struct Config {
    pub database_host: String,
    pub database_port: u16,
    pub database_user: String,
    pub database_password: String,
    pub database_name: String,
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
        let database_host = get_env("database_host")?;
        let database_port: u16 = match get_env("database_port") {
            Ok(e) => match e.parse() {
                Ok(f) => f,
                Err(_) => return Err("Server port must be a valid port.".to_string()),
            },
            Err(_) => 3306, //Default port
        };
        let database_user = get_env("database_user")?;
        let database_password = get_env("database_password")?;
        let database_name = get_env("database_name")?;
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
            database_name,
            database_tables,
        })
    }

    pub fn get_uri(&self) -> String {
                format!("mysql://{}:{}@{}:{}/{}", self.database_user, self.database_password, self.database_host, self.database_port, self.database_name)
    }
}
