use dotenvy;
use easyparquet::{Config, run};
use std::process;
use tokio;

#[tokio::main]
async fn main() {
    //Detect parsing errors in the .env file only.
    if let Err(e @ dotenvy::Error::LineParse(..)) = dotenvy::dotenv() {
        eprintln!("Error parsing .env file\n{e}");
        process::exit(1);
    }

    let config = Config::build().unwrap_or_else(|error| {
        eprintln!("Could not build config\n{error}");
        process::exit(1);
    });

    if let Err(e) = run(config).await {
        eprintln!("{e}");
        process::exit(1);
    }
}
