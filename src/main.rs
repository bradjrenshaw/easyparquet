use dotenvy;
use easyparquet::{Config, run};
use std::process;

fn main() {
    //Detect parsing errors in the .env file only.
    if let Err(e @ dotenvy::Error::LineParse(..)) = dotenvy::dotenv() {
        println!("Error parsing .env file\n{e}");
        process::exit(1);
    }

    let config = Config::build().unwrap_or_else(|error| {
        eprintln!("Could not build config\n{error}");
        process::exit(1);
    });

    if let Err(e) = run(config) {
        eprintln!("{e}");
        process::exit(1);
    }
}
