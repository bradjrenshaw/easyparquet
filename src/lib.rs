use std::error::Error;
mod config;
pub use config::Config;

pub fn run(config: Config) -> Result<(), Box<dyn Error>> {
    println!("{config:?}");
    Ok(())
}
