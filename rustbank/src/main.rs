use std::{path::PathBuf, process};
use anyhow::Result;
use dotenvy;
use clap::{Parser, Subcommand, Args};
//use rustbank::Backup;
use rustbank::{backup, generate, upload, Config};

#[derive(Parser, Debug)]
#[command(name = "rustbank")]
#[command(version, about="Simulates a tool to upload to and backup data from a bank server.")]
struct Cli {
    #[command(subcommand)]
    command: Commands
}

#[derive(Subcommand, Debug)]
enum Commands {
    Backup,
    Upload(UploadCommand),
    Generate(GenerateCommand),
}

impl Commands {

    async fn execute(&self, config: &Config) -> Result<()> {
        match self {
            Commands::Backup => {
                backup(config).await?
            },
            Commands::Generate(args) => {
                generate(args.path.clone(), args.rows)?
            },
            Commands::Upload(args) => {
                upload(config, args.path.clone()).await?
            }
        }
        Ok(())
    }
}

#[derive(Args, Debug)]
struct UploadCommand {
    #[arg(value_name = "DIR")]
    path: PathBuf,
}

#[derive(Args, Debug)]
struct GenerateCommand {
    #[arg(value_name = "DIR")]
    path: PathBuf,

    #[arg(short, long, default_value_t=3000)]
    rows: usize,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
        //Detect parsing errors in the .env file only.
    if let Err(e @ dotenvy::Error::LineParse(..)) = dotenvy::dotenv() {
        println!("Error parsing .env file\n{e}");
        process::exit(1);
    }

    let config = Config::build().unwrap_or_else(|error| {
        eprintln!("{error}");
        process::exit(1);
    });

    if let Err(e) = cli.command.execute(&config).await {
        eprintln!("{e}");
        process::exit(1);
    }

}
