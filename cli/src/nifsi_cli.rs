use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name="nifsi")]
#[command(about="Nifsi CLI", long_about=None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Join{
        address: String,
    },
    Leave,
    Status,
    Evict{
        node: String,
    },
}

#[tokio::main]
async fn main() {
    let cli=Cli::parse();

    match &cli.command {
        Commands::Join {address} => {
            println!("Joining mesh via {}....", address);
        }
        Commands::Leave => {
            println!("Leaving mesh....");
        }
        Commands::Status => {
            println!("Querying cluster status...");
        }
        Commands::Evict {node} => {
            println!("Evicting node {}...", node);
        }
    }
}

