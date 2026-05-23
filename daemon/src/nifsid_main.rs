use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber has failed");

    info!("starting Nifsi daemon....");

    info!("Daemon initialized. Listening on port 7331. ");

    loop{
        tokio::time::sleep(tokio::time::Duration::from_secs(70)).await;
    }
}
