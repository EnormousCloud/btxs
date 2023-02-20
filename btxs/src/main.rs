use clap::Parser;
use kv::KV;
use tracing::*;

#[derive(Debug, Clone, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// database url
    #[arg(long, env)]
    database_url: String,
}

mod logging {
    use tracing_error::ErrorLayer;
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    pub fn start() {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("sqlx::query=WARN,INFO"));
        let is_atty = atty::is(atty::Stream::Stdout);
        let subscriber = tracing_subscriber::fmt::fmt()
            .with_env_filter(env_filter)
            .with_ansi(is_atty)
            .with_span_events(fmt::format::FmtSpan::CLOSE) // enable durations
            .finish();
        // stacking up layers
        _ = subscriber.with(ErrorLayer::default()).try_init(); // suppress error about global
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::start();
    let _ = color_eyre::install();

    let args = Args::parse();
    debug!("args {:?}", args);

    let storage = kv::PostgresKV::new(&args.database_url, "btxs_blocks").await;
    storage.set(1000, vec![]).await.unwrap();
    let result = storage.get(1000).await.unwrap().unwrap();

    println!("{:?}", result);
    Ok(())
}
