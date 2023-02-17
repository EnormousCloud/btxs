use clap::Parser;
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

#[async_trait::async_trait]
trait KV {
    // get returns the block data from persistent storage
    async fn get(&self, n: u32) -> anyhow::Result<Option<Vec<u8>>>;
    // set updates or inserts the block into persistent storage
    async fn set(&self, n: u32, v: Vec<u8>) -> anyhow::Result<()>;
}

mod kv {
    use async_trait::async_trait;
    use sqlx::postgres::PgPool;
    use sqlx::Row;
    use tracing::*;

    #[derive(Debug)]
    pub struct PostgresKV {
        pub db: PgPool,
        pub table_name: String,
    }

    impl PostgresKV {
        pub async fn new(database_url: &str, table_name: &str) -> Self {
            let db = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(&database_url)
                .await
                .expect("could not connect to database_url");

            info!("checking postgres tables");
            sqlx::query(&format!(
                "CREATE TABLE IF NOT EXISTS {} (\"k\" INTEGER, \"v\" BYTEA, PRIMARY KEY (\"k\"))",
                table_name,
            ))
            .execute(&db)
            .await
            .expect("init database");

            Self {
                db,
                table_name: table_name.to_string(),
            }
        }
    }

    #[derive(sqlx::FromRow)]
    pub struct Record {
        pub k: u32,
        pub v: Vec<u8>,
    }

    #[async_trait]
    impl super::KV for PostgresKV {
        #[instrument(level = "TRACE")]
        async fn get(&self, n: u32) -> anyhow::Result<Option<Vec<u8>>> {
            let sql = format!("SELECT v FROM {} WHERE k=$1 LIMIT 1", self.table_name);
            let rows = sqlx::query(&sql)
                .bind(n as i32)
                .fetch_optional(&self.db)
                .await?;
            Ok(match rows {
                Some(row) => Some(row.get::<Vec<u8>, _>("v")),
                None => None,
            })
        }

        #[instrument(level = "TRACE")]
        async fn set(&self, n: u32, v: Vec<u8>) -> anyhow::Result<()> {
            let sql = format!(
                "INSERT INTO {} (k, v) VALUES ($1, $2) ON CONFLICT(k) DO UPDATE SET v=$2",
                self.table_name
            );
            let _ = sqlx::query(&sql)
                .bind(n as i32)
                .bind(v)
                .execute(&self.db)
                .await?
                .rows_affected();
            Ok(())
        }
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
