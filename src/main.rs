use sqlx::{ConnectOptions, Executor};
use std::{path::PathBuf, str::FromStr};
use tempfile::tempdir;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use tracing_test::traced_test;

static DEFAULT_ENV_FILTER: &str = "info,sqlx=trace,rusqlite=trace";
static CREATE_TABLE: &str = r#"
  create table test (id integer primary key, name text);
  insert into test (name) values ('hello');
  insert into test (name) values ('world');
"#;

static SELECT_ALL: &str = "select * from test";

#[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
struct Test {
    id: i64,
    name: String,
}

#[tracing::instrument(level = "debug", name = "Creating temp db and ensuring it exists")]
fn ensure_temp_dir_for_test_db_target_path_buffer() -> PathBuf {
    let new_db = tempdir().unwrap().path().join("new.db");

    tracing::info!(name: "Temp db location", ?new_db);

    std::fs::create_dir_all(new_db.parent().unwrap()).unwrap();

    new_db
}

#[tracing::instrument(level = "debug", name = "Creating vacuum statement")]
fn vacuum_into(new_db: &str) -> String {
    dbg!(format!("vacuum into '{}'", new_db))
}

#[tracing::instrument(level = "debug", name = "Running sqlx with shared cache connection")]
async fn sqlx_with_shared_cache_connection() -> Result<bool, Box<dyn std::error::Error>> {
    let new_db = ensure_temp_dir_for_test_db_target_path_buffer();

    tracing::info!(name = "Creating connection with shared cache");
    let mut conn = sqlx::sqlite::SqliteConnectOptions::from_str(":memory:")?
        .shared_cache(true)
        .connect()
        .await?;

    tracing::info!(name = "Creating table and inserting data", CREATE_TABLE);
    sqlx::query(CREATE_TABLE).execute(&mut conn).await?;

    tracing::info!(name = "Selecting all data from memory table", SELECT_ALL);
    let things = sqlx::query_as::<_, Test>(SELECT_ALL)
        .fetch_all(&mut conn)
        .await?;

    assert_eq!(things.len(), 2);

    let vacuum_statement = vacuum_into(new_db.to_str().unwrap());

    tracing::info!(
        name = "Vacuuming into new db",
        path = new_db.to_str().unwrap()
    );
    conn.execute(vacuum_statement.as_ref()).await?;

    tracing::info!(name = "Dropping connection");
    drop(conn);

    tracing::info!(name = "Checking if new db exists");

    Ok(new_db.exists())
}

#[tracing::instrument(level = "debug", name = "Running sqlx with pooled connection")]
async fn sqlx_with_pooled_connection() -> Result<bool, Box<dyn std::error::Error>> {
    let new_db = ensure_temp_dir_for_test_db_target_path_buffer();

    tracing::info!(name = "Creating pool with shared cache");
    let options = sqlx::sqlite::SqliteConnectOptions::from_str(":memory:")?.shared_cache(true);
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;

    tracing::info!(name = "Creating table and inserting data", CREATE_TABLE);
    sqlx::query(CREATE_TABLE).execute(&pool).await?;

    tracing::info!(name = "Selecting all data from memory table", SELECT_ALL);
    let things = sqlx::query_as::<_, Test>(SELECT_ALL)
        .fetch_all(&pool)
        .await?;

    assert_eq!(things.len(), 2);

    let vacuum_statement = vacuum_into(new_db.to_str().unwrap());

    tracing::info!(
        name = "Vacuuming into new db",
        path = new_db.to_str().unwrap()
    );
    pool.execute(vacuum_statement.as_ref()).await?;

    tracing::info!(name = "Dropping pool");
    drop(pool);

    tracing::info!(name = "Checking if new db exists");

    Ok(new_db.exists())
}

#[tracing::instrument(level = "debug", name = "Running rusqlite")]
async fn rusqlite() -> Result<(), Box<dyn std::error::Error>> {
    let new_db = ensure_temp_dir_for_test_db_target_path_buffer();
    let new_db_path = new_db.to_str().unwrap();
    let conn = rusqlite::Connection::open_in_memory().unwrap();

    tracing::info!(name = "Creating table and inserting data", CREATE_TABLE);
    conn.execute_batch(CREATE_TABLE).unwrap();

    tracing::info!(name = "Selecting all data from memory table", SELECT_ALL);
    let mut statement = conn.prepare(SELECT_ALL).unwrap();
    let things: Vec<Test> = statement
        .query_map([], |row| {
            Ok(Test {
                id: row.get(0).expect(""),
                name: row.get(1).expect(""),
            })
        })
        .unwrap()
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert!(things.len() == 2);

    tracing::info!(name = "Vacuuming into new db", new_db_path);
    conn.execute("vacuum into $1", [new_db_path]).unwrap();

    tracing::info!(name = "Dropping statement and connection");
    drop(statement);
    drop(conn);

    tracing::info!(name = "Opening new db connection to temp db path");
    let file_conn = rusqlite::Connection::open(new_db_path).unwrap();
    let mut statement = file_conn.prepare(SELECT_ALL).unwrap();

    tracing::info!(name = "Selecting all data from temp db");
    let stored_things: Vec<Test> = statement
        .query_map([], |row| {
            Ok(Test {
                id: row.get(0).expect(""),
                name: row.get(1).expect(""),
            })
        })
        .unwrap()
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert!(stored_things.len() == 2);

    tracing::info!(name = "Comparing stored rows with original rows");
    assert!(stored_things == things);

    Ok(())
}

#[traced_test]
#[tokio::test]
async fn test_rusqlite() -> Result<(), Box<dyn std::error::Error>> {
    rusqlite().await
}

#[traced_test]
#[tokio::test]
async fn test_sqlx_with_pools() -> Result<(), Box<dyn std::error::Error>> {
    assert!(sqlx_with_pooled_connection().await?);

    Ok(())
}

#[traced_test]
#[tokio::test]
async fn test_sqlx_with_shared_cache_connection() -> Result<(), Box<dyn std::error::Error>> {
    assert!(sqlx_with_shared_cache_connection().await?);

    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| DEFAULT_ENV_FILTER.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let _ = sqlx_with_pooled_connection().await;
    let _ = sqlx_with_shared_cache_connection().await;
    let _ = rusqlite().await;
}
