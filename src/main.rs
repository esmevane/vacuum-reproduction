use std::{fmt::Debug, str::FromStr};

use sqlx::{ConnectOptions, Executor};
use tempfile::NamedTempFile;
use tracing_test::traced_test;

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

#[derive(Debug, sqlx::FromRow)]
struct Table {
    name: String,
}

#[tracing::instrument(level = "debug", name = "Creating vacuum statement")]
fn vacuum_into(db_str: &str) -> String {
    dbg!(format!("vacuum into '{db_str}'"))
}

#[tracing::instrument(level = "debug", name = "Running sqlx with shared cache connection")]
async fn sqlx_with_shared_cache_connection() -> anyhow::Result<()> {
    let new_db = NamedTempFile::new()?;
    let db_path: &str = new_db.as_ref().as_os_str().try_into()?;

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

    let vacuum_statement = vacuum_into(db_path);

    tracing::info!(name = "Vacuuming into new db", path = db_path);
    conn.execute(vacuum_statement.as_ref()).await?;

    tracing::info!(name = "Dropping connection");
    drop(conn);

    tracing::info!(name = "Checking if new db exists");
    assert!(new_db.as_ref().exists());

    let pool = sqlx::sqlite::SqlitePool::connect(db_path).await?;

    tracing::info!(name = "Checking if tables exist");
    let tables = dbg!(
        sqlx::query_as::<_, Table>("SELECT name FROM sqlite_master WHERE type='table'")
            .fetch_all(&pool)
            .await?
    );

    tracing::info!(name = "Current tables", ?tables);

    assert!(tables.len() >= 1, "No tables found in new db");

    tracing::info!(name = "Selecting all data from temp db");
    let stored_things = sqlx::query_as::<_, Test>(SELECT_ALL)
        .fetch_all(&pool)
        .await?;

    assert_eq!(stored_things.len(), 2);

    tracing::info!(name = "Comparing stored rows with original rows");
    assert_eq!(stored_things, things);

    Ok(())
}

#[tracing::instrument(level = "debug", name = "Running sqlx with pooled connection")]
async fn sqlx_with_pooled_connection() -> anyhow::Result<()> {
    let new_db = NamedTempFile::new()?;
    let db_path: &str = new_db.as_ref().as_os_str().try_into()?;
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

    let vacuum_statement = vacuum_into(db_path);

    tracing::info!(name = "Vacuuming into new db", path = db_path);
    pool.execute(vacuum_statement.as_ref()).await?;

    tracing::info!(name = "Dropping pool");
    drop(pool);

    tracing::info!(name = "Checking if new db exists");

    assert!(new_db.as_ref().exists());

    tracing::info!(name = "Opening new db connection to temp db path");
    let pool = sqlx::sqlite::SqlitePool::connect(db_path).await?;

    tracing::info!(name = "Checking if tables exist");
    let tables = dbg!(
        sqlx::query_as::<_, Table>("SELECT name FROM sqlite_master WHERE type='table'")
            .fetch_all(&pool)
            .await?
    );

    tracing::info!(name = "Current tables", ?tables);

    assert!(tables.len() >= 1, "No tables found in new db");

    tracing::info!(name = "Selecting all data from temp db");
    let stored_things = sqlx::query_as::<_, Test>(SELECT_ALL)
        .fetch_all(&pool)
        .await?;

    assert_eq!(stored_things.len(), 2);

    tracing::info!(name = "Comparing stored rows with original rows");
    assert_eq!(stored_things, things);

    Ok(())
}

#[tracing::instrument(level = "debug", name = "Running rusqlite")]
async fn rusqlite() -> anyhow::Result<()> {
    let new_db = NamedTempFile::new()?;
    let db_path: &str = new_db.as_ref().as_os_str().try_into()?;
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

    tracing::info!(name = "Vacuuming into new db", db_path);
    conn.execute("vacuum into $1", [db_path]).unwrap();

    tracing::info!(name = "Dropping statement and connection");
    drop(statement);
    drop(conn);

    tracing::info!(name = "Opening new db connection to temp db path");
    let file_conn = rusqlite::Connection::open(new_db.as_ref()).unwrap();
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

    assert_eq!(stored_things.len(), 2);

    tracing::info!(name = "Comparing stored rows with original rows");
    assert_eq!(stored_things, things);

    Ok(())
}

#[traced_test]
#[tokio::test]
async fn test_rusqlite() -> anyhow::Result<()> {
    rusqlite().await
}

#[traced_test]
#[tokio::test]
async fn test_sqlx_with_pools() -> anyhow::Result<()> {
    sqlx_with_pooled_connection().await?;

    Ok(())
}

#[traced_test]
#[tokio::test]
async fn test_sqlx_with_shared_cache_connection() -> anyhow::Result<()> {
    sqlx_with_shared_cache_connection().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    sqlx_with_pooled_connection().await?;
    sqlx_with_shared_cache_connection().await?;
    rusqlite().await?;

    Ok(())
}
