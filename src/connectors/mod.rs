mod postgres;
mod sqlite;

use anyhow::Result;
use async_trait::async_trait;
use clap::ValueEnum;

pub use postgres::PostgresConnector;
pub use sqlite::SqliteConnector;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ConnectorKind {
    Postgres,
    Sqlite,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub not_null: bool,
    pub default_value: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub indexes: Vec<IndexInfo>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
}

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone)]
pub struct ForeignKeyInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

#[async_trait]
pub trait SchemaConnector: Send + Sync {
    fn kind(&self) -> ConnectorKind;
    async fn ping(&self) -> Result<()>;
    async fn list_tables(&self) -> Result<Vec<String>>;
    async fn load_schema(&self) -> Result<Vec<TableInfo>>;
}
