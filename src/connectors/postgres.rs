use std::collections::BTreeMap;

use anyhow::Result;
use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

use super::{ColumnInfo, ConnectorKind, ForeignKeyInfo, IndexInfo, SchemaConnector, TableInfo};

pub struct PostgresConnector {
    pool: PgPool,
}

impl PostgresConnector {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl SchemaConnector for PostgresConnector {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::Postgres
    }

    async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let tables = rows
            .iter()
            .map(|row| row.try_get::<String, _>("table_name"))
            .collect::<sqlx::Result<Vec<_>>>()?;

        Ok(tables)
    }

    async fn load_schema(&self) -> Result<Vec<TableInfo>> {
        let column_rows = sqlx::query(
            r#"
            SELECT
                c.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default
            FROM information_schema.columns c
            INNER JOIN information_schema.tables t
                ON c.table_schema = t.table_schema
                AND c.table_name = t.table_name
            WHERE c.table_schema = 'public'
              AND t.table_type = 'BASE TABLE'
            ORDER BY c.table_name, c.ordinal_position
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let index_rows = sqlx::query(
            r#"
            SELECT
                t.relname AS table_name,
                i.relname AS index_name,
                ix.indisunique AS is_unique,
                ARRAY_AGG(a.attname ORDER BY arr.ord) AS columns
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY arr(attnum, ord) ON TRUE
            LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = arr.attnum
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = 'public'
              AND t.relkind = 'r'
              AND NOT ix.indisprimary
              AND arr.attnum > 0
            GROUP BY t.relname, i.relname, ix.indisunique
            ORDER BY t.relname, i.relname
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let fk_rows = sqlx::query(
            r#"
            SELECT
                tc.table_name,
                tc.constraint_name,
                ccu.table_name AS referenced_table,
                ARRAY_AGG(kcu.column_name::text ORDER BY kcu.ordinal_position) AS local_columns,
                ARRAY_AGG(ccu.column_name::text ORDER BY kcu.ordinal_position) AS referenced_columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.constraint_schema
            WHERE tc.table_schema = 'public'
              AND tc.constraint_type = 'FOREIGN KEY'
            GROUP BY tc.table_name, tc.constraint_name, ccu.table_name
            ORDER BY tc.table_name, tc.constraint_name
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut by_table_columns: BTreeMap<String, Vec<ColumnInfo>> = BTreeMap::new();
        let mut by_table_indexes: BTreeMap<String, Vec<IndexInfo>> = BTreeMap::new();
        let mut by_table_fks: BTreeMap<String, Vec<ForeignKeyInfo>> = BTreeMap::new();

        for row in column_rows {
            let table_name: String = row.try_get("table_name")?;
            let column = ColumnInfo {
                name: row.try_get("column_name")?,
                data_type: row.try_get("data_type")?,
                not_null: row.try_get::<String, _>("is_nullable")? == "NO",
                default_value: row.try_get("column_default")?,
            };
            by_table_columns.entry(table_name).or_default().push(column);
        }

        for row in index_rows {
            let table_name: String = row.try_get("table_name")?;
            let index = IndexInfo {
                name: row.try_get("index_name")?,
                columns: row.try_get("columns")?,
                unique: row.try_get("is_unique")?,
            };
            by_table_indexes.entry(table_name).or_default().push(index);
        }

        for row in fk_rows {
            let table_name: String = row.try_get("table_name")?;
            let fk = ForeignKeyInfo {
                name: row.try_get("constraint_name")?,
                columns: row.try_get("local_columns")?,
                referenced_table: row.try_get("referenced_table")?,
                referenced_columns: row.try_get("referenced_columns")?,
            };
            by_table_fks.entry(table_name).or_default().push(fk);
        }

        let tables = by_table_columns
            .into_iter()
            .map(|(name, mut columns)| {
                columns.sort_by(|a, b| a.name.cmp(&b.name));
                let mut indexes = by_table_indexes.remove(&name).unwrap_or_default();
                indexes.sort_by(|a, b| a.name.cmp(&b.name));
                let mut foreign_keys = by_table_fks.remove(&name).unwrap_or_default();
                foreign_keys.sort_by(|a, b| a.name.cmp(&b.name));
                TableInfo {
                    name,
                    columns,
                    indexes,
                    foreign_keys,
                }
            })
            .collect();

        Ok(tables)
    }
}
