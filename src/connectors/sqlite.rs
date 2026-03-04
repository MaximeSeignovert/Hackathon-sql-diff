use std::collections::BTreeMap;

use anyhow::Result;
use async_trait::async_trait;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Row, SqlitePool};

use super::{ColumnInfo, ConnectorKind, ConstraintInfo, ForeignKeyInfo, IndexInfo, SchemaConnector, TableInfo};

pub struct SqliteConnector {
    pool: SqlitePool,
}

impl SqliteConnector {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl SchemaConnector for SqliteConnector {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::Sqlite
    }

    async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let tables = rows
            .iter()
            .map(|row| row.try_get::<String, _>("name"))
            .collect::<sqlx::Result<Vec<_>>>()?;

        Ok(tables)
    }

    async fn load_schema(&self) -> Result<Vec<TableInfo>> {
        let tables = self.list_tables().await?;
        let mut schema = Vec::with_capacity(tables.len());

        for table_name in tables {
            let escaped = table_name.replace('"', "\"\"");
            let pragma_sql = format!("PRAGMA table_info(\"{escaped}\")");
            let column_rows = sqlx::query(&pragma_sql).fetch_all(&self.pool).await?;

            let columns = column_rows
                .iter()
                .map(|row| {
                    Ok(ColumnInfo {
                        name: row.try_get("name")?,
                        data_type: row.try_get::<String, _>("type")?,
                        not_null: row.try_get::<i64, _>("notnull")? == 1,
                        default_value: row.try_get("dflt_value")?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let indexes = load_indexes(&self.pool, &table_name).await?;
            let foreign_keys = load_foreign_keys(&self.pool, &table_name).await?;
            let constraints = load_constraints(&self.pool, &table_name).await?;

            schema.push(TableInfo {
                name: table_name,
                columns,
                indexes,
                foreign_keys,
                constraints,
            });
        }

        Ok(schema)
    }
}

async fn load_indexes(pool: &SqlitePool, table_name: &str) -> Result<Vec<IndexInfo>> {
    let escaped_table = table_name.replace('"', "\"\"");
    let index_list_sql = format!("PRAGMA index_list(\"{escaped_table}\")");
    let index_rows = sqlx::query(&index_list_sql).fetch_all(pool).await?;

    let mut indexes = Vec::new();
    for row in index_rows {
        let origin: String = row.try_get("origin")?;
        if origin == "pk" {
            continue;
        }

        let index_name: String = row.try_get("name")?;
        let escaped_index = index_name.replace('"', "\"\"");
        let index_info_sql = format!("PRAGMA index_info(\"{escaped_index}\")");
        let cols_rows = sqlx::query(&index_info_sql).fetch_all(pool).await?;
        let mut columns = Vec::new();
        for c in cols_rows {
            let name: String = c.try_get("name")?;
            columns.push(name);
        }

        indexes.push(IndexInfo {
            name: index_name,
            columns,
            unique: row.try_get::<i64, _>("unique")? == 1,
        });
    }

    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(indexes)
}

/// Extrait les contraintes UNIQUE et CHECK depuis sqlite_master en parsant le DDL.
async fn load_constraints(pool: &SqlitePool, table_name: &str) -> Result<Vec<ConstraintInfo>> {
    let escaped = table_name.replace('\'', "''");
    let row = sqlx::query(&format!(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '{}'",
        escaped
    ))
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(vec![]);
    };
    let ddl: Option<String> = row.try_get("sql")?;
    let Some(ddl) = ddl else {
        return Ok(vec![]);
    };

    Ok(parse_sqlite_constraints(&ddl, table_name))
}

/// Parse les contraintes UNIQUE et CHECK inline d'un DDL SQLite CREATE TABLE.
fn parse_sqlite_constraints(ddl: &str, table_name: &str) -> Vec<ConstraintInfo> {
    let open = match ddl.find('(') {
        Some(i) => i,
        None => return vec![],
    };
    let close = match ddl.rfind(')') {
        Some(i) => i,
        None => return vec![],
    };
    let body = &ddl[open + 1..close];
    let mut constraints = Vec::new();
    let mut auto_idx = 0usize;

    for part in split_top_level_comma(body) {
        let item = part.trim();
        let lowered = item.to_lowercase();

        let (name, rest) = if lowered.starts_with("constraint") {
            let tokens: Vec<&str> = item.splitn(3, char::is_whitespace).collect();
            let n = tokens.get(1).map(|s| s.trim_matches('"').trim_matches('`').to_lowercase()).unwrap_or_default();
            let r = tokens.get(2).copied().unwrap_or("");
            (n, r)
        } else {
            auto_idx += 1;
            (format!("auto_{}_{}_{}", table_name, if lowered.contains("unique") { "uq" } else { "ck" }, auto_idx), item)
        };

        let rest_lower = rest.to_lowercase();
        if rest_lower.trim_start().starts_with("unique") {
            if let Some(cols) = extract_cols_from_parens(rest) {
                constraints.push(ConstraintInfo {
                    name,
                    kind: "unique".to_owned(),
                    columns: cols,
                    expression: None,
                });
            }
        } else if rest_lower.trim_start().starts_with("check") {
            if let Some(expr) = extract_check_expression(rest) {
                constraints.push(ConstraintInfo {
                    name,
                    kind: "check".to_owned(),
                    columns: vec![],
                    expression: Some(expr),
                });
            }
        }
    }

    constraints
}

fn split_top_level_comma(input: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut in_single = false;
    let mut in_double = false;
    for ch in input.chars() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '(' if !in_single && !in_double => depth += 1,
            ')' if !in_single && !in_double && depth > 0 => depth -= 1,
            _ => {}
        }
        if ch == ',' && depth == 0 && !in_single && !in_double {
            parts.push(current.trim().to_owned());
            current.clear();
        } else {
            current.push(ch);
        }
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_owned());
    }
    parts
}

fn extract_cols_from_parens(s: &str) -> Option<Vec<String>> {
    let open = s.find('(')?;
    let close = s.rfind(')')?;
    let inner = &s[open + 1..close];
    let cols = inner
        .split(',')
        .map(|c| c.trim().trim_matches('"').trim_matches('`').to_lowercase())
        .filter(|c| !c.is_empty())
        .collect();
    Some(cols)
}

fn extract_check_expression(s: &str) -> Option<String> {
    let open = s.find('(')?;
    let close = s.rfind(')')?;
    Some(s[open + 1..close].trim().to_owned())
}

async fn load_foreign_keys(pool: &SqlitePool, table_name: &str) -> Result<Vec<ForeignKeyInfo>> {
    let escaped_table = table_name.replace('"', "\"\"");
    let fk_sql = format!("PRAGMA foreign_key_list(\"{escaped_table}\")");
    let fk_rows = sqlx::query(&fk_sql).fetch_all(pool).await?;

    let mut grouped: BTreeMap<i64, ForeignKeyInfo> = BTreeMap::new();
    for row in fk_rows {
        let id: i64 = row.try_get("id")?;
        let local_col: String = row.try_get("from")?;
        let ref_col: String = row.try_get("to")?;
        let referenced_table: String = row.try_get("table")?;

        let entry = grouped.entry(id).or_insert_with(|| ForeignKeyInfo {
            name: format!("fk_{}_{}", table_name, id),
            columns: Vec::new(),
            referenced_table,
            referenced_columns: Vec::new(),
        });

        entry.columns.push(local_col);
        entry.referenced_columns.push(ref_col);
    }

    let mut fks = grouped.into_values().collect::<Vec<_>>();
    fks.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(fks)
}
