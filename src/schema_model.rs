use std::collections::BTreeMap;

use crate::connectors::TableInfo;

#[derive(Debug, Clone)]
pub struct SchemaModel {
    pub tables: BTreeMap<String, Table>,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: BTreeMap<String, Column>,
    pub indexes: BTreeMap<String, Index>,
    pub foreign_keys: BTreeMap<String, ForeignKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    pub not_null: bool,
    pub default_value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

impl SchemaModel {
    pub fn from_connector_tables(tables: Vec<TableInfo>) -> Self {
        let mut by_name = BTreeMap::new();

        for table in tables {
            let mut columns = BTreeMap::new();
            let mut indexes = BTreeMap::new();
            let mut foreign_keys = BTreeMap::new();
            for col in table.columns {
                let column = Column {
                    name: col.name.clone(),
                    data_type: normalize_type(&col.data_type),
                    not_null: col.not_null,
                    default_value: normalize_default(col.default_value),
                };
                columns.insert(col.name, column);
            }
            for idx in table.indexes {
                let index = Index {
                    name: idx.name.clone(),
                    columns: idx
                        .columns
                        .into_iter()
                        .map(|c| normalize_identifier(&c))
                        .collect(),
                    unique: idx.unique,
                };
                indexes.insert(idx.name, index);
            }
            for fk in table.foreign_keys {
                let key = ForeignKey {
                    name: fk.name.clone(),
                    columns: fk
                        .columns
                        .into_iter()
                        .map(|c| normalize_identifier(&c))
                        .collect(),
                    referenced_table: normalize_identifier(&fk.referenced_table),
                    referenced_columns: fk
                        .referenced_columns
                        .into_iter()
                        .map(|c| normalize_identifier(&c))
                        .collect(),
                };
                foreign_keys.insert(fk.name, key);
            }
            let model_table = Table {
                name: table.name.clone(),
                columns,
                indexes,
                foreign_keys,
            };
            by_name.insert(table.name, model_table);
        }

        Self { tables: by_name }
    }
}

fn normalize_type(raw: &str) -> String {
    canonical_type(raw)
}

/// Canonical type for cross-SGBD comparison. Used by connectors and SQL dump parser.
/// Documented equivalences: serial~integer, numeric(p,s)~numeric, timestamp variants~timestamp.
pub fn canonical_type(raw: &str) -> String {
    let t = raw.trim().to_lowercase();
    let base = t
        .split(|c: char| c == '(' || c.is_whitespace())
        .next()
        .unwrap_or(t.as_str());
    match base {
        "serial" | "bigserial" | "smallserial" => "integer".to_owned(),
        "int" | "int4" | "integer" => "integer".to_owned(),
        "int2" | "smallint" => "smallint".to_owned(),
        "int8" | "bigint" => "bigint".to_owned(),
        "numeric" | "decimal" => "numeric".to_owned(),
        "real" | "float4" => "real".to_owned(),
        "double" | "float8" => "double".to_owned(),
        "float" => "double".to_owned(),
        "character" | "varchar" | "char" | "nvarchar" => "text".to_owned(),
        "text" | "string" | "clob" => "text".to_owned(),
        "timestamp" | "timestamptz" | "datetime" => "timestamp".to_owned(),
        "boolean" | "bool" => "boolean".to_owned(),
        "date" => "date".to_owned(),
        "time" => "time".to_owned(),
        _ => {
            if t.starts_with("timestamp") {
                "timestamp".to_owned()
            } else if t.starts_with("varchar") || t.starts_with("character varying") {
                "text".to_owned()
            } else if t.starts_with("numeric") || t.starts_with("decimal") {
                "numeric".to_owned()
            } else if t.starts_with("double") || t.starts_with("float") {
                "double".to_owned()
            } else {
                t
            }
        }
    }
}

fn normalize_identifier(raw: &str) -> String {
    raw.trim().trim_matches('"').to_lowercase()
}

fn normalize_default(raw: Option<String>) -> Option<String> {
    raw.and_then(|v| {
        let normalized = v.trim().to_owned();
        if normalized.is_empty() {
            None
        } else {
            Some(normalized)
        }
    })
}
