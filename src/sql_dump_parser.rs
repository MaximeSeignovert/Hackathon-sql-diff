use std::collections::BTreeMap;

use anyhow::Result;

use crate::schema_model::{Column, ForeignKey, Index, SchemaModel, Table};

pub fn parse_schema_from_sql(sql: &str) -> Result<SchemaModel> {
    let mut tables: BTreeMap<String, Table> = BTreeMap::new();

    for statement in split_sql_statements(sql) {
        let stmt = statement.trim();
        if stmt.is_empty() {
            continue;
        }

        let lowered = stmt.to_lowercase();
        if lowered.starts_with("create table") {
            if let Some((name, table)) = parse_create_table(stmt) {
                tables.insert(name, table);
            }
            continue;
        }
        if lowered.starts_with("create index") || lowered.starts_with("create unique index") {
            if let Some((table_name, index)) = parse_create_index(stmt) {
                tables
                    .entry(table_name.clone())
                    .or_insert_with(|| empty_table(&table_name))
                    .indexes
                    .insert(index.name.clone(), index);
            }
            continue;
        }
        if lowered.starts_with("alter table") && lowered.contains("foreign key") {
            if let Some((table_name, fk)) = parse_alter_table_fk(stmt) {
                tables
                    .entry(table_name.clone())
                    .or_insert_with(|| empty_table(&table_name))
                    .foreign_keys
                    .insert(fk.name.clone(), fk);
            }
        }
    }

    Ok(SchemaModel { tables })
}

fn parse_create_table(statement: &str) -> Option<(String, Table)> {
    let rest = statement.strip_prefix("CREATE TABLE").or_else(|| {
        statement
            .strip_prefix("create table")
            .or_else(|| statement.strip_prefix("Create Table"))
    })?;

    let open_idx = rest.find('(')?;
    let table_part = rest[..open_idx].replace("IF NOT EXISTS", "");
    let table_name = normalize_identifier_token(table_part.trim())?;
    let close_idx = rest.rfind(')')?;
    let body = &rest[open_idx + 1..close_idx];

    let mut columns = BTreeMap::new();
    let mut foreign_keys = BTreeMap::new();
    for part in split_top_level(body, ',') {
        let item = part.trim();
        if item.is_empty() {
            continue;
        }
        let lowered = item.to_lowercase();
        if lowered.contains("foreign key") {
            if let Some(fk) = parse_inline_fk(item, &table_name) {
                foreign_keys.insert(fk.name.clone(), fk);
            }
            continue;
        }
        if let Some(column) = parse_column_def(item) {
            columns.insert(column.name.clone(), column);
        }
    }

    Some((
        table_name.clone(),
        Table {
            name: table_name,
            columns,
            indexes: BTreeMap::new(),
            foreign_keys,
        },
    ))
}

fn parse_create_index(statement: &str) -> Option<(String, Index)> {
    let tokens = statement.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 6 {
        return None;
    }

    let unique = tokens.get(1)?.eq_ignore_ascii_case("unique");
    let idx_pos = if unique { 2 } else { 1 };
    if !tokens.get(idx_pos)?.eq_ignore_ascii_case("index") {
        return None;
    }
    let mut name_pos = idx_pos + 1;
    if tokens.get(name_pos)?.eq_ignore_ascii_case("if")
        && tokens.get(name_pos + 1)?.eq_ignore_ascii_case("not")
        && tokens.get(name_pos + 2)?.eq_ignore_ascii_case("exists")
    {
        name_pos += 3;
    }
    let index_name = normalize_identifier_token(tokens.get(name_pos)?)?;

    let on_pos = tokens.iter().position(|t| t.eq_ignore_ascii_case("on"))?;
    let table_token = tokens.get(on_pos + 1)?;
    // Table name only: "users" from "users" or "users(email)" (avoid phantom table "users(email)")
    let table_name = extract_table_name_from_index_on(table_token)?;

    let open_idx = statement.find('(')?;
    let close_idx = statement.rfind(')')?;
    let cols_body = &statement[open_idx + 1..close_idx];
    let columns = split_top_level(cols_body, ',')
        .into_iter()
        .filter_map(|c| normalize_identifier_token(c.trim()))
        .collect::<Vec<_>>();

    Some((
        table_name,
        Index {
            name: index_name,
            columns,
            unique,
        },
    ))
}

fn parse_alter_table_fk(statement: &str) -> Option<(String, ForeignKey)> {
    let tokens = statement.split_whitespace().collect::<Vec<_>>();
    let table_name = normalize_identifier_token(tokens.get(2)?)?;
    parse_inline_fk(statement, &table_name).map(|fk| (table_name, fk))
}

fn parse_inline_fk(item: &str, table_name: &str) -> Option<ForeignKey> {
    let lowered = item.to_lowercase();
    let name = if lowered.starts_with("constraint") {
        let tokens = item.split_whitespace().collect::<Vec<_>>();
        normalize_identifier_token(tokens.get(1)?)
            .unwrap_or_else(|| format!("fk_{}_auto", table_name))
    } else {
        format!("fk_{}_auto", table_name)
    };

    let fk_pos = lowered.find("foreign key")?;
    let after_fk = &item[fk_pos + "foreign key".len()..];
    let local_open = after_fk.find('(')?;
    let local_close = after_fk[local_open + 1..].find(')')? + local_open + 1;
    let local_cols = &after_fk[local_open + 1..local_close];

    let ref_pos = lowered.find("references")?;
    let after_ref = &item[ref_pos + "references".len()..].trim();
    let ref_open = after_ref.find('(')?;
    let ref_table = normalize_identifier_token(after_ref[..ref_open].trim())?;
    let ref_close = after_ref[ref_open + 1..].find(')')? + ref_open + 1;
    let ref_cols = &after_ref[ref_open + 1..ref_close];

    Some(ForeignKey {
        name,
        columns: split_top_level(local_cols, ',')
            .into_iter()
            .filter_map(|c| normalize_identifier_token(c.trim()))
            .collect(),
        referenced_table: ref_table,
        referenced_columns: split_top_level(ref_cols, ',')
            .into_iter()
            .filter_map(|c| normalize_identifier_token(c.trim()))
            .collect(),
    })
}

/// Extracts table name from ON clause token: "users" or "users(email)" -> "users"
fn extract_table_name_from_index_on(token: &str) -> Option<String> {
    let trimmed = token.trim().trim_matches('"').trim_matches('`');
    let name = if let Some(paren) = trimmed.find('(') {
        trimmed[..paren].trim()
    } else {
        trimmed
    };
    if name.is_empty() {
        None
    } else {
        Some(name.to_lowercase())
    }
}

fn parse_column_def(item: &str) -> Option<Column> {
    let trimmed = item.trim();
    let lowered = trimmed.to_lowercase();
    if trimmed.is_empty() {
        return None;
    }
    let first_ws = trimmed.find(char::is_whitespace)?;
    let name = normalize_identifier_token(trimmed[..first_ws].trim())?;
    let after_name = trimmed[first_ws..].trim_start();
    let after_name_lower = after_name.to_lowercase();
    let type_end = [
        after_name_lower.find(" not null"),
        after_name_lower.find(" default "),
        after_name_lower.find(" primary key"),
        after_name_lower.find(" unique"),
        after_name_lower.find(" check "),
        after_name_lower.find(" references "),
    ]
    .into_iter()
    .flatten()
    .min()
    .unwrap_or(after_name.len());
    let type_str = after_name[..type_end].trim();
    let data_type = parse_column_type(type_str)?;
    let not_null = lowered.contains("not null");
    let default_value = lowered
        .find(" default ")
        .and_then(|idx| {
            let rest = item[idx + 9..].trim();
            let end = rest
                .find(|c: char| c == ',' || c == ')')
                .unwrap_or(rest.len());
            let value = rest[..end].trim();
            if value.is_empty() {
                None
            } else {
                Some(value.to_owned())
            }
        });

    Some(Column {
        name,
        data_type,
        not_null,
        default_value,
    })
}

/// Parses a full column type and returns canonical type for comparison.
fn parse_column_type(s: &str) -> Option<String> {
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    Some(crate::schema_model::canonical_type(t))
}

fn empty_table(name: &str) -> Table {
    Table {
        name: name.to_owned(),
        columns: BTreeMap::new(),
        indexes: BTreeMap::new(),
        foreign_keys: BTreeMap::new(),
    }
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    split_top_level(sql, ';')
}

fn split_top_level(input: &str, separator: char) -> Vec<String> {
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

        if ch == separator && depth == 0 && !in_single && !in_double {
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

fn normalize_identifier_token(token: &str) -> Option<String> {
    let trimmed = token.trim().trim_matches('"').trim_matches('`');
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_index_on_table_with_parens_extracts_table_name_only() {
        let sql = r#"
            CREATE TABLE users ( id integer, email text );
            CREATE UNIQUE INDEX idx_users_email ON users(email);
        "#;
        let model = parse_schema_from_sql(sql).unwrap();
        assert!(model.tables.contains_key("users"));
        assert!(!model.tables.contains_key("users(email)"));
        let users = &model.tables["users"];
        assert_eq!(users.indexes.len(), 1);
        assert!(users.indexes.contains_key("idx_users_email"));
    }

    #[test]
    fn parse_column_type_parameterized_canonical() {
        let sql = "CREATE TABLE t ( a numeric(10,2), b timestamp without time zone );";
        let model = parse_schema_from_sql(sql).unwrap();
        let t = &model.tables["t"];
        assert_eq!(t.columns["a"].data_type, "numeric");
        assert_eq!(t.columns["b"].data_type, "timestamp");
    }

    #[test]
    fn alter_table_fk_parsed() {
        let sql = r#"
            CREATE TABLE users ( id integer );
            CREATE TABLE orders ( id integer, user_id integer );
            ALTER TABLE orders ADD CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id);
        "#;
        let model = parse_schema_from_sql(sql).unwrap();
        let orders = &model.tables["orders"];
        assert_eq!(orders.foreign_keys.len(), 1);
        let fk = orders.foreign_keys.values().next().unwrap();
        assert_eq!(fk.referenced_table, "users");
        assert_eq!(fk.referenced_columns, vec!["id"]);
    }
}
