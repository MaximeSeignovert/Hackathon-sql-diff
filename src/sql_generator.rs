use crate::connectors::ConnectorKind;
use crate::diff_engine::{ColumnModification, DiffResult};
use crate::schema_model::{ForeignKey, Index, SchemaModel};

/// Target SQL dialect for generated migration script (idempotence and syntax adapted per dialect).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Postgres,
    Sqlite,
}

impl From<ConnectorKind> for SqlDialect {
    fn from(k: ConnectorKind) -> Self {
        match k {
            ConnectorKind::Postgres => SqlDialect::Postgres,
            ConnectorKind::Sqlite => SqlDialect::Sqlite,
        }
    }
}

/// Topological sort of table names based on FK dependencies within the set.
/// Tables with no intra-set dependencies come first. Cycles are broken arbitrarily.
fn topological_sort_tables<'a>(names: &'a [String], target: &SchemaModel) -> Vec<&'a String> {
    use std::collections::{HashMap, HashSet, VecDeque};

    let name_set: HashSet<&str> = names.iter().map(|s| s.as_str()).collect();

    // Build adjacency: dep[A] = tables that A depends on (i.e. A has a FK → dep)
    let mut deps: HashMap<&str, HashSet<&str>> = HashMap::new();
    let mut rdeps: HashMap<&str, HashSet<&str>> = HashMap::new(); // reverse: who depends on me
    let mut in_degree: HashMap<&str, usize> = HashMap::new();

    for name in names {
        deps.entry(name.as_str()).or_default();
        rdeps.entry(name.as_str()).or_default();
        in_degree.entry(name.as_str()).or_insert(0);
    }

    for name in names {
        if let Some(table) = target.tables.get(name) {
            for fk in table.foreign_keys.values() {
                let ref_table = fk.referenced_table.as_str();
                // Only care about deps within the set of new tables
                if name_set.contains(ref_table) && ref_table != name.as_str() {
                    if deps.entry(name.as_str()).or_default().insert(ref_table) {
                        rdeps.entry(ref_table).or_default().insert(name.as_str());
                        *in_degree.entry(name.as_str()).or_insert(0) += 1;
                    }
                }
            }
        }
    }

    // Kahn's algorithm
    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|&(_, &d)| d == 0)
        .map(|(&n, _)| n)
        .collect();
    // Deterministic order within queue
    let mut queue_vec: Vec<&str> = queue.drain(..).collect();
    queue_vec.sort();
    let mut queue: VecDeque<&str> = queue_vec.into_iter().collect();

    let mut sorted: Vec<&str> = Vec::with_capacity(names.len());
    while let Some(node) = queue.pop_front() {
        sorted.push(node);
        if let Some(dependents) = rdeps.get(node) {
            let mut next: Vec<&str> = dependents
                .iter()
                .filter_map(|&dep| {
                    let deg = in_degree.get_mut(dep)?;
                    *deg -= 1;
                    if *deg == 0 { Some(dep) } else { None }
                })
                .collect();
            next.sort();
            for n in next {
                queue.push_back(n);
            }
        }
    }

    // Any remaining (cycle): append in original order
    let sorted_set: HashSet<&str> = sorted.iter().copied().collect();
    for name in names {
        if !sorted_set.contains(name.as_str()) {
            sorted.push(name.as_str());
        }
    }

    // Map back to references into the original slice
    let name_to_ref: HashMap<&str, &String> = names.iter().map(|s| (s.as_str(), s)).collect();
    sorted.into_iter().filter_map(|s| name_to_ref.get(s).copied()).collect()
}

pub fn generate_migration_sql(
    _source: &SchemaModel,
    target: &SchemaModel,
    diff: &DiffResult,
    dialect: SqlDialect,
) -> String {
    let mut out = Vec::new();

    out.push("-- Migration SQL generee automatiquement".to_owned());
    out.push(format!("-- Cible: {:?}", dialect));
    out.push("-- Source -> Target".to_owned());
    out.push(String::new());

    // 1) Create all new tables in topological order (referenced tables before referencing ones).
    let sorted_new_tables = topological_sort_tables(&diff.added_tables, target);
    for table_name in &sorted_new_tables {
        if let Some(table) = target.tables.get(*table_name) {
            out.push(create_table_statement(table, dialect));
        }
    }

    // 2) Apply column/index changes on existing tables.
    for table_diff in &diff.altered_tables {
        for col in &table_diff.added_columns {
            let sql = format!(
                "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {name} {ty}{not_null}{default};",
                table = ident(&table_diff.table_name),
                name = ident(&col.name),
                ty = col.data_type,
                not_null = if col.not_null { " NOT NULL" } else { "" },
                default = format_default(&col.default_value),
            );
            out.push(sql);
        }

        for modified in &table_diff.modified_columns {
            out.extend(generate_modified_column_sql(
                &table_diff.table_name,
                modified,
                dialect,
            ));
        }

        for idx in &table_diff.added_indexes {
            out.push(create_index_statement(&table_diff.table_name, idx, dialect));
        }
        for idx in &table_diff.modified_indexes {
            out.push(drop_index_statement(&idx.old.name, dialect));
            out.push(create_index_statement(&table_diff.table_name, &idx.new, dialect));
        }
        for idx in &table_diff.removed_indexes {
            out.push(drop_index_statement(&idx.name, dialect));
        }

        for fk in &table_diff.added_foreign_keys {
            let _ = fk;
        }
        for fk in &table_diff.modified_foreign_keys {
            out.extend(drop_fk_statements(&table_diff.table_name, &fk.old.name, dialect));
        }
        for fk in &table_diff.removed_foreign_keys {
            out.push(format!(
                "-- DESTRUCTIVE: suppression de cle etrangere {}.{}",
                table_diff.table_name, fk.name
            ));
            out.extend(drop_fk_statements(&table_diff.table_name, &fk.name, dialect));
        }

        for col in &table_diff.removed_columns {
            out.push(format!(
                "-- DESTRUCTIVE: suppression de colonne {}.{}",
                table_diff.table_name, col.name
            ));
            out.push(format!(
                "ALTER TABLE {table} DROP COLUMN IF EXISTS {name};",
                table = ident(&table_diff.table_name),
                name = ident(&col.name),
            ));
        }
    }

    // 3) Create indexes for newly added tables (after table creation).
    for table_name in &sorted_new_tables {
        if let Some(table) = target.tables.get(*table_name) {
            for index in table.indexes.values() {
                out.push(create_index_statement(table_name, index, dialect));
            }
        }
    }

    // 4) Add all foreign keys at the end (better dependency ordering).
    for table_name in &sorted_new_tables {
        if let Some(table) = target.tables.get(*table_name) {
            for fk in table.foreign_keys.values() {
                out.push(add_fk_statement(table_name, fk, dialect));
            }
        }
    }
    for table_diff in &diff.altered_tables {
        for fk in &table_diff.added_foreign_keys {
            out.push(add_fk_statement(&table_diff.table_name, fk, dialect));
        }
        for fk in &table_diff.modified_foreign_keys {
            out.push(add_fk_statement(&table_diff.table_name, &fk.new, dialect));
        }
    }

    // 5) Drop removed tables last (destructive operations at end).
    for table_name in &diff.removed_tables {
        out.push(format!(
            "-- DESTRUCTIVE: suppression de table {}",
            table_name
        ));
        out.push(format!("DROP TABLE IF EXISTS {};", ident(table_name)));
    }

    if diff.added_tables.is_empty()
        && diff.removed_tables.is_empty()
        && diff.altered_tables.is_empty()
    {
        out.push("-- Aucun changement detecte".to_owned());
    }

    out.join("\n")
}

fn generate_modified_column_sql(
    table_name: &str,
    modification: &ColumnModification,
    dialect: SqlDialect,
) -> Vec<String> {
    let mut statements = Vec::new();

    if modification.old.data_type != modification.new.data_type {
        if modification.destructive {
            statements.push(format!(
                "-- ATTENTION: changement de type potentiellement destructif sur {}.{}",
                table_name, modification.old.name
            ));
        }
        match dialect {
            SqlDialect::Postgres => {
                let statement = format!(
                    "ALTER TABLE {table} ALTER COLUMN {name} TYPE {ty};",
                    table = ident(table_name),
                    name = ident(&modification.old.name),
                    ty = modification.new.data_type,
                );
                let expected_type = postgres_information_schema_type(&modification.new.data_type);
                statements.push(postgres_guarded_column_statement(
                    table_name,
                    &modification.old.name,
                    &format!(
                        "LOWER(data_type) <> LOWER('{}')",
                        escape_literal(&expected_type)
                    ),
                    &statement,
                ));
            }
            SqlDialect::Sqlite => {
                statements.push(format!(
                    "-- SQLite: ALTER COLUMN TYPE non supporte. Recreer la table pour {}.{} ({} -> {}).",
                    table_name, modification.old.name, modification.old.data_type, modification.new.data_type
                ));
            }
        }
    }

    if modification.old.not_null != modification.new.not_null {
        match dialect {
            SqlDialect::Postgres => {
                if modification.new.not_null {
                    let statement = format!(
                        "ALTER TABLE {table} ALTER COLUMN {name} SET NOT NULL;",
                        table = ident(table_name),
                        name = ident(&modification.old.name),
                    );
                    statements.push(postgres_guarded_column_statement(
                        table_name,
                        &modification.old.name,
                        "is_nullable = 'YES'",
                        &statement,
                    ));
                } else {
                    let statement = format!(
                        "ALTER TABLE {table} ALTER COLUMN {name} DROP NOT NULL;",
                        table = ident(table_name),
                        name = ident(&modification.old.name),
                    );
                    statements.push(postgres_guarded_column_statement(
                        table_name,
                        &modification.old.name,
                        "is_nullable = 'NO'",
                        &statement,
                    ));
                }
            }
            SqlDialect::Sqlite => {
                statements.push(format!(
                    "-- SQLite: modification NOT NULL sur {}.{} necessite recreation de table.",
                    table_name, modification.old.name
                ));
            }
        }
    }

    if modification.old.default_value != modification.new.default_value {
        match dialect {
            SqlDialect::Postgres => {
                match &modification.new.default_value {
                    Some(new_default) => {
                        statements.push(format!(
                            "ALTER TABLE {table} ALTER COLUMN {name} SET DEFAULT {default};",
                            table = ident(table_name),
                            name = ident(&modification.old.name),
                            default = new_default
                        ));
                    }
                    None => {
                        let statement = format!(
                            "ALTER TABLE {table} ALTER COLUMN {name} DROP DEFAULT;",
                            table = ident(table_name),
                            name = ident(&modification.old.name),
                        );
                        statements.push(postgres_guarded_column_statement(
                            table_name,
                            &modification.old.name,
                            "column_default IS NOT NULL",
                            &statement,
                        ));
                    }
                }
            }
            SqlDialect::Sqlite => {
                statements.push(format!(
                    "-- SQLite: modification DEFAULT sur {}.{} necessite recreation de table.",
                    table_name, modification.old.name
                ));
            }
        }
    }

    statements
}

fn create_table_statement(table: &crate::schema_model::Table, _dialect: SqlDialect) -> String {
    let mut lines = Vec::new();
    for col in table.columns.values() {
        lines.push(format!(
            "  {} {}{}{}",
            ident(&col.name),
            col.data_type,
            if col.not_null { " NOT NULL" } else { "" },
            format_default(&col.default_value)
        ));
    }

    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n{}\n);",
        ident(&table.name),
        lines.join(",\n")
    )
}

fn create_index_statement(table_name: &str, index: &Index, _dialect: SqlDialect) -> String {
    let cols = index
        .columns
        .iter()
        .map(|c| ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE {unique}INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns});",
        unique = if index.unique { "UNIQUE " } else { "" },
        index_name = ident(&index.name),
        table_name = ident(table_name),
        columns = cols
    )
}

fn drop_index_statement(index_name: &str, _dialect: SqlDialect) -> String {
    format!("DROP INDEX IF EXISTS {};", ident(index_name))
}

fn drop_fk_statements(table_name: &str, constraint_name: &str, dialect: SqlDialect) -> Vec<String> {
    match dialect {
        SqlDialect::Postgres => vec![format!(
            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {};",
            ident(table_name),
            ident(constraint_name)
        )],
        SqlDialect::Sqlite => vec![format!(
            "-- SQLite: DROP CONSTRAINT non supporte. Recreer la table sans la contrainte {}.",
            constraint_name
        )],
    }
}

fn add_fk_statement(table_name: &str, fk: &ForeignKey, dialect: SqlDialect) -> String {
    let local_cols = fk
        .columns
        .iter()
        .map(|c| ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let ref_cols = fk
        .referenced_columns
        .iter()
        .map(|c| ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    match dialect {
        SqlDialect::Postgres => format!(
            "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints tc WHERE tc.table_name = '{table}' AND tc.constraint_name = '{constraint}' AND tc.constraint_type = 'FOREIGN KEY') THEN ALTER TABLE {table_ident} ADD CONSTRAINT {constraint_ident} FOREIGN KEY ({local_cols}) REFERENCES {ref_table} ({ref_cols}); END IF; END $$;",
            table = escape_literal(table_name),
            constraint = escape_literal(&fk.name),
            table_ident = ident(table_name),
            constraint_ident = ident(&fk.name),
            local_cols = local_cols,
            ref_table = ident(&fk.referenced_table),
            ref_cols = ref_cols
        ),
        SqlDialect::Sqlite => format!(
            "-- SQLite: ajout FK {}.{} necessite recreation de table (ADD CONSTRAINT non supporte).",
            table_name,
            fk.name
        ),
    }
}

fn format_default(default: &Option<String>) -> String {
    match default {
        Some(v) => format!(" DEFAULT {}", v),
        None => String::new(),
    }
}

fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn postgres_information_schema_type(canonical_type: &str) -> String {
    match canonical_type {
        "timestamp" => "timestamp without time zone".to_owned(),
        "double" => "double precision".to_owned(),
        other => other.to_owned(),
    }
}

fn postgres_guarded_column_statement(
    table_name: &str,
    column_name: &str,
    condition_sql: &str,
    statement: &str,
) -> String {
    format!(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = '{table}' AND column_name = '{column}' AND {condition}) THEN {statement} END IF; END $$;",
        table = escape_literal(table_name),
        column = escape_literal(column_name),
        condition = condition_sql,
        statement = statement
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff_engine::DiffResult;
    use crate::schema_model::{ForeignKey, SchemaModel, Table};
    use std::collections::BTreeMap;

    fn empty_model() -> SchemaModel {
        SchemaModel {
            tables: BTreeMap::new(),
        }
    }

    #[test]
    fn postgres_dialect_fk_uses_do_block() {
        let mut target_tables = BTreeMap::new();
        target_tables.insert(
            "orders".to_string(),
            Table {
                name: "orders".to_string(),
                columns: BTreeMap::new(),
                indexes: BTreeMap::new(),
                foreign_keys: [(
                    "fk_orders_user".to_string(),
                    ForeignKey {
                        name: "fk_orders_user".to_string(),
                        columns: vec!["user_id".to_string()],
                        referenced_table: "users".to_string(),
                        referenced_columns: vec!["id".to_string()],
                    },
                )]
                .into_iter()
                .collect(),
            },
        );
        let target = SchemaModel {
            tables: target_tables,
        };
        let diff = DiffResult {
            added_tables: vec!["orders".to_string()],
            removed_tables: vec![],
            altered_tables: vec![],
            destructive_warnings: vec![],
        };
        let sql = generate_migration_sql(&empty_model(), &target, &diff, SqlDialect::Postgres);
        assert!(sql.contains("DO $$"), "Postgres FK should use idempotent DO block");
        assert!(sql.contains("information_schema.table_constraints"));
    }

    #[test]
    fn sqlite_dialect_fk_no_do_block() {
        let mut target_tables = BTreeMap::new();
        target_tables.insert(
            "orders".to_string(),
            Table {
                name: "orders".to_string(),
                columns: BTreeMap::new(),
                indexes: BTreeMap::new(),
                foreign_keys: [(
                    "fk_orders_user".to_string(),
                    ForeignKey {
                        name: "fk_orders_user".to_string(),
                        columns: vec!["user_id".to_string()],
                        referenced_table: "users".to_string(),
                        referenced_columns: vec!["id".to_string()],
                    },
                )]
                .into_iter()
                .collect(),
            },
        );
        let target = SchemaModel {
            tables: target_tables,
        };
        let diff = DiffResult {
            added_tables: vec!["orders".to_string()],
            removed_tables: vec![],
            altered_tables: vec![],
            destructive_warnings: vec![],
        };
        let sql = generate_migration_sql(&empty_model(), &target, &diff, SqlDialect::Sqlite);
        assert!(!sql.contains("DO $$"), "SQLite FK should not use DO block");
        assert!(sql.contains("recreation de table"));
        assert!(!sql.contains("ALTER TABLE"));
    }

    #[test]
    fn topological_sort_respects_fk_dependencies() {
        // orders depends on users via FK — users must be created first
        let mut tables = BTreeMap::new();
        tables.insert(
            "orders".to_string(),
            Table {
                name: "orders".to_string(),
                columns: BTreeMap::new(),
                indexes: BTreeMap::new(),
                foreign_keys: [(
                    "fk_orders_user".to_string(),
                    ForeignKey {
                        name: "fk_orders_user".to_string(),
                        columns: vec!["user_id".to_string()],
                        referenced_table: "users".to_string(),
                        referenced_columns: vec!["id".to_string()],
                    },
                )]
                .into_iter()
                .collect(),
            },
        );
        tables.insert(
            "users".to_string(),
            Table {
                name: "users".to_string(),
                columns: BTreeMap::new(),
                indexes: BTreeMap::new(),
                foreign_keys: BTreeMap::new(),
            },
        );
        let target = SchemaModel { tables };

        // BTreeSet iteration would give ["orders", "users"] (alphabetical)
        // topological sort must give ["users", "orders"]
        let names = vec!["orders".to_string(), "users".to_string()];
        let sorted = topological_sort_tables(&names, &target);
        let sorted_names: Vec<&str> = sorted.iter().map(|s| s.as_str()).collect();
        let users_pos = sorted_names.iter().position(|&s| s == "users").unwrap();
        let orders_pos = sorted_names.iter().position(|&s| s == "orders").unwrap();
        assert!(
            users_pos < orders_pos,
            "users (referenced) must come before orders (referencing)"
        );
    }

    #[test]
    fn order_added_tables_before_removed() {
        let source = SchemaModel {
            tables: [(
                "old_table".to_string(),
                Table {
                    name: "old_table".to_string(),
                    columns: BTreeMap::new(),
                    indexes: BTreeMap::new(),
                    foreign_keys: BTreeMap::new(),
                },
            )]
            .into_iter()
            .collect(),
        };
        let target = SchemaModel {
            tables: [(
                "new_table".to_string(),
                Table {
                    name: "new_table".to_string(),
                    columns: BTreeMap::new(),
                    indexes: BTreeMap::new(),
                    foreign_keys: BTreeMap::new(),
                },
            )]
            .into_iter()
            .collect(),
        };
        let diff = DiffResult {
            added_tables: vec!["new_table".to_string()],
            removed_tables: vec!["old_table".to_string()],
            altered_tables: vec![],
            destructive_warnings: vec![],
        };
        let sql = generate_migration_sql(&source, &target, &diff, SqlDialect::Postgres);
        let create_pos = sql.find("CREATE TABLE").unwrap_or(0);
        let drop_pos = sql.find("DROP TABLE").unwrap_or(sql.len());
        assert!(
            create_pos < drop_pos,
            "CREATE TABLE should appear before DROP TABLE"
        );
    }
}
