use crate::connectors::ConnectorKind;
use crate::diff_engine::{ColumnModification, DiffResult, TableDiff};
use crate::schema_model::{Constraint, ForeignKey, Index, SchemaModel, Table};

/// Target SQL dialect for generated migration script (idempotence and syntax adapted per dialect).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
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

    // 2) Apply column/index/constraint changes on existing tables.
    for table_diff in &diff.altered_tables {
        let needs_sqlite_rebuild = dialect == SqlDialect::Sqlite
            && sqlite_table_needs_rebuild(table_diff);

        if needs_sqlite_rebuild {
            let source_table = source.tables.get(&table_diff.table_name);
            let target_table = target.tables.get(&table_diff.table_name);
            if let (Some(src), Some(tgt)) = (source_table, target_table) {
                out.push(generate_sqlite_table_rebuild(src, tgt));
            }
            // Indexes are recreated as part of rebuild
            for idx in &table_diff.added_indexes {
                out.push(create_index_statement(&table_diff.table_name, idx, dialect));
            }
            continue;
        }

        // Renames: RENAME COLUMN (Postgres 10+; comment for SQLite)
        for rename in &table_diff.renamed_columns {
            match dialect {
                SqlDialect::Postgres => {
                    out.push(format!(
                        "ALTER TABLE {table} RENAME COLUMN {old} TO {new};",
                        table = ident(&table_diff.table_name),
                        old = ident(&rename.old_name),
                        new = ident(&rename.new_name),
                    ));
                }
                SqlDialect::Sqlite => {
                    out.push(format!(
                        "-- SQLite: renommage de colonne {table}.{old} -> {new} (ALTER TABLE ... RENAME COLUMN supporte depuis SQLite 3.25.0).",
                        table = table_diff.table_name,
                        old = rename.old_name,
                        new = rename.new_name,
                    ));
                    out.push(format!(
                        "ALTER TABLE {table} RENAME COLUMN {old} TO {new};",
                        table = ident(&table_diff.table_name),
                        old = ident(&rename.old_name),
                        new = ident(&rename.new_name),
                    ));
                }
            }
        }

        for col in &table_diff.added_columns {
            match dialect {
                SqlDialect::Postgres => {
                    out.push(format!(
                        "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {name} {ty}{not_null}{default};",
                        table = ident(&table_diff.table_name),
                        name = ident(&col.name),
                        ty = col.data_type,
                        not_null = if col.not_null { " NOT NULL" } else { "" },
                        default = format_default(&col.default_value),
                    ));
                }
                SqlDialect::Sqlite => {
                    // SQLite ADD COLUMN does not support IF NOT EXISTS before 3.37
                    out.push(format!(
                        "ALTER TABLE {table} ADD COLUMN {name} {ty}{default};",
                        table = ident(&table_diff.table_name),
                        name = ident(&col.name),
                        ty = col.data_type,
                        default = format_default(&col.default_value),
                    ));
                }
            }
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
            match dialect {
                SqlDialect::Postgres => out.push(format!(
                    "ALTER TABLE {table} DROP COLUMN IF EXISTS {name};",
                    table = ident(&table_diff.table_name),
                    name = ident(&col.name),
                )),
                SqlDialect::Sqlite => out.push(format!(
                    "ALTER TABLE {table} DROP COLUMN {name};",
                    table = ident(&table_diff.table_name),
                    name = ident(&col.name),
                )),
            }
        }

        // Constraints
        for (c_name, _constraint) in &table_diff.removed_constraints {
            out.push(format!(
                "-- DESTRUCTIVE: suppression de contrainte {}.{}",
                table_diff.table_name, c_name
            ));
            out.extend(drop_constraint_statements(&table_diff.table_name, c_name, dialect));
        }
        for (c_name, constraint) in &table_diff.added_constraints {
            out.extend(add_constraint_statements(&table_diff.table_name, c_name, constraint, dialect));
        }
        for c_mod in &table_diff.modified_constraints {
            out.extend(drop_constraint_statements(&table_diff.table_name, &c_mod.name, dialect));
            out.extend(add_constraint_statements(&table_diff.table_name, &c_mod.name, &c_mod.new, dialect));
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
            for (c_name, constraint) in &table.constraints {
                out.extend(add_constraint_statements(table_name, c_name, constraint, dialect));
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

/// Genere le script de rollback (migration inverse : target -> source).
pub fn generate_rollback_sql(
    source: &SchemaModel,
    target: &SchemaModel,
    diff: &DiffResult,
    dialect: SqlDialect,
) -> String {
    let mut out = Vec::new();

    out.push("-- Script de ROLLBACK (migration inverse)".to_owned());
    out.push(format!("-- Cible: {:?}", dialect));
    out.push("-- Target -> Source (annule la migration)".to_owned());
    out.push(String::new());

    // 1) Re-creer les tables supprimees par la migration
    for table_name in &diff.removed_tables {
        if let Some(table) = source.tables.get(table_name) {
            out.push(create_table_statement(table, dialect));
        }
    }

    // 2) Inverser les modifications de tables
    for table_diff in &diff.altered_tables {
        // Inverser les renommages
        for rename in &table_diff.renamed_columns {
            match dialect {
                SqlDialect::Postgres => {
                    out.push(format!(
                        "ALTER TABLE {table} RENAME COLUMN {new} TO {old};",
                        table = ident(&table_diff.table_name),
                        new = ident(&rename.new_name),
                        old = ident(&rename.old_name),
                    ));
                }
                SqlDialect::Sqlite => {
                    out.push(format!(
                        "ALTER TABLE {table} RENAME COLUMN {new} TO {old};",
                        table = ident(&table_diff.table_name),
                        new = ident(&rename.new_name),
                        old = ident(&rename.old_name),
                    ));
                }
            }
        }

        // Supprimer les colonnes ajoutees par la migration
        for col in &table_diff.added_columns {
            out.push(format!(
                "-- ROLLBACK: suppression de colonne {}.{} ajoutee par la migration",
                table_diff.table_name, col.name
            ));
            match dialect {
                SqlDialect::Postgres => out.push(format!(
                    "ALTER TABLE {table} DROP COLUMN IF EXISTS {name};",
                    table = ident(&table_diff.table_name),
                    name = ident(&col.name),
                )),
                SqlDialect::Sqlite => out.push(format!(
                    "ALTER TABLE {table} DROP COLUMN {name};",
                    table = ident(&table_diff.table_name),
                    name = ident(&col.name),
                )),
            }
        }

        // Re-ajouter les colonnes supprimees par la migration
        for col in &table_diff.removed_columns {
            out.push(format!(
                "ALTER TABLE {table} ADD COLUMN {name} {ty}{default};",
                table = ident(&table_diff.table_name),
                name = ident(&col.name),
                ty = col.data_type,
                default = format_default(&col.default_value),
            ));
        }

        // Inverser les modifications de colonnes
        for modified in &table_diff.modified_columns {
            let inverse = ColumnModification {
                old: modified.new.clone(),
                new: modified.old.clone(),
                destructive: false,
                reason: None,
            };
            out.extend(generate_modified_column_sql(&table_diff.table_name, &inverse, dialect));
        }

        // Inverser les indexes
        for idx in &table_diff.added_indexes {
            out.push(drop_index_statement(&idx.name, dialect));
        }
        for idx in &table_diff.removed_indexes {
            out.push(create_index_statement(&table_diff.table_name, idx, dialect));
        }
        for idx in &table_diff.modified_indexes {
            out.push(drop_index_statement(&idx.new.name, dialect));
            out.push(create_index_statement(&table_diff.table_name, &idx.old, dialect));
        }

        // Inverser les FK
        for fk in &table_diff.added_foreign_keys {
            out.extend(drop_fk_statements(&table_diff.table_name, &fk.name, dialect));
        }
        for fk in &table_diff.removed_foreign_keys {
            out.push(add_fk_statement(&table_diff.table_name, fk, dialect));
        }
        for fk in &table_diff.modified_foreign_keys {
            out.extend(drop_fk_statements(&table_diff.table_name, &fk.new.name, dialect));
            out.push(add_fk_statement(&table_diff.table_name, &fk.old, dialect));
        }

        // Inverser les contraintes
        for (c_name, _constraint) in &table_diff.added_constraints {
            out.extend(drop_constraint_statements(&table_diff.table_name, c_name, dialect));
        }
        for (c_name, constraint) in &table_diff.removed_constraints {
            out.extend(add_constraint_statements(&table_diff.table_name, c_name, constraint, dialect));
        }
    }

    // 3) Indexes des tables re-creees
    for table_name in &diff.removed_tables {
        if let Some(table) = source.tables.get(table_name) {
            for index in table.indexes.values() {
                out.push(create_index_statement(table_name, index, dialect));
            }
        }
    }

    // 4) FK des tables re-creees
    for table_name in &diff.removed_tables {
        if let Some(table) = source.tables.get(table_name) {
            for fk in table.foreign_keys.values() {
                out.push(add_fk_statement(table_name, fk, dialect));
            }
        }
    }

    // 5) Supprimer les tables ajoutees par la migration
    for table_name in &diff.added_tables {
        out.push(format!(
            "-- ROLLBACK: suppression de table {} ajoutee par la migration",
            table_name
        ));
        out.push(format!("DROP TABLE IF EXISTS {};", ident(table_name)));
    }

    if diff.added_tables.is_empty()
        && diff.removed_tables.is_empty()
        && diff.altered_tables.is_empty()
    {
        out.push("-- Aucun changement a annuler".to_owned());
    }

    let _ = target;
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

/// Verifie si un TableDiff sur SQLite necessite une recreation de table
/// (changement de type, NOT NULL, DEFAULT, ou suppression de colonne non supportes en ALTER).
fn sqlite_table_needs_rebuild(table_diff: &TableDiff) -> bool {
    !table_diff.modified_columns.is_empty()
}

/// Genere la sequence SQLite de recreation de table (pattern officiel SQLite).
/// Preserve toutes les colonnes communes, applique la nouvelle definition.
fn generate_sqlite_table_rebuild(source_table: &Table, target_table: &Table) -> String {
    let tmp = format!("_migration_tmp_{}", source_table.name);
    let target_cols: Vec<String> = target_table.columns.keys().cloned().collect();
    let common_cols: Vec<String> = target_cols
        .iter()
        .filter(|c| source_table.columns.contains_key(*c))
        .cloned()
        .collect();

    let col_list = common_cols
        .iter()
        .map(|c| ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    let target_col_defs = target_table
        .columns
        .values()
        .map(|col| {
            format!(
                "  {} {}{}{}",
                ident(&col.name),
                col.data_type,
                if col.not_null { " NOT NULL" } else { "" },
                format_default(&col.default_value)
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");

    format!(
        "-- SQLite: recreation de table {name} pour appliquer les modifications de colonnes\nBEGIN TRANSACTION;\nCREATE TABLE {tmp} AS SELECT {col_list} FROM {table};\nDROP TABLE {table};\nCREATE TABLE {table} (\n{col_defs}\n);\nINSERT INTO {table} ({col_list}) SELECT {col_list} FROM {tmp};\nDROP TABLE {tmp};\nCOMMIT;",
        name = source_table.name,
        tmp = ident(&tmp),
        table = ident(&source_table.name),
        col_list = col_list,
        col_defs = target_col_defs,
    )
}

fn drop_constraint_statements(table_name: &str, constraint_name: &str, dialect: SqlDialect) -> Vec<String> {
    match dialect {
        SqlDialect::Postgres => vec![format!(
            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {};",
            ident(table_name),
            ident(constraint_name)
        )],
        SqlDialect::Sqlite => vec![format!(
            "-- SQLite: DROP CONSTRAINT non supporte directement. Recreer la table pour supprimer {}.",
            constraint_name
        )],
    }
}

fn add_constraint_statements(
    table_name: &str,
    constraint_name: &str,
    constraint: &Constraint,
    dialect: SqlDialect,
) -> Vec<String> {
    match constraint {
        Constraint::Unique { columns } => {
            let cols = columns.iter().map(|c| ident(c)).collect::<Vec<_>>().join(", ");
            match dialect {
                SqlDialect::Postgres => vec![format!(
                    "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE table_schema = current_schema() AND table_name = '{}' AND constraint_name = '{}' AND constraint_type = 'UNIQUE') THEN ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({}); END IF; END $$;",
                    escape_literal(table_name),
                    escape_literal(constraint_name),
                    ident(table_name),
                    ident(constraint_name),
                    cols
                )],
                SqlDialect::Sqlite => vec![format!(
                    "-- SQLite: ajout contrainte UNIQUE {}.{} necessite recreation de table.",
                    table_name, constraint_name
                )],
            }
        }
        Constraint::Check { expression } => {
            match dialect {
                SqlDialect::Postgres => vec![format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} CHECK ({});",
                    ident(table_name),
                    ident(constraint_name),
                    expression
                )],
                SqlDialect::Sqlite => vec![format!(
                    "-- SQLite: ajout contrainte CHECK {}.{} necessite recreation de table.",
                    table_name, constraint_name
                )],
            }
        }
    }
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
                constraints: BTreeMap::new(),
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
                constraints: BTreeMap::new(),
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
                    constraints: BTreeMap::new(),
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
                    constraints: BTreeMap::new(),
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

    fn make_table(name: &str, cols: Vec<(&str, &str, bool)>) -> Table {
        use crate::schema_model::Column;
        let columns = cols
            .into_iter()
            .map(|(n, ty, nn)| {
                (
                    n.to_owned(),
                    Column {
                        name: n.to_owned(),
                        data_type: ty.to_owned(),
                        not_null: nn,
                        default_value: None,
                    },
                )
            })
            .collect();
        Table {
            name: name.to_owned(),
            columns,
            indexes: BTreeMap::new(),
            foreign_keys: BTreeMap::new(),
            constraints: BTreeMap::new(),
        }
    }

    #[test]
    fn rename_column_postgres_uses_rename_syntax() {
        let source = SchemaModel {
            tables: [("t".to_owned(), make_table("t", vec![("old_col", "text", false)]))]
                .into_iter()
                .collect(),
        };
        let target = SchemaModel {
            tables: [("t".to_owned(), make_table("t", vec![("new_col", "text", false)]))]
                .into_iter()
                .collect(),
        };
        let diff = crate::diff_engine::diff_schema(&source, &target);
        let sql = generate_migration_sql(&source, &target, &diff, SqlDialect::Postgres);
        assert!(sql.contains("RENAME COLUMN"), "Postgres should use RENAME COLUMN syntax");
        assert!(sql.contains("\"old_col\""));
        assert!(sql.contains("\"new_col\""));
    }

    #[test]
    fn sqlite_type_change_triggers_rebuild() {
        use crate::diff_engine::diff_schema;
        let src = make_table("orders", vec![("id", "integer", true), ("amount", "text", false)]);
        let tgt = make_table("orders", vec![("id", "integer", true), ("amount", "numeric", false)]);
        let source = SchemaModel {
            tables: [("orders".to_owned(), src)].into_iter().collect(),
        };
        let target = SchemaModel {
            tables: [("orders".to_owned(), tgt)].into_iter().collect(),
        };
        let diff = diff_schema(&source, &target);
        let sql = generate_migration_sql(&source, &target, &diff, SqlDialect::Sqlite);
        assert!(sql.contains("BEGIN TRANSACTION"), "SQLite type change should trigger table rebuild");
        assert!(sql.contains("COMMIT"), "SQLite rebuild should be wrapped in transaction");
        assert!(sql.contains("_migration_tmp_"), "SQLite rebuild should use temp table");
    }

    #[test]
    fn rollback_inverts_migration() {
        use crate::diff_engine::diff_schema;
        let src = SchemaModel { tables: BTreeMap::new() };
        let tgt = SchemaModel {
            tables: [("new_table".to_owned(), make_table("new_table", vec![("id", "integer", true)]))]
                .into_iter()
                .collect(),
        };
        let diff = diff_schema(&src, &tgt);
        let rollback = generate_rollback_sql(&src, &tgt, &diff, SqlDialect::Postgres);
        assert!(rollback.contains("DROP TABLE"), "Rollback should drop newly added tables");
        assert!(rollback.contains("ROLLBACK"), "Rollback header should mention ROLLBACK");
    }

    #[test]
    fn idempotent_create_table_uses_if_not_exists() {
        let target = SchemaModel {
            tables: [("users".to_owned(), make_table("users", vec![("id", "integer", true)]))]
                .into_iter()
                .collect(),
        };
        let diff = DiffResult {
            added_tables: vec!["users".to_owned()],
            removed_tables: vec![],
            altered_tables: vec![],
            destructive_warnings: vec![],
        };
        let sql = generate_migration_sql(&empty_model(), &target, &diff, SqlDialect::Postgres);
        assert!(sql.contains("IF NOT EXISTS"), "CREATE TABLE should use IF NOT EXISTS for idempotence");
    }

    #[test]
    fn integration_sql_to_sql_diff_and_migration() {
        // Test d'integration: 2 dumps SQL -> diff -> migration coherente
        use crate::sql_dump_parser::parse_schema_from_sql;
        let source_sql = r#"
            CREATE TABLE users (
              id integer NOT NULL,
              email text NOT NULL,
              age integer
            );
            CREATE UNIQUE INDEX idx_users_email ON users (email);
        "#;
        let target_sql = r#"
            CREATE TABLE users (
              id integer NOT NULL,
              email text NOT NULL,
              age bigint,
              full_name text DEFAULT 'anonymous'
            );
            CREATE UNIQUE INDEX idx_users_email ON users (email);
            CREATE INDEX idx_users_age ON users (age);

            CREATE TABLE orders (
              id integer NOT NULL,
              user_id integer NOT NULL,
              amount numeric NOT NULL
            );
        "#;
        let source = parse_schema_from_sql(source_sql).unwrap();
        let target = parse_schema_from_sql(target_sql).unwrap();
        let diff = crate::diff_engine::diff_schema(&source, &target);
        assert!(diff.has_changes());
        // orders is a new table
        assert!(diff.added_tables.contains(&"orders".to_owned()));
        // users was altered
        let users_diff = diff.altered_tables.iter().find(|td| td.table_name == "users");
        assert!(users_diff.is_some(), "users should be in altered tables");
        let ud = users_diff.unwrap();
        // full_name added
        assert!(ud.added_columns.iter().any(|c| c.name == "full_name"), "full_name should be added");
        // age widened (integer -> bigint): should be a modification
        assert!(ud.modified_columns.iter().any(|m| m.old.name == "age"), "age should be modified");
        // no destructive warnings for integer -> bigint
        assert!(!diff.destructive_warnings.iter().any(|w| w.contains("age")));

        // Generate migration SQL
        let sql = generate_migration_sql(&source, &target, &diff, SqlDialect::Postgres);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS"), "Should create orders table");
        assert!(sql.contains("idx_users_age"), "Should create new index");
        assert!(sql.contains("\"full_name\""), "Should add full_name column");
    }
}
