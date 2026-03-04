use std::collections::BTreeSet;

use crate::schema_model::{Column, ForeignKey, Index, SchemaModel};

#[derive(Debug, Clone)]
pub struct DiffResult {
    pub added_tables: Vec<String>,
    pub removed_tables: Vec<String>,
    pub altered_tables: Vec<TableDiff>,
    pub destructive_warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TableDiff {
    pub table_name: String,
    pub added_columns: Vec<Column>,
    pub removed_columns: Vec<Column>,
    pub modified_columns: Vec<ColumnModification>,
    pub added_indexes: Vec<Index>,
    pub removed_indexes: Vec<Index>,
    pub modified_indexes: Vec<IndexModification>,
    pub added_foreign_keys: Vec<ForeignKey>,
    pub removed_foreign_keys: Vec<ForeignKey>,
    pub modified_foreign_keys: Vec<ForeignKeyModification>,
}

#[derive(Debug, Clone)]
pub struct ColumnModification {
    pub old: Column,
    pub new: Column,
    pub destructive: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IndexModification {
    pub old: Index,
    pub new: Index,
}

#[derive(Debug, Clone)]
pub struct ForeignKeyModification {
    pub old: ForeignKey,
    pub new: ForeignKey,
}

impl DiffResult {
    pub fn has_changes(&self) -> bool {
        !(self.added_tables.is_empty()
            && self.removed_tables.is_empty()
            && self.altered_tables.is_empty())
    }
}

pub fn diff_schema(source: &SchemaModel, target: &SchemaModel) -> DiffResult {
    let source_tables: BTreeSet<String> = source.tables.keys().cloned().collect();
    let target_tables: BTreeSet<String> = target.tables.keys().cloned().collect();

    let added_tables = target_tables
        .difference(&source_tables)
        .cloned()
        .collect::<Vec<_>>();
    let removed_tables = source_tables
        .difference(&target_tables)
        .cloned()
        .collect::<Vec<_>>();

    let common_tables = source_tables
        .intersection(&target_tables)
        .cloned()
        .collect::<Vec<_>>();

    let mut altered_tables = Vec::new();
    let mut destructive_warnings = Vec::new();

    for table_name in common_tables {
        let Some(source_table) = source.tables.get(&table_name) else {
            continue;
        };
        let Some(target_table) = target.tables.get(&table_name) else {
            continue;
        };

        let source_cols: BTreeSet<String> = source_table.columns.keys().cloned().collect();
        let target_cols: BTreeSet<String> = target_table.columns.keys().cloned().collect();
        let source_indexes: BTreeSet<String> = source_table.indexes.keys().cloned().collect();
        let target_indexes: BTreeSet<String> = target_table.indexes.keys().cloned().collect();
        let source_fks: BTreeSet<String> = source_table.foreign_keys.keys().cloned().collect();
        let target_fks: BTreeSet<String> = target_table.foreign_keys.keys().cloned().collect();

        let mut added_columns = target_cols
            .difference(&source_cols)
            .filter_map(|name| target_table.columns.get(name).cloned())
            .collect::<Vec<_>>();
        let mut removed_columns = source_cols
            .difference(&target_cols)
            .filter_map(|name| source_table.columns.get(name).cloned())
            .collect::<Vec<_>>();

        let mut modified_columns = Vec::new();
        let mut added_indexes = target_indexes
            .difference(&source_indexes)
            .filter_map(|name| target_table.indexes.get(name).cloned())
            .collect::<Vec<_>>();
        let mut removed_indexes = source_indexes
            .difference(&target_indexes)
            .filter_map(|name| source_table.indexes.get(name).cloned())
            .collect::<Vec<_>>();
        let mut modified_indexes = Vec::new();

        let mut added_foreign_keys = target_fks
            .difference(&source_fks)
            .filter_map(|name| target_table.foreign_keys.get(name).cloned())
            .collect::<Vec<_>>();
        let mut removed_foreign_keys = source_fks
            .difference(&target_fks)
            .filter_map(|name| source_table.foreign_keys.get(name).cloned())
            .collect::<Vec<_>>();
        let mut modified_foreign_keys = Vec::new();
        for col_name in source_cols.intersection(&target_cols) {
            let old_col = source_table.columns.get(col_name).cloned();
            let new_col = target_table.columns.get(col_name).cloned();
            let (Some(old_col), Some(new_col)) = (old_col, new_col) else {
                continue;
            };

            if old_col != new_col {
                let (destructive, reason) = detect_destructive_change(&old_col, &new_col);
                if destructive {
                    destructive_warnings.push(format!(
                        "Colonne {}.{} modifiee de maniere destructive: {}",
                        table_name,
                        old_col.name,
                        reason
                            .as_deref()
                            .unwrap_or("changement potentiellement riske")
                    ));
                }
                modified_columns.push(ColumnModification {
                    old: old_col,
                    new: new_col,
                    destructive,
                    reason,
                });
            }
        }

        for index_name in source_indexes.intersection(&target_indexes) {
            let old_index = source_table.indexes.get(index_name).cloned();
            let new_index = target_table.indexes.get(index_name).cloned();
            let (Some(old_index), Some(new_index)) = (old_index, new_index) else {
                continue;
            };
            if old_index != new_index {
                modified_indexes.push(IndexModification {
                    old: old_index,
                    new: new_index,
                });
            }
        }

        for fk_name in source_fks.intersection(&target_fks) {
            let old_fk = source_table.foreign_keys.get(fk_name).cloned();
            let new_fk = target_table.foreign_keys.get(fk_name).cloned();
            let (Some(old_fk), Some(new_fk)) = (old_fk, new_fk) else {
                continue;
            };
            if old_fk != new_fk {
                modified_foreign_keys.push(ForeignKeyModification {
                    old: old_fk,
                    new: new_fk,
                });
            }
        }

        added_columns.sort_by(|a, b| a.name.cmp(&b.name));
        removed_columns.sort_by(|a, b| a.name.cmp(&b.name));
        modified_columns.sort_by(|a, b| a.old.name.cmp(&b.old.name));
        added_indexes.sort_by(|a, b| a.name.cmp(&b.name));
        removed_indexes.sort_by(|a, b| a.name.cmp(&b.name));
        modified_indexes.sort_by(|a, b| a.old.name.cmp(&b.old.name));
        added_foreign_keys.sort_by(|a, b| a.name.cmp(&b.name));
        removed_foreign_keys.sort_by(|a, b| a.name.cmp(&b.name));
        modified_foreign_keys.sort_by(|a, b| a.old.name.cmp(&b.old.name));

        if !added_columns.is_empty()
            || !removed_columns.is_empty()
            || !modified_columns.is_empty()
            || !added_indexes.is_empty()
            || !removed_indexes.is_empty()
            || !modified_indexes.is_empty()
            || !added_foreign_keys.is_empty()
            || !removed_foreign_keys.is_empty()
            || !modified_foreign_keys.is_empty()
        {
            for removed in &removed_columns {
                destructive_warnings.push(format!(
                    "Suppression de colonne detectee: {}.{}",
                    table_name, removed.name
                ));
            }
            for removed in &removed_foreign_keys {
                destructive_warnings.push(format!(
                    "Suppression de cle etrangere detectee: {}.{}",
                    table_name, removed.name
                ));
            }
            for modified in &modified_foreign_keys {
                destructive_warnings.push(format!(
                    "Modification de cle etrangere detectee: {}.{}",
                    table_name, modified.old.name
                ));
            }

            altered_tables.push(TableDiff {
                table_name,
                added_columns,
                removed_columns,
                modified_columns,
                added_indexes,
                removed_indexes,
                modified_indexes,
                added_foreign_keys,
                removed_foreign_keys,
                modified_foreign_keys,
            });
        }
    }

    for removed_table in &removed_tables {
        destructive_warnings.push(format!("Suppression de table detectee: {}", removed_table));
    }

    DiffResult {
        added_tables,
        removed_tables,
        altered_tables,
        destructive_warnings,
    }
}

fn detect_destructive_change(old: &Column, new: &Column) -> (bool, Option<String>) {
    if old.data_type != new.data_type {
        return is_destructive_type_change(&old.data_type, &new.data_type);
    }

    if !old.not_null && new.not_null && new.default_value.is_none() {
        return (
            true,
            Some("passage a NOT NULL sans valeur par defaut".to_owned()),
        );
    }

    (false, None)
}

/// Returns (destructive, reason). Safe widening (e.g. integer->bigint) is not destructive.
fn is_destructive_type_change(from_canonical: &str, to_canonical: &str) -> (bool, Option<String>) {
    if from_canonical == to_canonical {
        return (false, None);
    }
    let reason = format!("type {} -> {}", from_canonical, to_canonical);
    let destructive = match (from_canonical, to_canonical) {
        ("smallint", "integer") | ("smallint", "bigint") => false,
        ("integer", "bigint") => false,
        ("real", "double") => false,
        _ => true,
    };
    (destructive, Some(reason))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_model::{Column, SchemaModel, Table};
    use std::collections::BTreeMap;

    fn table_with_columns(cols: Vec<(&str, &str, bool)>) -> Table {
        let columns = cols
            .into_iter()
            .map(|(name, ty, not_null)| {
                (
                    name.to_string(),
                    Column {
                        name: name.to_string(),
                        data_type: ty.to_string(),
                        not_null,
                        default_value: None,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        Table {
            name: "t".to_string(),
            columns,
            indexes: BTreeMap::new(),
            foreign_keys: BTreeMap::new(),
        }
    }

    #[test]
    fn equivalent_canonical_types_not_destructive() {
        let source = SchemaModel {
            tables: [(
                "t".to_string(),
                table_with_columns(vec![
                    ("id", "integer", true),
                    ("amount", "numeric", true),
                ]),
            )]
            .into_iter()
            .collect(),
        };
        let target = SchemaModel {
            tables: [(
                "t".to_string(),
                table_with_columns(vec![
                    ("id", "integer", true),
                    ("amount", "numeric", true),
                ]),
            )]
            .into_iter()
            .collect(),
        };
        let diff = diff_schema(&source, &target);
        assert!(!diff.has_changes());
        assert!(diff.destructive_warnings.is_empty());
    }

    #[test]
    fn integer_to_bigint_not_destructive_warning() {
        let source = SchemaModel {
            tables: [(
                "t".to_string(),
                table_with_columns(vec![("age", "integer", false)]),
            )]
            .into_iter()
            .collect(),
        };
        let target = SchemaModel {
            tables: [(
                "t".to_string(),
                table_with_columns(vec![("age", "bigint", false)]),
            )]
            .into_iter()
            .collect(),
        };
        let diff = diff_schema(&source, &target);
        assert!(diff.has_changes());
        let destructive_for_age = diff.destructive_warnings.iter().any(|w| w.contains("age"));
        assert!(!destructive_for_age, "integer->bigint should not be destructive");
    }

    #[test]
    fn timestamp_to_text_destructive_warning() {
        let source = SchemaModel {
            tables: [(
                "t".to_string(),
                table_with_columns(vec![("created_at", "timestamp", false)]),
            )]
            .into_iter()
            .collect(),
        };
        let target = SchemaModel {
            tables: [(
                "t".to_string(),
                table_with_columns(vec![("created_at", "text", false)]),
            )]
            .into_iter()
            .collect(),
        };
        let diff = diff_schema(&source, &target);
        assert!(diff.has_changes());
        let has_destructive = diff
            .destructive_warnings
            .iter()
            .any(|w| w.contains("created_at") && w.contains("destructive"));
        assert!(has_destructive, "timestamp->text should be destructive");
    }
}
