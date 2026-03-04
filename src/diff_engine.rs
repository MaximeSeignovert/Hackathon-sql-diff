use std::collections::BTreeSet;

use crate::schema_model::{Column, Constraint, ForeignKey, Index, SchemaModel};

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
    /// Colonnes detectees comme renommees (meme type, meme not_null, nom different).
    pub renamed_columns: Vec<ColumnRename>,
    pub added_indexes: Vec<Index>,
    pub removed_indexes: Vec<Index>,
    pub modified_indexes: Vec<IndexModification>,
    pub added_foreign_keys: Vec<ForeignKey>,
    pub removed_foreign_keys: Vec<ForeignKey>,
    pub modified_foreign_keys: Vec<ForeignKeyModification>,
    pub added_constraints: Vec<(String, Constraint)>,
    pub removed_constraints: Vec<(String, Constraint)>,
    pub modified_constraints: Vec<ConstraintModification>,
}

#[derive(Debug, Clone)]
pub struct ColumnModification {
    pub old: Column,
    pub new: Column,
    pub destructive: bool,
    pub reason: Option<String>,
}

/// Renommage detecte par heuristique : colonne source absente de cible + colonne cible absente de source
/// avec meme type canonique et meme contrainte NOT NULL. Si le match est ambigu (plusieurs candidats
/// de meme type), aucun renommage n'est declare (les colonnes restent dans added/removed).
#[derive(Debug, Clone)]
pub struct ColumnRename {
    pub old_name: String,
    pub new_name: String,
    pub data_type: String,
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

#[derive(Debug, Clone)]
pub struct ConstraintModification {
    pub name: String,
    pub old: Constraint,
    pub new: Constraint,
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
        let source_constraints: BTreeSet<String> = source_table.constraints.keys().cloned().collect();
        let target_constraints: BTreeSet<String> = target_table.constraints.keys().cloned().collect();

        let raw_added_columns = target_cols
            .difference(&source_cols)
            .filter_map(|name| target_table.columns.get(name).cloned())
            .collect::<Vec<_>>();
        let raw_removed_columns = source_cols
            .difference(&target_cols)
            .filter_map(|name| source_table.columns.get(name).cloned())
            .collect::<Vec<_>>();

        // Heuristique de renommage: si une colonne supprimee et une colonne ajoutee ont exactement
        // le meme type canonique et le meme not_null, et qu'elles sont les seules candidates de ce type
        // dans l'ensemble removed/added, on considere qu'il s'agit d'un renommage.
        let (mut added_columns, mut removed_columns, mut renamed_columns) =
            detect_renames(raw_added_columns, raw_removed_columns);

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

        let mut added_constraints = target_constraints
            .difference(&source_constraints)
            .filter_map(|name| {
                target_table.constraints.get(name).cloned().map(|c| (name.clone(), c))
            })
            .collect::<Vec<_>>();
        let mut removed_constraints = source_constraints
            .difference(&target_constraints)
            .filter_map(|name| {
                source_table.constraints.get(name).cloned().map(|c| (name.clone(), c))
            })
            .collect::<Vec<_>>();
        let mut modified_constraints = Vec::new();

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

        for c_name in source_constraints.intersection(&target_constraints) {
            let old_c = source_table.constraints.get(c_name).cloned();
            let new_c = target_table.constraints.get(c_name).cloned();
            let (Some(old_c), Some(new_c)) = (old_c, new_c) else { continue };
            if old_c != new_c {
                modified_constraints.push(ConstraintModification {
                    name: c_name.clone(),
                    old: old_c,
                    new: new_c,
                });
            }
        }

        added_columns.sort_by(|a, b| a.name.cmp(&b.name));
        removed_columns.sort_by(|a, b| a.name.cmp(&b.name));
        renamed_columns.sort_by(|a, b| a.old_name.cmp(&b.old_name));
        modified_columns.sort_by(|a, b| a.old.name.cmp(&b.old.name));
        added_indexes.sort_by(|a, b| a.name.cmp(&b.name));
        removed_indexes.sort_by(|a, b| a.name.cmp(&b.name));
        modified_indexes.sort_by(|a, b| a.old.name.cmp(&b.old.name));
        added_foreign_keys.sort_by(|a, b| a.name.cmp(&b.name));
        removed_foreign_keys.sort_by(|a, b| a.name.cmp(&b.name));
        modified_foreign_keys.sort_by(|a, b| a.old.name.cmp(&b.old.name));
        added_constraints.sort_by(|a, b| a.0.cmp(&b.0));
        removed_constraints.sort_by(|a, b| a.0.cmp(&b.0));
        modified_constraints.sort_by(|a, b| a.name.cmp(&b.name));

        if !added_columns.is_empty()
            || !removed_columns.is_empty()
            || !renamed_columns.is_empty()
            || !modified_columns.is_empty()
            || !added_indexes.is_empty()
            || !removed_indexes.is_empty()
            || !modified_indexes.is_empty()
            || !added_foreign_keys.is_empty()
            || !removed_foreign_keys.is_empty()
            || !modified_foreign_keys.is_empty()
            || !added_constraints.is_empty()
            || !removed_constraints.is_empty()
            || !modified_constraints.is_empty()
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
            for removed in &removed_constraints {
                destructive_warnings.push(format!(
                    "Suppression de contrainte detectee: {}.{}",
                    table_name, removed.0
                ));
            }

            altered_tables.push(TableDiff {
                table_name,
                added_columns,
                removed_columns,
                modified_columns,
                renamed_columns,
                added_indexes,
                removed_indexes,
                modified_indexes,
                added_foreign_keys,
                removed_foreign_keys,
                modified_foreign_keys,
                added_constraints,
                removed_constraints,
                modified_constraints,
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

/// Detecte les renommages de colonnes par heuristique.
/// Regle: si une seule colonne supprimee et une seule colonne ajoutee ont exactement
/// le meme type canonique et le meme not_null, elles forment une paire de renommage.
/// Si plusieurs candidats de meme signature existent (ambigu), on ne declare aucun renommage.
/// Retourne (added_restants, removed_restants, renames_detectes).
fn detect_renames(
    added: Vec<Column>,
    removed: Vec<Column>,
) -> (Vec<Column>, Vec<Column>, Vec<ColumnRename>) {
    let mut renames = Vec::new();
    let mut used_added: Vec<bool> = vec![false; added.len()];
    let mut used_removed: Vec<bool> = vec![false; removed.len()];

    for (ri, rem) in removed.iter().enumerate() {
        // Chercher les colonnes ajoutees compatibles (meme type, meme not_null)
        let candidates: Vec<usize> = added
            .iter()
            .enumerate()
            .filter(|(ai, a)| {
                !used_added[*ai]
                    && a.data_type == rem.data_type
                    && a.not_null == rem.not_null
            })
            .map(|(ai, _)| ai)
            .collect();

        if candidates.len() == 1 {
            let ai = candidates[0];
            // Verifier unicite du cote removed aussi
            let other_removed_count = removed
                .iter()
                .enumerate()
                .filter(|(other_ri, r)| {
                    *other_ri != ri
                        && !used_removed[*other_ri]
                        && r.data_type == rem.data_type
                        && r.not_null == rem.not_null
                })
                .count();
            if other_removed_count == 0 {
                used_added[ai] = true;
                used_removed[ri] = true;
                renames.push(ColumnRename {
                    old_name: rem.name.clone(),
                    new_name: added[ai].name.clone(),
                    data_type: rem.data_type.clone(),
                });
            }
        }
    }

    let remaining_added = added
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !used_added[*i])
        .map(|(_, c)| c)
        .collect();
    let remaining_removed = removed
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !used_removed[*i])
        .map(|(_, c)| c)
        .collect();

    (remaining_added, remaining_removed, renames)
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
            constraints: BTreeMap::new(),
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

    #[test]
    fn column_rename_detected_by_heuristic() {
        // Meme type + meme not_null => renommage detecte
        let source = SchemaModel {
            tables: [(
                "t".to_string(),
                table_with_columns(vec![("full_name", "text", false)]),
            )]
            .into_iter()
            .collect(),
        };
        let target = SchemaModel {
            tables: [(
                "t".to_string(),
                table_with_columns(vec![("display_name", "text", false)]),
            )]
            .into_iter()
            .collect(),
        };
        let diff = diff_schema(&source, &target);
        assert!(diff.has_changes());
        let td = &diff.altered_tables[0];
        assert_eq!(td.renamed_columns.len(), 1, "Should detect 1 rename");
        assert_eq!(td.renamed_columns[0].old_name, "full_name");
        assert_eq!(td.renamed_columns[0].new_name, "display_name");
        assert!(td.added_columns.is_empty(), "Renamed col should not appear as added");
        assert!(td.removed_columns.is_empty(), "Renamed col should not appear as removed");
    }

    #[test]
    fn ambiguous_renames_not_detected() {
        // Deux colonnes de meme type supprimees/ajoutees => ambigu, pas de renommage
        let source = SchemaModel {
            tables: [(
                "t".to_string(),
                table_with_columns(vec![("col_a", "integer", false), ("col_b", "integer", false)]),
            )]
            .into_iter()
            .collect(),
        };
        let target = SchemaModel {
            tables: [(
                "t".to_string(),
                table_with_columns(vec![("col_x", "integer", false), ("col_y", "integer", false)]),
            )]
            .into_iter()
            .collect(),
        };
        let diff = diff_schema(&source, &target);
        assert!(diff.has_changes());
        let td = &diff.altered_tables[0];
        assert_eq!(td.renamed_columns.len(), 0, "Ambiguous case should not produce renames");
        assert_eq!(td.added_columns.len(), 2);
        assert_eq!(td.removed_columns.len(), 2);
    }

    #[test]
    fn no_changes_produces_empty_diff() {
        let source = SchemaModel {
            tables: [("t".to_string(), table_with_columns(vec![("id", "integer", true)]))]
                .into_iter()
                .collect(),
        };
        let diff = diff_schema(&source, &source);
        assert!(!diff.has_changes());
        assert!(diff.destructive_warnings.is_empty());
        assert!(diff.altered_tables.is_empty());
    }

    #[test]
    fn table_drop_is_destructive() {
        let source = SchemaModel {
            tables: [("old_table".to_string(), table_with_columns(vec![("id", "integer", true)]))]
                .into_iter()
                .collect(),
        };
        let target = SchemaModel {
            tables: std::collections::BTreeMap::new(),
        };
        let diff = diff_schema(&source, &target);
        assert!(diff.has_changes());
        assert!(!diff.removed_tables.is_empty());
        let has_drop_warning = diff.destructive_warnings.iter().any(|w| w.contains("old_table"));
        assert!(has_drop_warning, "Table drop should emit a destructive warning");
    }

    #[test]
    fn column_drop_is_destructive() {
        let source = SchemaModel {
            tables: [(
                "t".to_string(),
                table_with_columns(vec![("id", "integer", true), ("to_drop", "text", false)]),
            )]
            .into_iter()
            .collect(),
        };
        let target = SchemaModel {
            tables: [(
                "t".to_string(),
                table_with_columns(vec![("id", "integer", true)]),
            )]
            .into_iter()
            .collect(),
        };
        let diff = diff_schema(&source, &target);
        assert!(diff.has_changes());
        let has_warning = diff.destructive_warnings.iter().any(|w| w.contains("to_drop"));
        assert!(has_warning, "Column drop should emit a destructive warning");
    }

    #[test]
    fn unique_constraint_diff_detected() {
        use crate::schema_model::Constraint;
        let mut source_table = table_with_columns(vec![("email", "text", true)]);
        source_table.constraints.insert(
            "uq_email".to_owned(),
            Constraint::Unique { columns: vec!["email".to_owned()] },
        );
        let target_table = table_with_columns(vec![("email", "text", true)]);
        // target has no constraint => removed

        let source = SchemaModel {
            tables: [("t".to_string(), source_table)].into_iter().collect(),
        };
        let target = SchemaModel {
            tables: [("t".to_string(), target_table)].into_iter().collect(),
        };
        let diff = diff_schema(&source, &target);
        assert!(diff.has_changes());
        let td = &diff.altered_tables[0];
        assert_eq!(td.removed_constraints.len(), 1);
        assert_eq!(td.added_constraints.len(), 0);
        let has_warning = diff.destructive_warnings.iter().any(|w| w.contains("uq_email"));
        assert!(has_warning, "Removed constraint should produce a destructive warning");
    }
}
