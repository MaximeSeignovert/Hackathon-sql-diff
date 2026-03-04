use crate::diff_engine::DiffResult;
use crate::schema_model::Constraint;

pub fn render_diff_markdown(diff: &DiffResult) -> String {
    let mut out = Vec::new();

    out.push("# Rapport de differences de schema".to_owned());
    out.push(String::new());

    out.push("## Tables ajoutees".to_owned());
    if diff.added_tables.is_empty() {
        out.push("- Aucune".to_owned());
    } else {
        for table in &diff.added_tables {
            out.push(format!("- `{}`", table));
        }
    }
    out.push(String::new());

    out.push("## Tables supprimees".to_owned());
    if diff.removed_tables.is_empty() {
        out.push("- Aucune".to_owned());
    } else {
        for table in &diff.removed_tables {
            out.push(format!("- `{}` ⚠ DESTRUCTIF", table));
        }
    }
    out.push(String::new());

    out.push("## Modifications de tables".to_owned());
    if diff.altered_tables.is_empty() {
        out.push("- Aucune".to_owned());
    } else {
        for table_diff in &diff.altered_tables {
            out.push(format!("### Table `{}`", table_diff.table_name));
            for rename in &table_diff.renamed_columns {
                out.push(format!(
                    "  - ↩ Colonne `{}` renommee en `{}` ({}) *(heuristique)*",
                    rename.old_name, rename.new_name, rename.data_type
                ));
            }
            for col in &table_diff.added_columns {
                out.push(format!("  - + Colonne `{}` ({})", col.name, col.data_type));
            }
            for col in &table_diff.removed_columns {
                out.push(format!("  - - Colonne `{}` ({}) ⚠ DESTRUCTIF", col.name, col.data_type));
            }
            for change in &table_diff.modified_columns {
                let destructive_mark = if change.destructive { " ⚠ DESTRUCTIF" } else { "" };
                out.push(format!(
                    "  - ~ Colonne `{}`: type `{}` -> `{}`, not_null {} -> {}{}",
                    change.old.name,
                    change.old.data_type,
                    change.new.data_type,
                    change.old.not_null,
                    change.new.not_null,
                    destructive_mark
                ));
            }
            for idx in &table_diff.added_indexes {
                out.push(format!(
                    "  - + Index `{}` [{}]{}",
                    idx.name,
                    idx.columns.join(", "),
                    if idx.unique { " UNIQUE" } else { "" }
                ));
            }
            for idx in &table_diff.removed_indexes {
                out.push(format!(
                    "  - - Index `{}` [{}]",
                    idx.name,
                    idx.columns.join(", ")
                ));
            }
            for idx in &table_diff.modified_indexes {
                out.push(format!(
                    "  - ~ Index `{}`: [{}] -> [{}]",
                    idx.old.name,
                    idx.old.columns.join(", "),
                    idx.new.columns.join(", ")
                ));
            }
            for fk in &table_diff.added_foreign_keys {
                out.push(format!(
                    "  - + FK `{}`: ({}) -> `{}`({})",
                    fk.name,
                    fk.columns.join(", "),
                    fk.referenced_table,
                    fk.referenced_columns.join(", ")
                ));
            }
            for fk in &table_diff.removed_foreign_keys {
                out.push(format!(
                    "  - - FK `{}`: ({}) -> `{}`({}) ⚠ DESTRUCTIF",
                    fk.name,
                    fk.columns.join(", "),
                    fk.referenced_table,
                    fk.referenced_columns.join(", ")
                ));
            }
            for fk in &table_diff.modified_foreign_keys {
                out.push(format!("  - ~ FK `{}` modifiee ⚠", fk.old.name));
            }
            for (name, constraint) in &table_diff.added_constraints {
                out.push(format!("  - + Contrainte `{}`: {}", name, format_constraint_md(constraint)));
            }
            for (name, constraint) in &table_diff.removed_constraints {
                out.push(format!("  - - Contrainte `{}`: {} ⚠ DESTRUCTIF", name, format_constraint_md(constraint)));
            }
            for c_mod in &table_diff.modified_constraints {
                out.push(format!(
                    "  - ~ Contrainte `{}`: {} -> {}",
                    c_mod.name,
                    format_constraint_md(&c_mod.old),
                    format_constraint_md(&c_mod.new)
                ));
            }
        }
    }
    out.push(String::new());

    out.push("## Alertes destructives".to_owned());
    if diff.destructive_warnings.is_empty() {
        out.push("- Aucune".to_owned());
    } else {
        for warning in &diff.destructive_warnings {
            out.push(format!("- ⚠ {}", warning));
        }
    }

    out.join("\n")
}

fn format_constraint_md(c: &Constraint) -> String {
    match c {
        Constraint::Unique { columns } => format!("UNIQUE ({})", columns.join(", ")),
        Constraint::Check { expression } => format!("CHECK ({})", expression),
    }
}

pub fn render_diff_html(diff: &DiffResult) -> String {
    let mut table_cards = String::new();
    for table_diff in &diff.altered_tables {
        let mut lines = String::new();

        for rename in &table_diff.renamed_columns {
            lines.push_str(&format!(
                r#"<li class="py-1"><span class="inline-block min-w-6 rounded bg-violet-100 px-2 py-0.5 text-xs font-semibold text-violet-800">RENAME</span> colonne <code class="rounded bg-slate-100 px-1.5 py-0.5 text-slate-800">{}</code> <span class="text-slate-500">-&gt;</span> <code class="rounded bg-slate-100 px-1.5 py-0.5 text-slate-800">{}</code> <span class="text-xs text-slate-400">(heuristique)</span></li>"#,
                escape_html(&rename.old_name),
                escape_html(&rename.new_name)
            ));
        }
        for col in &table_diff.added_columns {
            lines.push_str(&format!(
                r#"<li class="py-1"><span class="inline-block min-w-6 rounded bg-emerald-100 px-2 py-0.5 text-xs font-semibold text-emerald-800">ADD</span> colonne <code class="rounded bg-slate-100 px-1.5 py-0.5 text-slate-800">{}</code> <span class="text-slate-500">({})</span></li>"#,
                escape_html(&col.name),
                escape_html(&col.data_type)
            ));
        }
        for col in &table_diff.removed_columns {
            lines.push_str(&format!(
                r#"<li class="py-1"><span class="inline-block min-w-6 rounded bg-rose-100 px-2 py-0.5 text-xs font-semibold text-rose-800">DROP</span> colonne <code class="rounded bg-slate-100 px-1.5 py-0.5 text-slate-800">{}</code> <span class="text-slate-500">({})</span> <span class="text-xs font-semibold text-rose-700">DESTRUCTIF</span></li>"#,
                escape_html(&col.name),
                escape_html(&col.data_type)
            ));
        }
        for change in &table_diff.modified_columns {
            let destructive_badge = if change.destructive {
                r#" <span class="text-xs font-semibold text-rose-700">DESTRUCTIF</span>"#
            } else {
                ""
            };
            lines.push_str(&format!(
                r#"<li class="py-1"><span class="inline-block min-w-6 rounded bg-amber-100 px-2 py-0.5 text-xs font-semibold text-amber-800">ALTER</span> colonne <code class="rounded bg-slate-100 px-1.5 py-0.5 text-slate-800">{}</code> <span class="text-slate-500">type {} -&gt; {} | not_null {} -&gt; {}</span>{}</li>"#,
                escape_html(&change.old.name),
                escape_html(&change.old.data_type),
                escape_html(&change.new.data_type),
                change.old.not_null,
                change.new.not_null,
                destructive_badge
            ));
        }
        for idx in &table_diff.added_indexes {
            lines.push_str(&format!(
                r#"<li class="py-1"><span class="inline-block min-w-6 rounded bg-emerald-100 px-2 py-0.5 text-xs font-semibold text-emerald-800">ADD</span> index <code class="rounded bg-slate-100 px-1.5 py-0.5 text-slate-800">{}</code> <span class="text-slate-500">[{}]</span></li>"#,
                escape_html(&idx.name),
                escape_html(&idx.columns.join(", "))
            ));
        }
        for idx in &table_diff.removed_indexes {
            lines.push_str(&format!(
                r#"<li class="py-1"><span class="inline-block min-w-6 rounded bg-rose-100 px-2 py-0.5 text-xs font-semibold text-rose-800">DROP</span> index <code class="rounded bg-slate-100 px-1.5 py-0.5 text-slate-800">{}</code> <span class="text-slate-500">[{}]</span></li>"#,
                escape_html(&idx.name),
                escape_html(&idx.columns.join(", "))
            ));
        }
        for idx in &table_diff.modified_indexes {
            lines.push_str(&format!(
                r#"<li class="py-1"><span class="inline-block min-w-6 rounded bg-amber-100 px-2 py-0.5 text-xs font-semibold text-amber-800">ALTER</span> index <code class="rounded bg-slate-100 px-1.5 py-0.5 text-slate-800">{}</code> <span class="text-slate-500">[{}] -&gt; [{}]</span></li>"#,
                escape_html(&idx.old.name),
                escape_html(&idx.old.columns.join(", ")),
                escape_html(&idx.new.columns.join(", "))
            ));
        }
        for fk in &table_diff.added_foreign_keys {
            lines.push_str(&format!(
                r#"<li class="py-1"><span class="inline-block min-w-6 rounded bg-emerald-100 px-2 py-0.5 text-xs font-semibold text-emerald-800">ADD</span> fk <code class="rounded bg-slate-100 px-1.5 py-0.5 text-slate-800">{}</code> <span class="text-slate-500">({}) -&gt; {}({})</span></li>"#,
                escape_html(&fk.name),
                escape_html(&fk.columns.join(", ")),
                escape_html(&fk.referenced_table),
                escape_html(&fk.referenced_columns.join(", "))
            ));
        }
        for fk in &table_diff.removed_foreign_keys {
            lines.push_str(&format!(
                r#"<li class="py-1"><span class="inline-block min-w-6 rounded bg-rose-100 px-2 py-0.5 text-xs font-semibold text-rose-800">DROP</span> fk <code class="rounded bg-slate-100 px-1.5 py-0.5 text-slate-800">{}</code> <span class="text-slate-500">({}) -&gt; {}({})</span> <span class="text-xs font-semibold text-rose-700">DESTRUCTIF</span></li>"#,
                escape_html(&fk.name),
                escape_html(&fk.columns.join(", ")),
                escape_html(&fk.referenced_table),
                escape_html(&fk.referenced_columns.join(", "))
            ));
        }
        for fk in &table_diff.modified_foreign_keys {
            lines.push_str(&format!(
                r#"<li class="py-1"><span class="inline-block min-w-6 rounded bg-amber-100 px-2 py-0.5 text-xs font-semibold text-amber-800">ALTER</span> fk <code class="rounded bg-slate-100 px-1.5 py-0.5 text-slate-800">{}</code></li>"#,
                escape_html(&fk.old.name),
            ));
        }
        for (name, constraint) in &table_diff.added_constraints {
            lines.push_str(&format!(
                r#"<li class="py-1"><span class="inline-block min-w-6 rounded bg-emerald-100 px-2 py-0.5 text-xs font-semibold text-emerald-800">ADD</span> contrainte <code class="rounded bg-slate-100 px-1.5 py-0.5 text-slate-800">{}</code> <span class="text-slate-500">{}</span></li>"#,
                escape_html(name),
                escape_html(&format_constraint_html(constraint))
            ));
        }
        for (name, constraint) in &table_diff.removed_constraints {
            lines.push_str(&format!(
                r#"<li class="py-1"><span class="inline-block min-w-6 rounded bg-rose-100 px-2 py-0.5 text-xs font-semibold text-rose-800">DROP</span> contrainte <code class="rounded bg-slate-100 px-1.5 py-0.5 text-slate-800">{}</code> <span class="text-slate-500">{}</span> <span class="text-xs font-semibold text-rose-700">DESTRUCTIF</span></li>"#,
                escape_html(name),
                escape_html(&format_constraint_html(constraint))
            ));
        }
        for c_mod in &table_diff.modified_constraints {
            lines.push_str(&format!(
                r#"<li class="py-1"><span class="inline-block min-w-6 rounded bg-amber-100 px-2 py-0.5 text-xs font-semibold text-amber-800">ALTER</span> contrainte <code class="rounded bg-slate-100 px-1.5 py-0.5 text-slate-800">{}</code></li>"#,
                escape_html(&c_mod.name),
            ));
        }

        if lines.is_empty() {
            lines.push_str(r#"<li class="py-1 text-slate-500">Aucune modification</li>"#);
        }

        table_cards.push_str(&format!(
            r#"<section class="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
  <h3 class="mb-3 text-base font-semibold text-slate-900">Table <code class="rounded bg-slate-100 px-1.5 py-0.5">{}</code></h3>
  <ul class="text-sm text-slate-700">{}</ul>
</section>"#,
            escape_html(&table_diff.table_name),
            lines
        ));
    }

    if table_cards.is_empty() {
        table_cards.push_str(
            r#"<section class="rounded-xl border border-slate-200 bg-white p-5 shadow-sm text-sm text-slate-500">Aucune modification de table.</section>"#,
        );
    }

    let added_tables = if diff.added_tables.is_empty() {
        "<li class=\"text-slate-500\">Aucune</li>".to_owned()
    } else {
        diff.added_tables
            .iter()
            .map(|t| {
                format!(
                    "<li><code class=\"rounded bg-slate-100 px-1.5 py-0.5 text-slate-800\">{}</code></li>",
                    escape_html(t)
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };

    let removed_tables = if diff.removed_tables.is_empty() {
        "<li class=\"text-slate-500\">Aucune</li>".to_owned()
    } else {
        diff.removed_tables
            .iter()
            .map(|t| {
                format!(
                    "<li><code class=\"rounded bg-slate-100 px-1.5 py-0.5 text-slate-800\">{}</code> <span class=\"text-xs font-semibold text-rose-700\">DESTRUCTIF</span></li>",
                    escape_html(t)
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };

    let warnings = if diff.destructive_warnings.is_empty() {
        "<li class=\"text-slate-500\">Aucune alerte destructive.</li>".to_owned()
    } else {
        diff.destructive_warnings
            .iter()
            .map(|w| {
                format!(
                    "<li class=\"rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-rose-900\">{}</li>",
                    escape_html(w)
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };

    let total_alters = diff.altered_tables.len();

    format!(
        r#"<!doctype html>
<html lang="fr">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Rapport Diff de schema</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="min-h-screen bg-slate-100 text-slate-900">
  <main class="mx-auto max-w-7xl px-6 py-8">
    <header class="mb-6 rounded-2xl border border-slate-300 bg-white px-6 py-5 shadow-sm">
      <h1 class="text-2xl font-semibold tracking-tight">Rapport de differences de schema</h1>
      <p class="mt-2 text-sm text-slate-600">Comparaison source vers cible.</p>
      <div class="mt-4 grid gap-3 sm:grid-cols-3">
        <div class="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3">
          <p class="text-xs font-medium uppercase text-slate-500">Tables ajoutees</p>
          <p class="mt-1 text-2xl font-semibold">{}</p>
        </div>
        <div class="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3">
          <p class="text-xs font-medium uppercase text-slate-500">Tables supprimees</p>
          <p class="mt-1 text-2xl font-semibold">{}</p>
        </div>
        <div class="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3">
          <p class="text-xs font-medium uppercase text-slate-500">Tables modifiees</p>
          <p class="mt-1 text-2xl font-semibold">{}</p>
        </div>
      </div>
    </header>

    <section class="mb-6 grid gap-4 lg:grid-cols-2">
      <article class="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <h2 class="mb-3 text-base font-semibold">Tables ajoutees</h2>
        <ul class="space-y-2 text-sm">{}</ul>
      </article>
      <article class="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <h2 class="mb-3 text-base font-semibold">Tables supprimees</h2>
        <ul class="space-y-2 text-sm">{}</ul>
      </article>
    </section>

    <section class="mb-6">
      <h2 class="mb-4 text-lg font-semibold">Details des modifications</h2>
      <div class="grid gap-4">{}</div>
    </section>

    <section class="rounded-xl border border-rose-300 bg-white p-5 shadow-sm">
      <h2 class="mb-3 text-base font-semibold text-rose-900">Operations destructives</h2>
      <ul class="space-y-2 text-sm">{}</ul>
    </section>
  </main>
</body>
</html>"#,
        diff.added_tables.len(),
        diff.removed_tables.len(),
        total_alters,
        added_tables,
        removed_tables,
        table_cards,
        warnings
    )
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn format_constraint_html(c: &Constraint) -> String {
    match c {
        Constraint::Unique { columns } => format!("UNIQUE ({})", columns.join(", ")),
        Constraint::Check { expression } => format!("CHECK ({})", expression),
    }
}
