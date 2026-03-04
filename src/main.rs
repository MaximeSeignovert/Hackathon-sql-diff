use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use hackathon_diff_sql::connectors::{
    ConnectorKind, PostgresConnector, SchemaConnector, SqliteConnector,
};
use hackathon_diff_sql::diff_engine::diff_schema;
use hackathon_diff_sql::reporter::{render_diff_html, render_diff_markdown};
use hackathon_diff_sql::schema_model::SchemaModel;
use hackathon_diff_sql::sql_dump_parser::parse_schema_from_sql;
use hackathon_diff_sql::sql_generator::{generate_migration_sql, SqlDialect};

#[derive(Parser, Debug)]
#[command(author, version, about = "Schema diff tooling bootstrap (Rust)")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Teste la connexion vers une base
    Ping {
        #[arg(value_enum)]
        connector: ConnectorKind,
        #[arg(long)]
        url: Option<String>,
    },
    /// Affiche tables + colonnes detectees
    Schema {
        #[arg(value_enum)]
        connector: ConnectorKind,
        #[arg(long)]
        url: Option<String>,
    },
    /// Compare 2 schemas et genere un script SQL
    Diff {
        #[arg(long = "source-connector", value_enum)]
        source_connector: Option<ConnectorKind>,
        #[arg(long = "source-url")]
        source_url: Option<String>,
        #[arg(long = "source-sql")]
        source_sql: Option<String>,
        #[arg(long = "target-connector", value_enum)]
        target_connector: Option<ConnectorKind>,
        #[arg(long = "target-url")]
        target_url: Option<String>,
        #[arg(long = "target-sql")]
        target_sql: Option<String>,
        #[arg(long, default_value = "migration.sql")]
        out_sql: String,
        #[arg(long, default_value = "diff_report.md")]
        out_report: String,
        #[arg(long, default_value = "diff_report.html")]
        out_html: String,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Ping { connector, url } => {
            let connector = build_connector(connector, url).await?;
            connector.ping().await?;
            println!("Connexion {:?} OK", connector.kind());
        }
        Commands::Schema { connector, url } => {
            let connector = build_connector(connector, url).await?;
            connector.ping().await?;
            let schema = connector.load_schema().await?;
            let model = SchemaModel::from_connector_tables(schema);

            println!("Schema ({:?})", connector.kind());
            for table in model.tables.values() {
                println!("- table: {}", table.name);
                for col in table.columns.values() {
                    let default = col
                        .default_value
                        .clone()
                        .unwrap_or_else(|| "NULL".to_owned());
                    println!(
                        "  - {} {} not_null={} default={}",
                        col.name, col.data_type, col.not_null, default
                    );
                }
                for idx in table.indexes.values() {
                    println!(
                        "  - index {} unique={} columns={}",
                        idx.name,
                        idx.unique,
                        idx.columns.join(",")
                    );
                }
                for fk in table.foreign_keys.values() {
                    println!(
                        "  - fk {} ({}) -> {}({})",
                        fk.name,
                        fk.columns.join(","),
                        fk.referenced_table,
                        fk.referenced_columns.join(",")
                    );
                }
            }
        }
        Commands::Diff {
            source_connector,
            source_url,
            source_sql,
            target_connector,
            target_url,
            target_sql,
            out_sql,
            out_report,
            out_html,
            dry_run,
        } => {
            let source_schema =
                load_schema_model(source_connector, source_url, source_sql, "source").await?;
            let target_schema =
                load_schema_model(target_connector, target_url, target_sql, "target").await?;

            let diff = diff_schema(&source_schema, &target_schema);
            let report = render_diff_markdown(&diff);
            let report_html = render_diff_html(&diff);
            let dialect = target_connector
                .map(SqlDialect::from)
                .unwrap_or(SqlDialect::Postgres);
            let sql = generate_migration_sql(&source_schema, &target_schema, &diff, dialect);

            println!("{}", report);
            println!();
            println!("--- SQL ---");
            println!("{}", sql);

            if !dry_run {
                std::fs::write(&out_report, report)
                    .with_context(|| format!("Impossible d'ecrire {}", out_report))?;
                std::fs::write(&out_sql, sql)
                    .with_context(|| format!("Impossible d'ecrire {}", out_sql))?;
                std::fs::write(&out_html, report_html)
                    .with_context(|| format!("Impossible d'ecrire {}", out_html))?;
                println!(
                    "Fichiers generes: rapport_md={} rapport_html={} script={}",
                    out_report, out_html, out_sql
                );
            } else {
                println!("Mode dry-run: aucun fichier ecrit.");
            }

            if !diff.has_changes() {
                println!("Aucun changement detecte.");
            }
            if !diff.destructive_warnings.is_empty() {
                println!("ATTENTION: operations destructives detectees.");
            }
        }
    }

    Ok(())
}

async fn build_connector(
    kind: ConnectorKind,
    explicit_url: Option<String>,
) -> Result<Box<dyn SchemaConnector>> {
    let url = resolve_url(kind, explicit_url)?;

    match kind {
        ConnectorKind::Postgres => {
            let connector = PostgresConnector::new(&url)
                .await
                .with_context(|| "Impossible d'initialiser le connecteur PostgreSQL")?;
            Ok(Box::new(connector))
        }
        ConnectorKind::Sqlite => {
            let connector = SqliteConnector::new(&url)
                .await
                .with_context(|| "Impossible d'initialiser le connecteur SQLite")?;
            Ok(Box::new(connector))
        }
    }
}

async fn load_schema_model(
    connector: Option<ConnectorKind>,
    url: Option<String>,
    sql_path: Option<String>,
    side: &str,
) -> Result<SchemaModel> {
    match (connector, sql_path) {
        (Some(kind), None) => {
            let conn = build_connector(kind, url).await?;
            conn.ping().await?;
            let tables = conn.load_schema().await?;
            Ok(SchemaModel::from_connector_tables(tables))
        }
        (None, Some(path)) => {
            let content = std::fs::read_to_string(&path)
                .with_context(|| format!("Impossible de lire {} sql file: {}", side, path))?;
            parse_schema_from_sql(&content)
                .with_context(|| format!("Impossible de parser {} sql file: {}", side, path))
        }
        (Some(_), Some(_)) => Err(anyhow!(
            "Pour {}: utilise soit connector/url, soit sql file, pas les deux.",
            side
        )),
        (None, None) => Err(anyhow!(
            "Pour {}: renseigne --{}-connector (+ --{}-url) ou --{}-sql.",
            side,
            side,
            side,
            side
        )),
    }
}

fn resolve_url(kind: ConnectorKind, explicit_url: Option<String>) -> Result<String> {
    if let Some(url) = explicit_url {
        return Ok(url);
    }

    let env_var = match kind {
        ConnectorKind::Postgres => "PG_DATABASE_URL",
        ConnectorKind::Sqlite => "SQLITE_DATABASE_URL",
    };

    std::env::var(env_var).map_err(|_| {
        anyhow!(
            "URL manquante: utilise --url ou definit la variable d'environnement {}",
            env_var
        )
    })
}
