# Hackathon Diff SQL (Rust)

Base de projet en Rust avec deux connecteurs:
- PostgreSQL
- SQLite

## Prerequis

- Rust stable (`cargo`, `rustc`)

## Variables d'environnement (optionnel)

- `PG_DATABASE_URL` pour PostgreSQL (ex: `postgres://user:password@localhost:5432/dbname`)
- `SQLITE_DATABASE_URL` pour SQLite (ex: `sqlite://./data/dev.db`)

## Commandes

Tester une connexion:

```bash
cargo run -- ping postgres --url "postgres://user:password@localhost:5432/dbname"
cargo run -- ping sqlite --url "sqlite://./data/dev.db"
```

Afficher le schema detecte:

```bash
cargo run -- schema postgres --url "postgres://user:password@localhost:5432/dbname"
cargo run -- schema sqlite --url "sqlite://./data/dev.db"
```

Sans `--url`, l'outil lit les variables d'environnement.

## Comparer deux bases et generer migration

```bash
cargo run -- diff \
  --source-connector postgres \
  --source-url "postgres://user:password@localhost:5432/source_db" \
  --target-connector sqlite \
  --target-url "sqlite://./data/target.db" \
  --out-sql "migration.sql" \
  --out-report "diff_report.md" \
  --out-html "diff_report.html"
```

Mode dry-run (affiche sans ecrire de fichiers):

```bash
cargo run -- diff \
  --source-connector postgres \
  --source-url "postgres://user:password@localhost:5432/source_db" \
  --target-connector sqlite \
  --target-url "sqlite://./data/target.db" \
  --dry-run
```

La commande `diff`:
- normalise les schemas dans un modele canonique (types canoniques inter-SGBD: serial~integer, numeric(p,s)~numeric, etc.)
- detecte tables/colonnes/index/fk ajoutes, supprimes, modifies
- genere un rapport Markdown et un rapport HTML stylise (Tailwind CDN)
- genere un SQL de migration adapte au **dialecte cible** (Postgres ou SQLite selon `--target-connector`); si la cible est un fichier SQL, le dialecte par defaut est Postgres
- pour SQLite, les operations `ALTER` non supportees nativement (FK, changement de type, certains changements de contraintes) sont signalees explicitement avec un commentaire de recreation de table
- marque les operations destructives et evite les faux positifs sur les conversions de type non destructives (ex. integer -> bigint)

## Comparer a partir de dumps SQL

Source par fichier SQL, cible par DB:

```bash
cargo run -- diff \
  --source-sql "./schema/source.sql" \
  --target-connector postgres \
  --target-url "postgres://user:password@localhost:5432/target_db" \
  --dry-run
```

Source et cible par fichiers SQL:

```bash
cargo run -- diff \
  --source-sql "./schema/source.sql" \
  --target-sql "./schema/target.sql" \
  --out-sql "migration.sql" \
  --out-report "diff_report.md"
```

Regle CLI:
- pour chaque cote (`source`, `target`), choisir exactement une entree:
  - `--*-connector` (+ `--*-url` optionnel via variables d'environnement), ou
  - `--*-sql`

## Tests et validation

Lancer les tests unitaires (parser, diff engine, generateur SQL):

```bash
cargo test
```

Verification rapide en dry-run (fichiers SQL du depot):

```bash
cargo run -- diff --source-sql "./docker/postgres-init/001_init.sql" --target-sql "./schema/sqlite_init.sql" --dry-run
```
