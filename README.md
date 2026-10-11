# pgmig

Apply PostgreSQL migrations with checksum drift detection, a transaction-control scanner, and least-privilege role verification.

## Features

- Ordered `NNNNNN_name.sql` files. Versions must be contiguous, start at `000001`, and contain no gaps.
- Each file is scanned before execution. Top-level transaction control (`BEGIN`, `COMMIT`, `ROLLBACK`, `START TRANSACTION`, `PREPARE TRANSACTION`, `END`, `ABORT`) is rejected. Quoted text, dollar-quoted bodies, and comments do not count.
- SHA-256 checksums and names are recorded in the `pgmig_meta.schema_migrations` table. A changed file fails the run before anything applies.
- Every pending migration runs in one transaction under a `SET LOCAL ROLE` owner role. A failure rolls back the schema change and the history row together.
- A session advisory lock serializes concurrent runners. A canceled run closes its connection instead of leaving a locked session behind.
- The migration identity must be a least-privilege login, separated from the owner role. `SUPERUSER`, `CREATEDB`, `CREATEROLE`, `REPLICATION`, `BYPASSRLS`, `INHERIT`, extra memberships, and direct owner logins are rejected.
- Strict mode when `AppSchema` is set: the app schema must be owned by the owner role and granted `USAGE` to exactly one least-privilege runtime login. The database ACL must contain only the owner privileges and the two `CONNECT` grants. Any tampering fails before apply.

## Install

```
go get github.com/m1chlcz/pgmig
```

Go 1.27 or later is required.

## Quick start

```go
//go:embed migrations
var migrations embed.FS

files, err := fs.Sub(migrations, "migrations")
if err != nil {
	return err
}

err = pgmig.Run(ctx, databaseURL, pgmig.Config{
	Role:      "app_owner",
	AppSchema: "app",
	Files:     files,
})
```

`Run` opens a dedicated connection and closes it. Use `Migrate` to run on an existing `*pgx.Conn`.

## Config

| Field | Meaning |
| --- | --- |
| `Role` | The owner role. It must own the database, the app schema, and the migration metadata. Required. |
| `Files` | The migration set. Required. |
| `MetaSchema` | The metadata schema. Defaults to `pgmig_meta`. |
| `AppSchema` | The application schema. When set, strict role and ACL verification runs. |

## Inspect state

Three read-only entry points use the same least-privilege session as
`Migrate`:

```go
statuses, err := pgmig.Status(ctx, conn, config) // applied, pending, drift, unknown
err = pgmig.Verify(ctx, conn, config)            // checksum and name drift only
planned, err := pgmig.Plan(ctx, conn, config)    // what a run would apply
```

The CLI wraps them:

```
pgmig status -database-url "$DATABASE_URL" -dir ./migrations -role app_owner -json
pgmig verify -database-url "$DATABASE_URL" -dir ./migrations -role app_owner
pgmig apply  -database-url "$DATABASE_URL" -dir ./migrations -role app_owner -dry-run
```

`status` exits 0 even when versions are pending or drifted; `verify` exits 1
on drift. Add `-app-schema` and `-meta-schema` to match the strict setup. The
database URL can come from `PGMIG_DATABASE_URL` instead of the flag.

## Provisioning for strict mode

Run these statements as a superuser on the target database.

```sql
CREATE ROLE app_owner NOLOGIN NOINHERIT;
CREATE ROLE app_migrator LOGIN NOINHERIT;
CREATE ROLE app_runtime LOGIN NOINHERIT;

GRANT app_owner TO app_migrator WITH ADMIN FALSE, INHERIT FALSE, SET TRUE;

ALTER DATABASE app_db OWNER TO app_owner;
REVOKE ALL PRIVILEGES ON DATABASE app_db FROM PUBLIC;
SET ROLE app_owner;
GRANT CONNECT ON DATABASE app_db TO app_migrator, app_runtime;
RESET ROLE;

CREATE SCHEMA app AUTHORIZATION app_owner;
REVOKE ALL ON SCHEMA app FROM PUBLIC, app_migrator, app_runtime;
GRANT USAGE ON SCHEMA app TO app_runtime;
```

The runner creates `pgmig_meta` itself. Grant no privileges on that schema to any other role.

## Development

```
go test ./...
```

The integration tests create roles, schemas, and grants. Point them at a disposable database:

```
PGMIG_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5432/pgmig_test go test -run TestMigrateIntegration -v
```

Without `PGMIG_TEST_DATABASE_URL`, the integration tests skip.

## License

MIT. See `LICENSE`.
