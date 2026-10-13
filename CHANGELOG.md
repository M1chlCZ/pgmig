# Changelog

All notable changes to this project are documented in this file. The format
follows Keep a Changelog, and the project uses semantic versioning.

## [0.3.0] - 2026-10-13

### Added

- `Config.LockTimeout` bounds the wait for the migration advisory lock with a
  `pg_try_advisory_lock` poll loop; a zero timeout keeps the blocking wait.
  `ErrLockTimeout` identifies the exhausted wait.
- `Config.StatementTimeout` puts a transaction-local `statement_timeout` on
  every migration and metadata transaction. A migration file can override it
  with its own `SET LOCAL`.
- `Version` and `CheckReadiness` read the applied migration count and the
  latest version through any `Querier` (`*pgx.Conn`, `*pgxpool.Pool` or
  `pgx.Tx`) and compare them against an expected `VersionInfo`.

### Changed

- `Migrate`, `Run`, `Status`, `Verify` and `Plan` reject negative lock or
  statement timeouts.

## [0.2.0] - 2026-10-31

### Added

- `Status` reports every migration version as `applied`, `pending`, `drift`
  or `unknown`, with the applied timestamp and the file checksum.
- `Verify` checks the recorded history against the migration files without
  applying anything.
- `Plan` returns the migrations a run would apply, for dry-run reporting.
- `cmd/pgmig` CLI with `apply` (including `-dry-run`), `status` (including
  `-json`) and `verify` subcommands.

## [0.1.0] - 2026-10-04

### Added

- Ordered migrations with a transaction-control scanner.
- SHA-256 checksum and name drift detection.
- Least-privilege migration identity and strict app schema verification.
