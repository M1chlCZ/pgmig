# Changelog

All notable changes to this project are documented in this file. The format
follows Keep a Changelog, and the project uses semantic versioning.

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
