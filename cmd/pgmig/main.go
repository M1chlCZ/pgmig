// Command pgmig applies and inspects PostgreSQL migrations.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/m1chlcz/pgmig"
)

const usageText = `usage: pgmig <command> [flags]

commands:
  apply    apply pending migrations (-dry-run only lists them)
  status   print the state of every migration version
  verify   check recorded checksums against the migration files

flags:
  -database-url  PostgreSQL connection string (default PGMIG_DATABASE_URL)
  -dir           directory with the NNNNNN_name.sql files
  -role          owner role
  -app-schema    application schema (enables strict verification)
  -meta-schema   metadata schema (default pgmig_meta)
`

const connectionCleanupTimeout = 5 * time.Second

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usageText)
		return 2
	}
	command := args[0]
	switch command {
	case "apply", "status", "verify":
	default:
		fmt.Fprintf(stderr, "pgmig: unknown command %q\n", command)
		fmt.Fprint(stderr, usageText)
		return 2
	}

	flags := flag.NewFlagSet("pgmig "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	databaseURL := flags.String("database-url", os.Getenv("PGMIG_DATABASE_URL"), "PostgreSQL connection string")
	directory := flags.String("dir", "", "directory with the migration files")
	role := flags.String("role", "", "owner role")
	appSchema := flags.String("app-schema", "", "application schema")
	metaSchema := flags.String("meta-schema", "", "metadata schema")
	jsonOutput := false
	dryRun := false
	switch command {
	case "status":
		flags.BoolVar(&jsonOutput, "json", false, "print machine-readable JSON")
	case "apply":
		flags.BoolVar(&dryRun, "dry-run", false, "list the migrations that would be applied")
	}
	if err := flags.Parse(args[1:]); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "pgmig: unexpected arguments")
		return 2
	}
	if *databaseURL == "" {
		fmt.Fprintln(stderr, "pgmig: -database-url or PGMIG_DATABASE_URL is required")
		return 2
	}
	if *directory == "" {
		fmt.Fprintln(stderr, "pgmig: -dir is required")
		return 2
	}
	if *role == "" {
		fmt.Fprintln(stderr, "pgmig: -role is required")
		return 2
	}
	config := pgmig.Config{
		Role:       *role,
		AppSchema:  *appSchema,
		MetaSchema: *metaSchema,
		Files:      os.DirFS(*directory),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch command {
	case "apply":
		return runApply(ctx, *databaseURL, config, dryRun, stdout, stderr)
	case "status":
		return runStatus(ctx, *databaseURL, config, jsonOutput, stdout, stderr)
	case "verify":
		return runVerify(ctx, *databaseURL, config, stdout, stderr)
	}
	return 2
}

func runApply(
	ctx context.Context,
	databaseURL string,
	config pgmig.Config,
	dryRun bool,
	stdout, stderr io.Writer,
) int {
	conn, err := openConnection(ctx, databaseURL)
	if err != nil {
		fmt.Fprintf(stderr, "pgmig: %v\n", err)
		return 1
	}
	defer closeConnection(conn)
	planned, err := pgmig.Plan(ctx, conn, config)
	if err != nil {
		fmt.Fprintf(stderr, "pgmig: %v\n", err)
		return 1
	}
	if len(planned) == 0 {
		fmt.Fprintln(stdout, "no pending migrations")
		return 0
	}
	if dryRun {
		for _, migration := range planned {
			fmt.Fprintf(stdout, "%06d %s\n", migration.Version, migration.Name)
		}
		return 0
	}
	if err := pgmig.Migrate(ctx, conn, config); err != nil {
		fmt.Fprintf(stderr, "pgmig: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "applied %d migrations\n", len(planned))
	return 0
}

func runStatus(
	ctx context.Context,
	databaseURL string,
	config pgmig.Config,
	jsonOutput bool,
	stdout, stderr io.Writer,
) int {
	conn, err := openConnection(ctx, databaseURL)
	if err != nil {
		fmt.Fprintf(stderr, "pgmig: %v\n", err)
		return 1
	}
	defer closeConnection(conn)
	statuses, err := pgmig.Status(ctx, conn, config)
	if err != nil {
		fmt.Fprintf(stderr, "pgmig: %v\n", err)
		return 1
	}
	if jsonOutput {
		data, marshalErr := json.MarshalIndent(statusDocument(statuses), "", "  ")
		if marshalErr != nil {
			fmt.Fprintf(stderr, "pgmig: encode status: %v\n", marshalErr)
			return 1
		}
		fmt.Fprintln(stdout, string(data))
		return 0
	}
	fmt.Fprintf(stdout, "%-8s %-8s %-25s %s\n", "VERSION", "STATE", "APPLIED_AT", "NAME")
	for _, status := range statuses {
		appliedAt := "-"
		if !status.AppliedAt.IsZero() {
			appliedAt = status.AppliedAt.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(stdout, "%06d   %-8s %-25s %s\n", status.Version, status.State, appliedAt, status.Name)
	}
	return 0
}

func runVerify(
	ctx context.Context,
	databaseURL string,
	config pgmig.Config,
	stdout, stderr io.Writer,
) int {
	conn, err := openConnection(ctx, databaseURL)
	if err != nil {
		fmt.Fprintf(stderr, "pgmig: %v\n", err)
		return 1
	}
	defer closeConnection(conn)
	if err := pgmig.Verify(ctx, conn, config); err != nil {
		fmt.Fprintf(stderr, "pgmig: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "verified")
	return 0
}

func openConnection(ctx context.Context, databaseURL string) (*pgx.Conn, error) {
	parsed, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, errors.New("database URL is invalid")
	}
	conn, err := pgx.ConnectConfig(ctx, parsed)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("database connection canceled: %w", ctx.Err())
		}
		return nil, errors.New("database URL is unavailable")
	}
	return conn, nil
}

func closeConnection(conn *pgx.Conn) {
	closeCtx, cancel := context.WithTimeout(context.Background(), connectionCleanupTimeout)
	defer cancel()
	_ = conn.Close(closeCtx)
}

type statusEntry struct {
	Version   int64  `json:"version"`
	Name      string `json:"name"`
	State     string `json:"state"`
	AppliedAt string `json:"applied_at,omitempty"`
	Checksum  string `json:"checksum,omitempty"`
}

func statusDocument(statuses []pgmig.MigrationStatus) []statusEntry {
	entries := make([]statusEntry, 0, len(statuses))
	for _, status := range statuses {
		entry := statusEntry{
			Version: status.Version,
			Name:    status.Name,
			State:   string(status.State),
		}
		if !status.AppliedAt.IsZero() {
			entry.AppliedAt = status.AppliedAt.UTC().Format(time.RFC3339)
		}
		var zero [32]byte
		if status.Checksum != zero {
			entry.Checksum = hex.EncodeToString(status.Checksum[:])
		}
		entries = append(entries, entry)
	}
	return entries
}
