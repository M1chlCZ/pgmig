//nolint:testpackage // White-box tests exercise unexported migration loading, planning and SQL validation helpers.
package pgmig

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"testing/fstest"
)

func TestRunRejectsMissingOrInvalidDatabaseURL(t *testing.T) {
	t.Parallel()

	config := Config{Role: "app_owner", Files: fstest.MapFS{"000001_alpha.sql": {Data: []byte("SELECT 1;\n")}}}
	if err := Run(context.Background(), "", config); err == nil {
		t.Fatal("Run accepted an empty database URL")
	}
	if err := Run(context.Background(), "postgres://user:pass@host:notaport/db", config); err == nil {
		t.Fatal("Run accepted an invalid database URL")
	}
	err := Run(context.Background(), "postgres://postgres@127.0.0.1:1/pgmig?sslmode=disable&connect_timeout=1", config)
	if err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("Run unavailable error = %v", err)
	}
}

func TestLoadMigrationsOrdersAndHashesSQL(t *testing.T) {
	t.Parallel()

	files := fstest.MapFS{
		"000002_catalog.sql":  {Data: []byte("SELECT 2;\n")},
		"000001_accounts.sql": {Data: []byte("SELECT 1;\n")},
	}

	got, err := loadMigrations(files)
	if err != nil {
		t.Fatalf("loadMigrations returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(migrations) = %d, want 2", len(got))
	}
	if got[0].Version != 1 || got[0].Name != "accounts" || got[0].SQL != "SELECT 1;\n" {
		t.Fatalf("first migration = %#v", got[0])
	}
	if got[0].Checksum != sha256.Sum256([]byte(got[0].SQL)) {
		t.Fatal("first migration checksum does not match its exact SQL bytes")
	}
	if got[1].Version != 2 || got[1].Name != "catalog" {
		t.Fatalf("second migration = %#v", got[1])
	}
}

func TestLoadMigrationsRejectsInvalidOrDuplicateVersions(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		files fstest.MapFS
	}{
		{
			name:  "invalid filename",
			files: fstest.MapFS{"accounts.sql": {Data: []byte("SELECT 1;")}},
		},
		{
			name:  "invalid name characters",
			files: fstest.MapFS{"000001_Bad name.sql": {Data: []byte("SELECT 1;")}},
		},
		{
			name: "duplicate version",
			files: fstest.MapFS{
				"000001_accounts.sql": {Data: []byte("SELECT 1;")},
				"000001_users.sql":    {Data: []byte("SELECT 2;")},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := loadMigrations(tt.files); err == nil {
				t.Fatal("loadMigrations accepted invalid migration set")
			}
		})
	}
}

func TestLoadMigrationsRejectsEmptySQLAndVersionGaps(t *testing.T) {
	t.Parallel()

	for _, files := range []fstest.MapFS{
		{"000001_empty.sql": {Data: nil}},
		{
			"000001_one.sql":   {Data: []byte("SELECT 1;")},
			"000003_three.sql": {Data: []byte("SELECT 3;")},
		},
	} {
		if _, err := loadMigrations(files); err == nil {
			t.Fatal("loadMigrations accepted empty SQL or a version gap")
		}
	}
}

func TestMigrationRoleIdentifierRejectsUnsafeInputBeforeUsingConnection(t *testing.T) {
	t.Parallel()

	for _, role := range []string{"", "UPPER_OWNER", "1owner", "owner-name", "owner; RESET ROLE", "owner.role", "owner\x00role", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {
		t.Run(role, func(t *testing.T) {
			t.Parallel()
			if _, err := migrationRoleIdentifier(role); err == nil {
				t.Fatal("migrationRoleIdentifier accepted an unsafe role")
			}
		})
	}
}

func TestMigrationRoleIdentifierQuotesPortableOwnerRole(t *testing.T) {
	t.Parallel()

	got, err := migrationRoleIdentifier("app_production_owner")
	if err != nil {
		t.Fatalf("migrationRoleIdentifier returned error: %v", err)
	}
	if got != `"app_production_owner"` {
		t.Fatalf("quoted role = %q", got)
	}
}

func TestMigrationPlanDefaultsAndRejectsInvalidSchemas(t *testing.T) {
	t.Parallel()

	plan, err := newMigrationPlan(Config{Role: "app_owner", Files: fstest.MapFS{}})
	if err != nil {
		t.Fatalf("newMigrationPlan: %v", err)
	}
	if plan.metaSchema != DefaultMetaSchema || plan.metaTable != `"pgmig_meta".schema_migrations` {
		t.Fatalf("default plan = %#v", plan)
	}
	for _, config := range []Config{
		{Role: "UPPER_OWNER", Files: fstest.MapFS{}},
		{Role: "app_owner", MetaSchema: "Bad Schema", Files: fstest.MapFS{}},
		{Role: "app_owner", MetaSchema: "meta; DROP TABLE x", Files: fstest.MapFS{}},
		{Role: "app_owner", AppSchema: "app schema", Files: fstest.MapFS{}},
	} {
		if _, planErr := newMigrationPlan(config); planErr == nil {
			t.Fatalf("newMigrationPlan accepted %#v", config)
		}
	}
}

func TestValidateMigrationSQLRejectsTopLevelTransactionControl(t *testing.T) {
	t.Parallel()

	for _, sql := range []string{
		"BEGIN; SELECT 1;",
		" commit AND CHAIN;",
		"-- comment\nROLLBACK TO SAVEPOINT before_change;",
		"-- carriage-return comment\rCOMMIT;",
		"\vCOMMIT;",
		"\ufeffCOMMIT;",
		"/* outer /* nested */ comment */ ABORT;",
		"START /* allowed gap */ TRANSACTION READ WRITE;",
		"END;",
		"PREPARE -- gap\n TRANSACTION 'migration';",
	} {
		if err := validateMigrationSQL(sql); err == nil {
			t.Fatalf("validateMigrationSQL accepted transaction control in %q", sql)
		}
	}
}

func TestValidateMigrationSQLIgnoresProtectedAndNonStatementTokens(t *testing.T) {
	t.Parallel()

	for _, sql := range []string{
		"SELECT 'COMMIT; BEGIN', \"ROLLBACK\";",
		"SELECT E'quote\\' COMMIT', 'it''s ROLLBACK';",
		"SELECT 'value'; -- COMMIT\nSELECT 1; /* BEGIN; */",
		"CREATE FUNCTION app.probe() RETURNS void LANGUAGE plpgsql AS $$ BEGIN PERFORM 'COMMIT'; END $$;",
		"CREATE FUNCTION app.probe() RETURNS text LANGUAGE sql AS $body$ SELECT 'PREPARE TRANSACTION'; $body$;",
		"SELECT begin_column FROM app.example;",
		"PREPARE safe_statement AS SELECT 1;",
		"SET application_name TO transaction;",
		"SET LOCAL application_name TO 'transaction';",
	} {
		if err := validateMigrationSQL(sql); err != nil {
			t.Fatalf("validateMigrationSQL rejected safe SQL %q: %v", sql, err)
		}
	}
}

func TestValidateMigrationSQLRejectsUnterminatedProtectedText(t *testing.T) {
	t.Parallel()

	for _, sql := range []string{
		"SELECT 'unterminated",
		"SELECT \"unterminated",
		"SELECT $body$unterminated",
		"SELECT 1; /* unterminated",
	} {
		if err := validateMigrationSQL(sql); err == nil {
			t.Fatalf("validateMigrationSQL accepted unterminated SQL %q", sql)
		}
	}
}
