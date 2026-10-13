//nolint:paralleltest,testpackage // Scenarios share one isolated PostgreSQL database. Serial execution is intentional.
package pgmig

import (
	"context"
	"crypto/sha256"
	"os"
	"strings"
	"testing"
	"testing/fstest"
)

func TestInspectIntegration(t *testing.T) {
	adminURL := strings.TrimSpace(os.Getenv(testDatabaseEnv))
	if adminURL == "" {
		t.Skip("set " + testDatabaseEnv + " to run PostgreSQL integration tests")
	}
	admin := connect(t, adminURL)
	defer func() { _ = admin.Close(context.Background()) }()
	resetDatabase(t, admin)
	ctx := context.Background()

	migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = migrator.Close(ctx) }()

	alpha := []byte("CREATE TABLE app.alpha (id integer PRIMARY KEY);\n")
	beta := []byte("CREATE TABLE app.beta (id integer);\n")
	files := fstest.MapFS{
		"000001_alpha.sql": {Data: alpha},
		"000002_beta.sql":  {Data: beta},
	}
	config := Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: files}

	statuses, err := Status(ctx, migrator, config)
	if err != nil {
		t.Fatalf("Status before apply: %v", err)
	}
	if len(statuses) != 2 || statuses[0].State != StatePending || statuses[1].State != StatePending {
		t.Fatalf("statuses before apply = %+v", statuses)
	}
	var zeroChecksum [sha256.Size]byte
	if statuses[0].Name != "alpha" || statuses[0].Checksum == zeroChecksum {
		t.Fatalf("pending status metadata = %+v", statuses[0])
	}
	if !statuses[0].AppliedAt.IsZero() {
		t.Fatalf("pending status carries applied_at: %s", statuses[0].AppliedAt)
	}
	planned, err := Plan(ctx, migrator, config)
	if err != nil {
		t.Fatalf("Plan before apply: %v", err)
	}
	if len(planned) != 2 || planned[1].Version != 2 || planned[1].Name != "beta" {
		t.Fatalf("plan before apply = %+v", planned)
	}
	if err := Verify(ctx, migrator, config); err != nil {
		t.Fatalf("Verify before apply: %v", err)
	}

	if err := Run(ctx, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword), config); err != nil {
		t.Fatalf("Run: %v", err)
	}

	statuses, err = Status(ctx, migrator, config)
	if err != nil {
		t.Fatalf("Status after apply: %v", err)
	}
	for _, status := range statuses {
		if status.State != StateApplied || status.AppliedAt.IsZero() || status.Checksum == zeroChecksum {
			t.Fatalf("applied status = %+v", status)
		}
	}
	planned, err = Plan(ctx, migrator, config)
	if err != nil {
		t.Fatalf("Plan after apply: %v", err)
	}
	if len(planned) != 0 {
		t.Fatalf("plan after apply = %+v, want empty", planned)
	}
	if err := Verify(ctx, migrator, config); err != nil {
		t.Fatalf("Verify after apply: %v", err)
	}

	drifted := fstest.MapFS{
		"000001_alpha.sql": {Data: alpha},
		"000002_beta.sql":  {Data: []byte("CREATE TABLE app.beta (id bigint);\n")},
	}
	driftConfig := Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: drifted}
	statuses, err = Status(ctx, migrator, driftConfig)
	if err != nil {
		t.Fatalf("Status with drift: %v", err)
	}
	if statuses[0].State != StateApplied || statuses[1].State != StateDrift {
		t.Fatalf("drift statuses = %+v", statuses)
	}
	assertErrorContains(t, Verify(ctx, migrator, driftConfig), "checksum drift")
	if _, planErr := Plan(ctx, migrator, driftConfig); planErr == nil {
		t.Fatal("Plan with drift did not fail")
	}

	partial := fstest.MapFS{"000001_alpha.sql": {Data: alpha}}
	partialConfig := Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: partial}
	statuses, err = Status(ctx, migrator, partialConfig)
	if err != nil {
		t.Fatalf("Status with unknown version: %v", err)
	}
	if len(statuses) != 2 || statuses[0].State != StateApplied || statuses[1].State != StateUnknown {
		t.Fatalf("unknown statuses = %+v", statuses)
	}
	assertErrorContains(t, Verify(ctx, migrator, partialConfig), "unknown migration")
}

func TestVersionReadinessIntegration(t *testing.T) {
	adminURL := strings.TrimSpace(os.Getenv(testDatabaseEnv))
	if adminURL == "" {
		t.Skip("set " + testDatabaseEnv + " to run PostgreSQL integration tests")
	}
	admin := connect(t, adminURL)
	defer func() { _ = admin.Close(context.Background()) }()
	resetDatabase(t, admin)
	ctx := context.Background()

	migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = migrator.Close(ctx) }()
	readiness := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = readiness.Close(ctx) }()
	if _, err := readiness.Exec(ctx, "SET ROLE "+testOwnerRole); err != nil {
		t.Fatalf("raise readiness session to the owner role: %v", err)
	}

	alpha := []byte("CREATE TABLE app.alpha (id integer PRIMARY KEY);\n")
	beta := []byte("CREATE TABLE app.beta (id integer);\n")
	firstConfig := Config{
		Role:      testOwnerRole,
		AppSchema: testAppSchema,
		Files:     fstest.MapFS{"000001_alpha.sql": {Data: alpha}},
	}

	empty, err := Version(ctx, readiness, firstConfig)
	if err != nil {
		t.Fatalf("Version before apply: %v", err)
	}
	if empty != (VersionInfo{}) {
		t.Fatalf("empty VersionInfo = %+v", empty)
	}
	emptyErr := CheckReadiness(ctx, readiness, firstConfig, VersionInfo{Version: 1, MigrationCount: 1})
	assertErrorContains(t, emptyErr, "behind")
	assertErrorContains(t, emptyErr, "0/0")

	if err := Migrate(ctx, migrator, firstConfig); err != nil {
		t.Fatalf("apply first migration: %v", err)
	}
	partial, err := Version(ctx, readiness, firstConfig)
	if err != nil {
		t.Fatalf("Version after first apply: %v", err)
	}
	if partial != (VersionInfo{Version: 1, MigrationCount: 1}) {
		t.Fatalf("partial VersionInfo = %+v", partial)
	}

	fullConfig := Config{
		Role:      testOwnerRole,
		AppSchema: testAppSchema,
		Files: fstest.MapFS{
			"000001_alpha.sql": {Data: alpha},
			"000002_beta.sql":  {Data: beta},
		},
	}
	partialErr := CheckReadiness(ctx, readiness, fullConfig, VersionInfo{Version: 2, MigrationCount: 2})
	assertErrorContains(t, partialErr, "behind")
	assertErrorContains(t, partialErr, "1/1")

	if err := Migrate(ctx, migrator, fullConfig); err != nil {
		t.Fatalf("apply second migration: %v", err)
	}
	full, err := Version(ctx, readiness, fullConfig)
	if err != nil {
		t.Fatalf("Version after full apply: %v", err)
	}
	if full != (VersionInfo{Version: 2, MigrationCount: 2}) {
		t.Fatalf("full VersionInfo = %+v", full)
	}
	if err := CheckReadiness(ctx, readiness, fullConfig, full); err != nil {
		t.Fatalf("CheckReadiness at the applied version: %v", err)
	}
	aheadErr := CheckReadiness(ctx, readiness, fullConfig, VersionInfo{Version: 1, MigrationCount: 1})
	assertErrorContains(t, aheadErr, "ahead")
	assertErrorContains(t, aheadErr, "2/2")

	tx, err := readiness.Begin(ctx)
	if err != nil {
		t.Fatalf("begin readiness transaction: %v", err)
	}
	txVersion, err := Version(ctx, tx, fullConfig)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("Version on a transaction: %v", err)
	}
	if txVersion != full {
		t.Fatalf("transaction VersionInfo = %+v", txVersion)
	}
	if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
		t.Fatalf("rollback readiness transaction: %v", rollbackErr)
	}

	customConfig := Config{
		Role:       testOwnerRole,
		AppSchema:  testAppSchema,
		MetaSchema: "custom_meta",
		Files: fstest.MapFS{
			"000001_probe.sql":  {Data: []byte("SELECT 1;\n")},
			"000002_probe2.sql": {Data: []byte("SELECT 2;\n")},
		},
	}
	if err := Migrate(ctx, migrator, customConfig); err != nil {
		t.Fatalf("apply custom metadata schema: %v", err)
	}
	custom, err := Version(ctx, readiness, customConfig)
	if err != nil {
		t.Fatalf("Version on custom metadata schema: %v", err)
	}
	if custom != full {
		t.Fatalf("custom metadata VersionInfo = %+v", custom)
	}
}
