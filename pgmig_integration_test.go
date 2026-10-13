//nolint:paralleltest,testpackage // Scenarios share one isolated PostgreSQL database and exercise the unexported migrationLockKey; serial execution is intentional.
package pgmig

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	testDatabaseEnv      = "PGMIG_TEST_DATABASE_URL"
	testOwnerRole        = "pgmig_test_owner"
	testMigratorRole     = "pgmig_test_migrator"
	testAppRole          = "pgmig_test_app"
	testAppSchema        = "app"
	testMigratorPassword = "pgmig_test_migrator_pw"
	testOwnerPassword    = "pgmig_test_owner_pw"
)

func TestMigrateIntegration(t *testing.T) {
	adminURL := strings.TrimSpace(os.Getenv(testDatabaseEnv))
	if adminURL == "" {
		t.Skip("set " + testDatabaseEnv + " to run PostgreSQL integration tests")
	}

	for _, scenario := range []struct {
		name string
		run  func(t *testing.T, adminURL string)
	}{
		{name: "clean run and no-op rerun", run: runCleanRunAndNoopRerun},
		{name: "custom metadata schema", run: runCustomMetadataSchema},
		{name: "checksum and name drift", run: runChecksumAndNameDrift},
		{name: "failed migration rolls back", run: runFailedMigrationRollsBack},
		{name: "transaction control is rejected before execution", run: runTransactionControlRejected},
		{name: "standard conforming strings are local to the migration", run: runStandardConformingStringsLocal},
		{name: "unknown applied migration fails closed", run: runUnknownAppliedMigrationFailsClosed},
		{name: "lock wait honors cancellation and releases connection", run: runLockWaitHonorsCancellation},
		{name: "concurrent runners use one session advisory lock", run: runConcurrentRunnersUseOneAdvisoryLock},
		{name: "lock timeout bounds the advisory lock wait", run: runLockTimeoutBoundsLockWait},
		{name: "statement timeout bounds migrations", run: runStatementTimeoutBoundsMigrations},
		{
			name: "statement timeout bounds the metadata transaction",
			run:  runStatementTimeoutBoundsMetadataTransaction,
		},
		{name: "migration identity is least privileged", run: runMigrationIdentityLeastPrivileged},
		{name: "metadata ACL tampering fails closed", run: runMetadataACLTamperingFailsClosed},
		{name: "app schema ACL tampering fails closed", run: runAppSchemaACLTamperingFailsClosed},
		{
			name: "database ACL tampering fails closed before applying migrations",
			run:  runDatabaseACLTamperingFailsClosed,
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			scenario.run(t, adminURL)
		})
	}
}

func runCleanRunAndNoopRerun(t *testing.T, adminURL string) {
	t.Helper()
	admin := connect(t, adminURL)
	defer func() { _ = admin.Close(context.Background()) }()
	resetDatabase(t, admin)
	ctx := context.Background()

	files := fstest.MapFS{
		"000001_alpha.sql": {Data: []byte("CREATE TABLE app.alpha (id integer PRIMARY KEY);\n")},
		"000002_beta.sql":  {Data: []byte("CREATE TABLE app.beta (id integer);\n")},
	}
	config := Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: files}
	migratorURL := databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword)
	if err := Run(ctx, migratorURL, config); err != nil {
		t.Fatalf("Run clean database: %v", err)
	}
	assertHistory(t, admin, 2)

	migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = migrator.Close(ctx) }()
	var currentUser, sessionUser string
	if err := migrator.QueryRow(ctx, "SELECT current_user, session_user").Scan(&currentUser, &sessionUser); err != nil {
		t.Fatalf("read migration session role: %v", err)
	}
	if currentUser != testMigratorRole || sessionUser != testMigratorRole {
		t.Fatalf("migration role leaked after run: current=%q session=%q", currentUser, sessionUser)
	}

	var firstApplied time.Time
	if err := admin.QueryRow(ctx, `SELECT applied_at FROM pgmig_meta.schema_migrations WHERE version = 1`).
		Scan(&firstApplied); err != nil {
		t.Fatalf("read first applied timestamp: %v", err)
	}
	if err := Migrate(ctx, migrator, config); err != nil {
		t.Fatalf("Migrate second run: %v", err)
	}
	var secondApplied time.Time
	if err := admin.QueryRow(ctx, `SELECT applied_at FROM pgmig_meta.schema_migrations WHERE version = 1`).
		Scan(&secondApplied); err != nil {
		t.Fatalf("read second applied timestamp: %v", err)
	}
	if !secondApplied.Equal(firstApplied) {
		t.Fatalf("no-op rerun changed applied_at: %s/%s", firstApplied, secondApplied)
	}
}

func runCustomMetadataSchema(t *testing.T, adminURL string) {
	t.Helper()
	admin := connect(t, adminURL)
	defer func() { _ = admin.Close(context.Background()) }()
	resetDatabase(t, admin)
	ctx := context.Background()
	migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = migrator.Close(ctx) }()

	files := fstest.MapFS{"000001_custom.sql": {Data: []byte("SELECT 1;\n")}}
	config := Config{
		Role:       testOwnerRole,
		AppSchema:  testAppSchema,
		MetaSchema: "custom_meta",
		Files:      files,
	}
	if err := Migrate(ctx, migrator, config); err != nil {
		t.Fatalf("Migrate custom metadata schema: %v", err)
	}
	var count int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM custom_meta.schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("read custom migration history: %v", err)
	}
	if count != 1 {
		t.Fatalf("custom migration history count = %d, want 1", count)
	}
	var defaultExists bool
	if err := admin.QueryRow(ctx, `SELECT to_regnamespace('pgmig_meta') IS NOT NULL`).Scan(&defaultExists); err != nil {
		t.Fatalf("check default metadata schema: %v", err)
	}
	if defaultExists {
		t.Fatal("default metadata schema was created for a custom schema")
	}
}

func runChecksumAndNameDrift(t *testing.T, adminURL string) {
	t.Helper()
	for _, testCase := range []struct {
		name    string
		changed fstest.MapFS
		want    string
	}{
		{
			name:    "checksum",
			changed: fstest.MapFS{"000001_alpha.sql": {Data: []byte("SELECT 2;\n")}},
			want:    "checksum",
		},
		{
			name:    "name",
			changed: fstest.MapFS{"000001_beta.sql": {Data: []byte("SELECT 1;\n")}},
			want:    "name",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runDriftScenario(t, adminURL, testCase.changed, testCase.want)
		})
	}
}

func runDriftScenario(t *testing.T, adminURL string, changed fstest.MapFS, want string) {
	t.Helper()
	admin := connect(t, adminURL)
	defer func() { _ = admin.Close(context.Background()) }()
	resetDatabase(t, admin)
	ctx := context.Background()
	migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = migrator.Close(ctx) }()

	original := fstest.MapFS{"000001_alpha.sql": {Data: []byte("SELECT 1;\n")}}
	if err := Migrate(
		ctx,
		migrator,
		Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: original},
	); err != nil {
		t.Fatalf("apply original migration: %v", err)
	}
	err := Migrate(ctx, migrator, Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: changed})
	assertErrorContains(t, err, want)
	assertErrorContains(t, err, "000001")
}

func runFailedMigrationRollsBack(t *testing.T, adminURL string) {
	t.Helper()
	admin := connect(t, adminURL)
	defer func() { _ = admin.Close(context.Background()) }()
	resetDatabase(t, admin)
	ctx := context.Background()
	migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = migrator.Close(ctx) }()

	broken := fstest.MapFS{
		"000001_broken.sql": {Data: []byte("CREATE TABLE app.must_rollback (id integer); SELECT 1 / 0;\n")},
	}
	if err := Migrate(ctx, migrator, Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: broken}); err == nil {
		t.Fatal("broken migration succeeded")
	}
	var tableExists bool
	if err := admin.QueryRow(ctx, `SELECT to_regclass('app.must_rollback') IS NOT NULL`).
		Scan(&tableExists); err != nil {
		t.Fatalf("check rolled-back table: %v", err)
	}
	if tableExists {
		t.Fatal("failed migration left its table behind")
	}
	assertHistory(t, admin, 0)

	fixed := fstest.MapFS{"000001_broken.sql": {Data: []byte("CREATE TABLE app.did_commit (id integer);\n")}}
	if err := Migrate(ctx, migrator, Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: fixed}); err != nil {
		t.Fatalf("retry corrected migration: %v", err)
	}
	assertHistory(t, admin, 1)
}

func runTransactionControlRejected(t *testing.T, adminURL string) {
	t.Helper()
	admin := connect(t, adminURL)
	defer func() { _ = admin.Close(context.Background()) }()
	resetDatabase(t, admin)
	ctx := context.Background()
	migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = migrator.Close(ctx) }()

	files := fstest.MapFS{
		"000001_escape.sql": {
			Data: []byte("CREATE TABLE app.must_not_commit (id integer); \vCOMMIT; SELECT 1 / 0;\n"),
		},
	}
	migrateErr := Migrate(ctx, migrator, Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: files})
	assertErrorContains(t, migrateErr, "transaction control")
	var tableExists bool
	if err := admin.QueryRow(ctx, `SELECT to_regclass('app.must_not_commit') IS NOT NULL`).
		Scan(&tableExists); err != nil {
		t.Fatalf("check transaction-control side effect: %v", err)
	}
	if tableExists {
		t.Fatal("rejected transaction-control migration committed a table")
	}
}

func runStandardConformingStringsLocal(t *testing.T, adminURL string) {
	t.Helper()
	admin := connect(t, adminURL)
	defer func() { _ = admin.Close(context.Background()) }()
	resetDatabase(t, admin)
	ctx := context.Background()
	migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = migrator.Close(ctx) }()

	if _, err := migrator.Exec(ctx, `SET standard_conforming_strings = off`); err != nil {
		t.Fatalf("disable standard conforming strings for probe: %v", err)
	}
	files := fstest.MapFS{
		"000001_strings.sql": {Data: []byte(`
			CREATE TABLE app.string_setting_probe (
				value text NOT NULL CHECK (octet_length(value) = 2)
			);
			INSERT INTO app.string_setting_probe (value) VALUES ('\n');
		`)},
	}
	if err := Migrate(ctx, migrator, Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: files}); err != nil {
		t.Fatalf("migrate with nonstandard session string setting: %v", err)
	}
	var value, setting string
	if err := admin.QueryRow(ctx, `SELECT value FROM app.string_setting_probe`).Scan(&value); err != nil {
		t.Fatalf("read string setting probe: %v", err)
	}
	if err := migrator.QueryRow(ctx, `SHOW standard_conforming_strings`).Scan(&setting); err != nil {
		t.Fatalf("read post-migration string setting: %v", err)
	}
	if value != `\n` || setting != "off" {
		t.Fatalf("string probe value/setting = %q/%q, want %q/off", value, setting, `\n`)
	}
}

func runUnknownAppliedMigrationFailsClosed(t *testing.T, adminURL string) {
	t.Helper()
	admin := connect(t, adminURL)
	defer func() { _ = admin.Close(context.Background()) }()
	resetDatabase(t, admin)
	ctx := context.Background()
	migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = migrator.Close(ctx) }()

	files := fstest.MapFS{"000001_known.sql": {Data: []byte("SELECT 1;\n")}}
	if err := Migrate(ctx, migrator, Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: files}); err != nil {
		t.Fatalf("apply known migration: %v", err)
	}
	if _, err := admin.Exec(ctx, `
		INSERT INTO pgmig_meta.schema_migrations (version, name, checksum)
		VALUES (2, 'unknown', decode(repeat('10', 32), 'hex'))
	`); err != nil {
		t.Fatalf("insert unknown migration probe: %v", err)
	}
	err := Migrate(ctx, migrator, Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: files})
	assertErrorContains(t, err, "unknown migration 000002")
}

func runLockWaitHonorsCancellation(t *testing.T, adminURL string) {
	t.Helper()
	admin := connect(t, adminURL)
	defer func() { _ = admin.Close(context.Background()) }()
	resetDatabase(t, admin)
	ctx := context.Background()

	if _, err := admin.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		t.Fatalf("acquire cancellation probe lock: %v", err)
	}
	migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = migrator.Close(ctx) }()
	cancelCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	files := fstest.MapFS{"000001_cancel.sql": {Data: []byte("SELECT 1;\n")}}
	cancelErr := Migrate(cancelCtx, migrator, Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: files})
	if cancelErr == nil || !errors.Is(cancelErr, context.DeadlineExceeded) {
		t.Fatalf("canceled migration error = %v", cancelErr)
	}
	var unlocked bool
	if err := admin.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, migrationLockKey).
		Scan(&unlocked); err != nil ||
		!unlocked {
		t.Fatalf("release cancellation probe lock: unlocked=%t err=%v", unlocked, err)
	}
	if !migrator.IsClosed() {
		t.Fatal("pgx did not fail closed by closing the canceled dedicated connection")
	}
	replacement := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = replacement.Close(ctx) }()
	if err := Migrate(
		ctx,
		replacement,
		Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: files},
	); err != nil {
		t.Fatalf("new dedicated connection could not migrate after cancellation: %v", err)
	}
}

func runConcurrentRunnersUseOneAdvisoryLock(t *testing.T, adminURL string) {
	t.Helper()
	admin := connect(t, adminURL)
	defer func() { _ = admin.Close(context.Background()) }()
	resetDatabase(t, admin)
	ctx := context.Background()
	if _, err := admin.Exec(ctx, `
		SET ROLE `+testOwnerRole+`;
		CREATE TABLE app.migration_gate (id integer PRIMARY KEY);
		RESET ROLE;
	`); err != nil {
		t.Fatalf("create migration gate: %v", err)
	}
	blocker, beginErr := admin.Begin(ctx)
	if beginErr != nil {
		t.Fatalf("begin migration blocker: %v", beginErr)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	if _, err := blocker.Exec(ctx, `LOCK TABLE app.migration_gate IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock migration gate: %v", err)
	}

	first := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = first.Close(ctx) }()
	second := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = second.Close(ctx) }()
	var firstPID, secondPID int32
	if err := first.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&firstPID); err != nil {
		t.Fatalf("read first runner PID: %v", err)
	}
	if err := second.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&secondPID); err != nil {
		t.Fatalf("read second runner PID: %v", err)
	}
	files := fstest.MapFS{
		"000001_gate.sql": {
			Data: []byte(
				"LOCK TABLE app.migration_gate IN ACCESS EXCLUSIVE MODE; INSERT INTO app.migration_gate VALUES (1);\n",
			),
		},
	}
	config := Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: files}
	firstDone := make(chan error, 1)
	go func() { firstDone <- Migrate(ctx, first, config) }()
	waitForAdvisoryLock(t, admin, firstPID, true)

	secondDone := make(chan error, 1)
	go func() { secondDone <- Migrate(ctx, second, config) }()
	waitForAdvisoryLock(t, admin, secondPID, false)
	select {
	case err := <-secondDone:
		t.Fatalf("second runner was not serialized: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := blocker.Commit(ctx); err != nil {
		t.Fatalf("release migration gate: %v", err)
	}
	for number, done := range []<-chan error{firstDone, secondDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("runner %d: %v", number+1, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("runner %d did not finish", number+1)
		}
	}
	var rows int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM app.migration_gate`).Scan(&rows); err != nil {
		t.Fatalf("count migration side effects: %v", err)
	}
	if rows != 1 {
		t.Fatalf("migration side effects = %d rows, want 1", rows)
	}
	assertHistory(t, admin, 1)
}

func runLockTimeoutBoundsLockWait(t *testing.T, adminURL string) {
	t.Helper()
	admin := connect(t, adminURL)
	defer func() { _ = admin.Close(context.Background()) }()
	resetDatabase(t, admin)
	ctx := context.Background()

	if _, err := admin.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		t.Fatalf("acquire lock timeout probe lock: %v", err)
	}
	probeLockHeld := true
	defer func() {
		if !probeLockHeld {
			return
		}
		var released bool
		if err := admin.QueryRow(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockKey).
			Scan(&released); err != nil || !released {
			t.Errorf("release lock timeout probe lock: released=%t err=%v", released, err)
		}
	}()

	migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = migrator.Close(ctx) }()
	files := fstest.MapFS{"000001_lock_timeout.sql": {Data: []byte("SELECT 1;\n")}}
	type outcome struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan outcome, 1)
	started := time.Now()
	go func() {
		err := Migrate(ctx, migrator, Config{
			Role:        testOwnerRole,
			AppSchema:   testAppSchema,
			Files:       files,
			LockTimeout: 300 * time.Millisecond,
		})
		done <- outcome{err: err, elapsed: time.Since(started)}
	}()
	select {
	case got := <-done:
		if !errors.Is(got.err, ErrLockTimeout) {
			t.Fatalf("lock timeout error = %v", got.err)
		}
		if got.elapsed < 200*time.Millisecond || got.elapsed > 5*time.Second {
			t.Fatalf("lock timeout returned after %s", got.elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not honor the lock timeout")
	}
	if migrator.IsClosed() {
		t.Fatal("bounded lock wait closed the migration connection")
	}

	if _, err := admin.Exec(ctx, `SELECT pg_advisory_unlock($1)`, migrationLockKey); err != nil {
		t.Fatalf("release lock timeout probe lock: %v", err)
	}
	probeLockHeld = false
	if err := Migrate(ctx, migrator, Config{
		Role:        testOwnerRole,
		AppSchema:   testAppSchema,
		Files:       files,
		LockTimeout: time.Second,
	}); err != nil {
		t.Fatalf("migration after the released probe lock: %v", err)
	}
	assertHistory(t, admin, 1)
}

func runStatementTimeoutBoundsMigrations(t *testing.T, adminURL string) {
	t.Helper()
	admin := connect(t, adminURL)
	defer func() { _ = admin.Close(context.Background()) }()
	resetDatabase(t, admin)
	ctx := context.Background()
	migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = migrator.Close(ctx) }()

	var sessionTimeoutBefore string
	if err := migrator.QueryRow(ctx, `SHOW statement_timeout`).Scan(&sessionTimeoutBefore); err != nil {
		t.Fatalf("read initial session statement timeout: %v", err)
	}
	slow := fstest.MapFS{"000001_slow.sql": {Data: []byte("SELECT pg_sleep(2);\n")}}
	slowErr := Migrate(ctx, migrator, Config{
		Role:             testOwnerRole,
		AppSchema:        testAppSchema,
		Files:            slow,
		StatementTimeout: 200 * time.Millisecond,
	})
	assertErrorContains(t, slowErr, "statement timeout")
	assertHistory(t, admin, 0)

	overridden := fstest.MapFS{
		"000001_slow.sql": {Data: []byte("SET LOCAL statement_timeout = '5s';\nSELECT pg_sleep(0.5);\n")},
	}
	if err := Migrate(ctx, migrator, Config{
		Role:             testOwnerRole,
		AppSchema:        testAppSchema,
		Files:            overridden,
		StatementTimeout: 200 * time.Millisecond,
	}); err != nil {
		t.Fatalf("migration with its own SET LOCAL failed: %v", err)
	}
	assertHistory(t, admin, 1)

	var sessionTimeoutAfter string
	if err := migrator.QueryRow(ctx, `SHOW statement_timeout`).Scan(&sessionTimeoutAfter); err != nil {
		t.Fatalf("read final session statement timeout: %v", err)
	}
	if sessionTimeoutAfter != sessionTimeoutBefore {
		t.Fatalf(
			"statement timeout leaked into the session: %q, want %q",
			sessionTimeoutAfter,
			sessionTimeoutBefore,
		)
	}
}

func runStatementTimeoutBoundsMetadataTransaction(t *testing.T, adminURL string) {
	t.Helper()
	admin := connect(t, adminURL)
	defer func() { _ = admin.Close(context.Background()) }()
	resetDatabase(t, admin)
	ctx := context.Background()
	migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
	defer func() { _ = migrator.Close(ctx) }()

	alpha := fstest.MapFS{"000001_alpha.sql": {Data: []byte("SELECT 1;\n")}}
	if err := Migrate(ctx, migrator, Config{
		Role:      testOwnerRole,
		AppSchema: testAppSchema,
		Files:     alpha,
	}); err != nil {
		t.Fatalf("apply metadata probe migration: %v", err)
	}
	blocker, beginErr := admin.Begin(ctx)
	if beginErr != nil {
		t.Fatalf("begin metadata blocker: %v", beginErr)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	if _, err := blocker.Exec(ctx, `LOCK TABLE pgmig_meta.schema_migrations IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock migration metadata: %v", err)
	}
	next := fstest.MapFS{
		"000001_alpha.sql":  {Data: []byte("SELECT 1;\n")},
		"000002_second.sql": {Data: []byte("CREATE TABLE app.second (id integer);\n")},
	}
	metadataErr := Migrate(ctx, migrator, Config{
		Role:             testOwnerRole,
		AppSchema:        testAppSchema,
		Files:            next,
		StatementTimeout: 200 * time.Millisecond,
	})
	assertErrorContains(t, metadataErr, "statement timeout")
	assertHistory(t, admin, 1)
	var sideEffect bool
	if err := admin.QueryRow(ctx, `SELECT to_regclass('app.second') IS NOT NULL`).Scan(&sideEffect); err != nil {
		t.Fatalf("check metadata timeout side effect: %v", err)
	}
	if sideEffect {
		t.Fatal("blocked metadata transaction applied a migration")
	}
}

func runMigrationIdentityLeastPrivileged(t *testing.T, adminURL string) {
	t.Helper()
	files := fstest.MapFS{"000001_identity.sql": {Data: []byte("SELECT 1;\n")}}
	alterMigrator := func(statement string) func(t *testing.T, admin *pgx.Conn) {
		return func(t *testing.T, admin *pgx.Conn) {
			t.Helper()
			if _, err := admin.Exec(context.Background(), statement); err != nil {
				t.Fatalf("make migrator overprivileged: %v", err)
			}
		}
	}
	for _, testCase := range []struct {
		name          string
		prepare       func(t *testing.T, admin *pgx.Conn)
		prepareActive func(t *testing.T, candidate *pgx.Conn)
		url           func() string
	}{
		{
			name: "admin superuser",
			url:  func() string { return adminURL },
		},
		{
			name: "direct owner login",
			prepare: func(t *testing.T, admin *pgx.Conn) {
				t.Helper()
				if _, err := admin.Exec(
					context.Background(),
					`ALTER ROLE `+testOwnerRole+` LOGIN PASSWORD '`+testOwnerPassword+`'`,
				); err != nil {
					t.Fatalf("enable direct owner login probe: %v", err)
				}
			},
			url: func() string {
				return databaseURLForRole(t, adminURL, testOwnerRole, testOwnerPassword)
			},
		},
		{
			name:    "migrator SUPERUSER",
			prepare: alterMigrator(`ALTER ROLE ` + testMigratorRole + ` SUPERUSER`),
			url:     func() string { return databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword) },
		},
		{
			name: "migrator database CREATE",
			prepare: func(t *testing.T, admin *pgx.Conn) {
				t.Helper()
				if _, err := admin.Exec(
					context.Background(),
					`GRANT CREATE ON DATABASE `+databaseName(t, admin)+` TO `+testMigratorRole,
				); err != nil {
					t.Fatalf("make migrator overprivileged: %v", err)
				}
			},
			url: func() string { return databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword) },
		},
		{
			name: "migrator extra inbound role",
			prepare: alterMigrator(`
				DO $role$
				BEGIN
					IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pgmig_extra_migration_role') THEN
						CREATE ROLE pgmig_extra_migration_role NOLOGIN;
					END IF;
				END
				$role$;
				GRANT pgmig_extra_migration_role TO ` + testMigratorRole + ` WITH ADMIN FALSE, INHERIT FALSE, SET TRUE
			`),
			url: func() string { return databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword) },
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			admin := connect(t, adminURL)
			defer func() { _ = admin.Close(context.Background()) }()
			resetDatabase(t, admin)
			ctx := context.Background()
			if testCase.prepare != nil {
				testCase.prepare(t, admin)
			}
			candidate := connect(t, testCase.url())
			defer func() { _ = candidate.Close(ctx) }()
			if testCase.prepareActive != nil {
				testCase.prepareActive(t, candidate)
			}
			err := Migrate(ctx, candidate, Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: files})
			assertErrorContains(t, err, "migration identity")
		})
	}
}

func runMetadataACLTamperingFailsClosed(t *testing.T, adminURL string) {
	t.Helper()
	files := fstest.MapFS{"000001_acl.sql": {Data: []byte("SELECT 1;\n")}}
	for _, testCase := range []struct {
		name   string
		tamper string
	}{
		{
			name:   "named metadata schema grant",
			tamper: `GRANT USAGE ON SCHEMA pgmig_meta TO ` + testAppRole,
		},
		{
			name:   "PUBLIC metadata table grant",
			tamper: `GRANT SELECT ON pgmig_meta.schema_migrations TO PUBLIC`,
		},
		{
			name:   "named metadata column grant",
			tamper: `GRANT SELECT (version) ON pgmig_meta.schema_migrations TO ` + testAppRole,
		},
		{
			name:   "wrong metadata table owner",
			tamper: `ALTER TABLE pgmig_meta.schema_migrations OWNER TO ` + testAppRole,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			admin := connect(t, adminURL)
			defer func() { _ = admin.Close(context.Background()) }()
			resetDatabase(t, admin)
			ctx := context.Background()
			migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
			defer func() { _ = migrator.Close(ctx) }()
			config := Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: files}
			if err := Migrate(ctx, migrator, config); err != nil {
				t.Fatalf("apply ACL probe migration: %v", err)
			}
			if _, err := admin.Exec(ctx, testCase.tamper); err != nil {
				t.Fatalf("tamper migration metadata: %v", err)
			}
			if err := Migrate(ctx, migrator, config); err == nil {
				t.Fatal("migration accepted tampered metadata ownership or ACL")
			}
		})
	}
}

func runAppSchemaACLTamperingFailsClosed(t *testing.T, adminURL string) {
	t.Helper()
	files := fstest.MapFS{"000001_app_acl.sql": {Data: []byte("SELECT 1;\n")}}
	for _, testCase := range []struct {
		name   string
		tamper string
	}{
		{
			name:   "missing runtime grant",
			tamper: `REVOKE USAGE ON SCHEMA app FROM ` + testAppRole,
		},
		{
			name:   "PUBLIC app grant",
			tamper: `GRANT USAGE ON SCHEMA app TO PUBLIC`,
		},
		{
			name:   "wrong app owner",
			tamper: `ALTER SCHEMA app OWNER TO ` + testAppRole,
		},
		{
			name: "extra runtime role",
			tamper: `
				DO $role$
				BEGIN
					IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pgmig_extra_runtime_role') THEN
						CREATE ROLE pgmig_extra_runtime_role LOGIN;
					END IF;
				END
				$role$;
				ALTER ROLE pgmig_extra_runtime_role LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
				GRANT USAGE ON SCHEMA app TO pgmig_extra_runtime_role
			`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			admin := connect(t, adminURL)
			defer func() { _ = admin.Close(context.Background()) }()
			resetDatabase(t, admin)
			ctx := context.Background()
			migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
			defer func() { _ = migrator.Close(ctx) }()
			config := Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: files}
			if err := Migrate(ctx, migrator, config); err != nil {
				t.Fatalf("apply app ACL probe migration: %v", err)
			}
			if _, err := admin.Exec(ctx, testCase.tamper); err != nil {
				t.Fatalf("tamper app schema ACL: %v", err)
			}
			if err := Migrate(ctx, migrator, config); err == nil {
				t.Fatal("migration accepted a tampered app schema ACL")
			}
		})
	}
}

func runDatabaseACLTamperingFailsClosed(t *testing.T, adminURL string) {
	t.Helper()
	first := fstest.MapFS{"000001_db_acl.sql": {Data: []byte("SELECT 1;\n")}}
	withSideEffect := fstest.MapFS{
		"000001_db_acl.sql":      {Data: []byte("SELECT 1;\n")},
		"000002_side_effect.sql": {Data: []byte("CREATE TABLE app.database_acl_must_not_apply (id integer);\n")},
	}
	for _, testCase := range []struct {
		name   string
		tamper func(t *testing.T, admin *pgx.Conn)
	}{
		{
			name: "PUBLIC CONNECT",
			tamper: func(t *testing.T, admin *pgx.Conn) {
				t.Helper()
				if _, err := admin.Exec(
					context.Background(),
					`GRANT CONNECT ON DATABASE `+databaseName(t, admin)+` TO PUBLIC`,
				); err != nil {
					t.Fatalf("tamper database ACL: %v", err)
				}
			},
		},
		{
			name: "extra named role CONNECT",
			tamper: func(t *testing.T, admin *pgx.Conn) {
				t.Helper()
				if _, err := admin.Exec(context.Background(), `
					DO $role$
					BEGIN
						IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pgmig_extra_database_role') THEN
							CREATE ROLE pgmig_extra_database_role LOGIN;
						END IF;
					END
					$role$;
					GRANT CONNECT ON DATABASE `+databaseName(t, admin)+` TO pgmig_extra_database_role
				`); err != nil {
					t.Fatalf("tamper database ACL: %v", err)
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			admin := connect(t, adminURL)
			defer func() { _ = admin.Close(context.Background()) }()
			resetDatabase(t, admin)
			ctx := context.Background()
			migrator := connect(t, databaseURLForRole(t, adminURL, testMigratorRole, testMigratorPassword))
			defer func() { _ = migrator.Close(ctx) }()
			if err := Migrate(
				ctx,
				migrator,
				Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: first},
			); err != nil {
				t.Fatalf("apply database ACL probe migration: %v", err)
			}
			testCase.tamper(t, admin)
			migrateErr := Migrate(
				ctx,
				migrator,
				Config{Role: testOwnerRole, AppSchema: testAppSchema, Files: withSideEffect},
			)
			assertErrorContains(t, migrateErr, "database ACL")
			var sideEffect bool
			if err := admin.QueryRow(ctx, `SELECT to_regclass('app.database_acl_must_not_apply') IS NOT NULL`).
				Scan(&sideEffect); err != nil {
				t.Fatalf("check database ACL side effect: %v", err)
			}
			if sideEffect {
				t.Fatal("invalid database ACL allowed migration side effect")
			}
			assertHistory(t, admin, 1)
		})
	}
}

func resetDatabase(t *testing.T, admin *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	database := databaseName(t, admin)
	statements := []string{
		`DROP SCHEMA IF EXISTS pgmig_meta CASCADE`,
		`DROP SCHEMA IF EXISTS custom_meta CASCADE`,
		`DROP SCHEMA IF EXISTS app CASCADE`,
		`DO $roles$
		BEGIN
			IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '` + testOwnerRole + `') THEN
				CREATE ROLE ` + testOwnerRole + ` NOLOGIN;
			END IF;
			IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '` + testMigratorRole + `') THEN
				CREATE ROLE ` + testMigratorRole + ` LOGIN;
			END IF;
			IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '` + testAppRole + `') THEN
				CREATE ROLE ` + testAppRole + ` LOGIN;
			END IF;
		END
		$roles$`,
		`DO $memberships$
		DECLARE
			membership_entry record;
		BEGIN
			FOR membership_entry IN
				SELECT granted.rolname
				FROM pg_auth_members auth_member
				JOIN pg_roles granted ON granted.oid = auth_member.roleid
				WHERE auth_member.member = '` + testMigratorRole + `'::regrole
				  AND auth_member.roleid <> '` + testOwnerRole + `'::regrole
			LOOP
				EXECUTE format('REVOKE %I FROM ` + testMigratorRole + `', membership_entry.rolname);
			END LOOP;
			FOR membership_entry IN
				SELECT member_role.rolname
				FROM pg_auth_members auth_member
				JOIN pg_roles member_role ON member_role.oid = auth_member.member
				WHERE auth_member.roleid = '` + testMigratorRole + `'::regrole
			LOOP
				EXECUTE format('REVOKE ` + testMigratorRole + ` FROM %I', membership_entry.rolname);
			END LOOP;
			FOR membership_entry IN
				SELECT granted.rolname
				FROM pg_auth_members auth_member
				JOIN pg_roles granted ON granted.oid = auth_member.roleid
				WHERE auth_member.member = '` + testAppRole + `'::regrole
			LOOP
				EXECUTE format('REVOKE %I FROM ` + testAppRole + `', membership_entry.rolname);
			END LOOP;
			FOR membership_entry IN
				SELECT member_role.rolname
				FROM pg_auth_members auth_member
				JOIN pg_roles member_role ON member_role.oid = auth_member.member
				WHERE auth_member.roleid = '` + testAppRole + `'::regrole
			LOOP
				EXECUTE format('REVOKE ` + testAppRole + ` FROM %I', membership_entry.rolname);
			END LOOP;
		END
		$memberships$`,
		`ALTER ROLE ` + testOwnerRole + ` NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD NULL`,
		`ALTER ROLE ` + testMigratorRole + ` LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '` + testMigratorPassword + `'`,
		`ALTER ROLE ` + testAppRole + ` LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'pgmig_test_app_pw'`,
		`GRANT ` + testOwnerRole + ` TO ` + testMigratorRole + ` WITH ADMIN FALSE, INHERIT FALSE, SET TRUE`,
		`ALTER DATABASE ` + database + ` OWNER TO ` + testOwnerRole,
		`UPDATE pg_database SET datacl = NULL WHERE datname = current_database()`,
		`REVOKE ALL PRIVILEGES ON DATABASE ` + database + ` FROM PUBLIC`,
		`SET ROLE ` + testOwnerRole,
		`GRANT CONNECT ON DATABASE ` + database + ` TO ` + testMigratorRole + `, ` + testAppRole,
		`RESET ROLE`,
		`REVOKE CREATE ON SCHEMA public FROM PUBLIC`,
		`CREATE SCHEMA app AUTHORIZATION ` + testOwnerRole,
		`REVOKE ALL ON SCHEMA app FROM PUBLIC, ` + testMigratorRole + `, ` + testAppRole,
		`GRANT USAGE ON SCHEMA app TO ` + testAppRole,
		`ALTER DEFAULT PRIVILEGES FOR ROLE ` + testOwnerRole + ` IN SCHEMA app REVOKE ALL ON TABLES FROM PUBLIC, ` + testMigratorRole + `, ` + testAppRole,
		`ALTER DEFAULT PRIVILEGES FOR ROLE ` + testOwnerRole + ` IN SCHEMA app GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ` + testAppRole,
		`ALTER DEFAULT PRIVILEGES FOR ROLE ` + testOwnerRole + ` IN SCHEMA app REVOKE ALL ON SEQUENCES FROM PUBLIC, ` + testMigratorRole + `, ` + testAppRole,
		`ALTER DEFAULT PRIVILEGES FOR ROLE ` + testOwnerRole + ` IN SCHEMA app GRANT USAGE ON SEQUENCES TO ` + testAppRole,
	}
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("reset isolated database: %v", err)
		}
	}
}

func databaseName(t *testing.T, admin *pgx.Conn) string {
	t.Helper()
	var name string
	if err := admin.QueryRow(context.Background(), `SELECT current_database()`).Scan(&name); err != nil {
		t.Fatalf("read test database name: %v", err)
	}
	return pgx.Identifier{name}.Sanitize()
}

func databaseURLForRole(t *testing.T, base, username, password string) string {
	t.Helper()
	parsed, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	parsed.User = url.UserPassword(username, password)
	return parsed.String()
}

func connect(t *testing.T, databaseURL string) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect isolated database: %v", err)
	}
	return conn
}

func assertHistory(t *testing.T, admin *pgx.Conn, want int64) {
	t.Helper()
	var count, maximum int64
	if err := admin.QueryRow(context.Background(), `
		SELECT count(*), COALESCE(max(version), 0)
		FROM pgmig_meta.schema_migrations
	`).Scan(&count, &maximum); err != nil {
		t.Fatalf("read migration history: %v", err)
	}
	if count != want || maximum != want {
		t.Fatalf("migration history count/max = %d/%d, want %d/%d", count, maximum, want, want)
	}
}

func assertErrorContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

func waitForAdvisoryLock(t *testing.T, admin *pgx.Conn, pid int32, granted bool) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var found bool
		if err := admin.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT FROM pg_locks
				WHERE pid = $1 AND locktype = 'advisory' AND granted = $2
			)
		`, pid, granted).Scan(&found); err != nil {
			t.Fatalf("inspect advisory lock: %v", err)
		}
		if found {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("backend %d did not expose advisory lock granted=%t", pid, granted)
}
