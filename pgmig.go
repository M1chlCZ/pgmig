package pgmig

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	migrationLockKey             int64 = 0x70676d69675f7631
	migrationFilenameParts             = 2
	migrationStatementTokenLimit       = 2
	sqlCommentDelimiterSize            = 2
	migrationCleanupTimeout            = 5 * time.Second
)

const migrationIdentifierPatternText = `^[a-z_][a-z0-9_]{0,62}$`

var (
	migrationRolePattern   = regexp.MustCompile(migrationIdentifierPatternText)
	migrationSchemaPattern = regexp.MustCompile(migrationIdentifierPatternText)
	migrationNamePattern   = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

// DefaultMetaSchema is the schema that stores migration history.
const DefaultMetaSchema = "pgmig_meta"

// Config controls one migration run. Role is required. Files supplies the
// migration set. MetaSchema defaults to DefaultMetaSchema. When AppSchema is
// set, Migrate verifies the schema ownership, the single runtime role, and
// the exact database ACL before it applies anything.
type Config struct {
	Role       string
	AppSchema  string
	MetaSchema string
	Files      fs.FS
}

// Migrate applies every pending migration on the connection.
func Migrate(ctx context.Context, conn *pgx.Conn, config Config) error {
	return migrateFS(ctx, conn, config)
}

// Run opens a dedicated connection, applies migrations, and closes the
// connection.
func Run(ctx context.Context, databaseURL string, config Config) (returnErr error) {
	if databaseURL == "" {
		return errors.New("database URL is required")
	}
	parsed, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return errors.New("database URL is invalid")
	}
	conn, err := pgx.ConnectConfig(ctx, parsed)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("database connection canceled: %w", ctx.Err())
		}
		return errors.New("database URL is unavailable")
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), migrationCleanupTimeout)
		defer cancel()
		if closeErr := conn.Close(closeCtx); closeErr != nil && returnErr == nil {
			returnErr = errors.New("close migration connection")
		}
	}()
	return Migrate(ctx, conn, config)
}

// MigrationState names the state of one migration version.
type MigrationState string

const (
	// StateApplied means the version is recorded and matches the file.
	StateApplied MigrationState = "applied"
	// StatePending means the file exists and the version is not recorded.
	StatePending MigrationState = "pending"
	// StateDrift means the version is recorded but the file name or content changed.
	StateDrift MigrationState = "drift"
	// StateUnknown means the version is recorded and the file is missing.
	StateUnknown MigrationState = "unknown"
)

// MigrationStatus describes one migration version from the files and the
// recorded history. AppliedAt is zero unless the version is recorded. Checksum
// is zero for unknown versions.
type MigrationStatus struct {
	Version   int64
	Name      string
	State     MigrationState
	AppliedAt time.Time
	Checksum  [sha256.Size]byte
}

// PlannedMigration is one migration that a run applies.
type PlannedMigration struct {
	Version int64
	Name    string
}

// Status reports the state of every migration version in the history and the
// file set. It uses the same least-privilege session requirements as Migrate
// and does not apply migrations.
func Status(ctx context.Context, conn *pgx.Conn, config Config) ([]MigrationStatus, error) {
	var statuses []MigrationStatus
	err := withMigrationSession(ctx, conn, config, func(
		_ migrationPlan,
		migrations []migration,
		applied []appliedMigration,
	) error {
		statuses = buildMigrationStatuses(migrations, applied)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return statuses, nil
}

// Verify checks the recorded history against the migration files. It returns
// an error on checksum or name drift and on recorded versions without a file.
func Verify(ctx context.Context, conn *pgx.Conn, config Config) error {
	return withMigrationSession(ctx, conn, config, func(
		_ migrationPlan,
		migrations []migration,
		applied []appliedMigration,
	) error {
		return verifyMigrationHistory(applied, migrations)
	})
}

// Plan returns the migrations that Migrate applies, without applying
// them. It fails on drift or unknown recorded versions, like Migrate.
func Plan(ctx context.Context, conn *pgx.Conn, config Config) ([]PlannedMigration, error) {
	var planned []PlannedMigration
	err := withMigrationSession(ctx, conn, config, func(
		_ migrationPlan,
		migrations []migration,
		applied []appliedMigration,
	) error {
		if err := verifyMigrationHistory(applied, migrations); err != nil {
			return err
		}
		for _, migration := range migrations[len(applied):] {
			planned = append(planned, PlannedMigration{Version: migration.Version, Name: migration.Name})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return planned, nil
}

func buildMigrationStatuses(migrations []migration, applied []appliedMigration) []MigrationStatus {
	byVersion := make(map[int64]appliedMigration, len(applied))
	for _, recorded := range applied {
		byVersion[recorded.Version] = recorded
	}
	statuses := make([]MigrationStatus, 0, len(migrations)+len(applied))
	seen := make(map[int64]struct{}, len(migrations))
	for _, file := range migrations {
		seen[file.Version] = struct{}{}
		status := MigrationStatus{Version: file.Version, Name: file.Name, Checksum: file.Checksum}
		recorded, exists := byVersion[file.Version]
		switch {
		case !exists:
			status.State = StatePending
		case recorded.Name == file.Name &&
			len(recorded.Checksum) == sha256.Size &&
			bytes.Equal(recorded.Checksum, file.Checksum[:]):
			status.State = StateApplied
			status.AppliedAt = recorded.AppliedAt
		default:
			status.State = StateDrift
			status.AppliedAt = recorded.AppliedAt
		}
		statuses = append(statuses, status)
	}
	for _, recorded := range applied {
		if _, exists := seen[recorded.Version]; exists {
			continue
		}
		statuses = append(statuses, MigrationStatus{
			Version:   recorded.Version,
			Name:      recorded.Name,
			State:     StateUnknown,
			AppliedAt: recorded.AppliedAt,
		})
	}
	slices.SortFunc(statuses, func(first, second MigrationStatus) int {
		return cmp.Compare(first.Version, second.Version)
	})
	return statuses
}

func migrationSchemaIdentifier(name, fallback string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = fallback
	}
	if !migrationSchemaPattern.MatchString(name) {
		return "", errors.New("schema name must be a lowercase PostgreSQL identifier")
	}
	return name, nil
}

type migrationPlan struct {
	role           string
	roleIdentifier string
	metaSchema     string
	metaTable      string
	appSchema      string
}

func newMigrationPlan(config Config) (migrationPlan, error) {
	role := strings.TrimSpace(config.Role)
	roleIdentifier, err := migrationRoleIdentifier(role)
	if err != nil {
		return migrationPlan{}, err
	}
	metaSchema, err := migrationSchemaIdentifier(config.MetaSchema, DefaultMetaSchema)
	if err != nil {
		return migrationPlan{}, err
	}
	appSchema := strings.TrimSpace(config.AppSchema)
	if appSchema != "" && !migrationSchemaPattern.MatchString(appSchema) {
		return migrationPlan{}, errors.New("app schema must be a lowercase PostgreSQL identifier")
	}
	return migrationPlan{
		role:           role,
		roleIdentifier: roleIdentifier,
		metaSchema:     metaSchema,
		metaTable:      pgx.Identifier{metaSchema}.Sanitize() + ".schema_migrations",
		appSchema:      appSchema,
	}, nil
}

type migration struct {
	Version  int64
	Name     string
	SQL      string
	Checksum [sha256.Size]byte
}

func loadMigrations(files fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}

	migrations := make([]migration, 0, len(entries))
	versions := make(map[int64]struct{}, len(entries))
	for _, entry := range entries {
		loaded, loadErr := loadMigrationEntry(files, entry)
		if loadErr != nil {
			return nil, loadErr
		}
		if _, exists := versions[loaded.Version]; exists {
			return nil, fmt.Errorf("duplicate migration version %06d", loaded.Version)
		}
		versions[loaded.Version] = struct{}{}
		migrations = append(migrations, loaded)
	}
	slices.SortFunc(migrations, func(first, second migration) int {
		return cmp.Compare(first.Version, second.Version)
	})
	if validateMigrationSequenceErr := validateMigrationSequence(migrations); validateMigrationSequenceErr != nil {
		return nil, validateMigrationSequenceErr
	}
	return migrations, nil
}

func loadMigrationEntry(files fs.FS, entry fs.DirEntry) (migration, error) {
	if entry.IsDir() {
		return migration{}, fmt.Errorf("migration directory %q is not allowed", entry.Name())
	}
	name := entry.Name()
	if path.Ext(name) != ".sql" {
		return migration{}, fmt.Errorf("invalid migration filename %q", name)
	}
	base := strings.TrimSuffix(name, ".sql")
	parts := strings.SplitN(base, "_", migrationFilenameParts)
	if len(parts) != migrationFilenameParts || len(parts[0]) != 6 || parts[0] == "000000" ||
		!migrationNamePattern.MatchString(parts[1]) {
		return migration{}, fmt.Errorf("invalid migration filename %q", name)
	}
	version, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return migration{}, fmt.Errorf("invalid migration filename %q", name)
	}
	contents, err := fs.ReadFile(files, name)
	if err != nil {
		return migration{}, fmt.Errorf("read migration %q: %w", name, err)
	}
	if len(strings.TrimSpace(string(contents))) == 0 {
		return migration{}, fmt.Errorf("migration %q is empty", name)
	}
	if validateMigrationSQLErr := validateMigrationSQL(string(contents)); validateMigrationSQLErr != nil {
		return migration{}, fmt.Errorf("migration %q: %w", name, validateMigrationSQLErr)
	}
	return migration{
		Version: version, Name: parts[1], SQL: string(contents), Checksum: sha256.Sum256(contents),
	}, nil
}

func validateMigrationSequence(migrations []migration) error {
	if len(migrations) == 0 {
		return errors.New("no migrations found")
	}
	for index, migration := range migrations {
		if migration.Version != int64(index+1) {
			return fmt.Errorf("migration version gap before %06d", migration.Version)
		}
	}
	return nil
}

func validateMigrationSQL(sql string) error {
	var statementTokens []string
	for index := 0; index < len(sql); {
		next, err := scanMigrationSQLToken(sql, index, &statementTokens)
		if err != nil {
			return err
		}
		index = next
	}
	return nil
}

func scanMigrationSQLToken(sql string, index int, statementTokens *[]string) (int, error) {
	switch {
	case index == 0 && strings.HasPrefix(sql, "\ufeff"):
		return index + len("\ufeff"), nil
	case isSQLSpace(sql[index]):
		return index + 1, nil
	case sql[index] == ';':
		*statementTokens = (*statementTokens)[:0]
		return index + 1, nil
	case index+1 < len(sql) && sql[index:index+2] == "--":
		return skipSQLLineComment(sql, index), nil
	case index+1 < len(sql) && sql[index:index+2] == "/*":
		return skipSQLBlockComment(sql, index)
	case sql[index] == '\'':
		next, err := skipSQLSingleQuote(sql, index, isEscapeStringPrefix(sql, index))
		appendMigrationSQLToken(statementTokens, "<VALUE>")
		return next, err
	case sql[index] == '"':
		next, err := skipSQLDoubleQuote(sql, index)
		appendMigrationSQLToken(statementTokens, "<IDENTIFIER>")
		return next, err
	case sql[index] == '$':
		return scanMigrationSQLDollarQuote(sql, index, statementTokens)
	case isSQLIdentifierStart(sql[index]):
		end := sqlIdentifierEnd(sql, index)
		return end, markMigrationSQLToken(statementTokens, sql[index:end])
	default:
		appendMigrationSQLToken(statementTokens, "<SYMBOL>")
		return index + 1, nil
	}
}

func skipSQLLineComment(sql string, start int) int {
	index := start + sqlCommentDelimiterSize
	for index < len(sql) && sql[index] != '\n' && sql[index] != '\r' {
		index++
	}
	return index
}

func scanMigrationSQLDollarQuote(
	sql string,
	index int,
	statementTokens *[]string,
) (int, error) {
	delimiter, bodyStart := sqlDollarDelimiter(sql, index)
	if delimiter == "" {
		appendMigrationSQLToken(statementTokens, "<SYMBOL>")
		return index + 1, nil
	}
	bodyEnd := strings.Index(sql[bodyStart:], delimiter)
	if bodyEnd < 0 {
		return 0, errors.New("unterminated dollar-quoted migration text")
	}
	appendMigrationSQLToken(statementTokens, "<VALUE>")
	return bodyStart + bodyEnd + len(delimiter), nil
}

func sqlIdentifierEnd(sql string, start int) int {
	end := start + 1
	for end < len(sql) && isSQLIdentifierContinue(sql[end]) {
		end++
	}
	return end
}

func appendMigrationSQLToken(statementTokens *[]string, token string) {
	if len(*statementTokens) < migrationStatementTokenLimit {
		*statementTokens = append(*statementTokens, token)
	}
}

func markMigrationSQLToken(statementTokens *[]string, token string) error {
	appendMigrationSQLToken(statementTokens, strings.ToUpper(token))
	if len(*statementTokens) == 1 {
		switch (*statementTokens)[0] {
		case "BEGIN", "COMMIT", "ROLLBACK", "ABORT", "END":
			return errors.New("top-level transaction control is not allowed")
		}
	}
	if len(*statementTokens) == migrationStatementTokenLimit {
		first, second := (*statementTokens)[0], (*statementTokens)[1]
		if (first == "START" || first == "PREPARE") && second == "TRANSACTION" {
			return errors.New("top-level transaction control is not allowed")
		}
	}
	return nil
}

func skipSQLBlockComment(sql string, start int) (int, error) {
	depth := 1
	for index := start + sqlCommentDelimiterSize; index < len(sql); {
		switch {
		case index+1 < len(sql) && sql[index:index+2] == "/*":
			depth++
			index += 2
		case index+1 < len(sql) && sql[index:index+2] == "*/":
			depth--
			index += 2
			if depth == 0 {
				return index, nil
			}
		default:
			index++
		}
	}
	return 0, errors.New("unterminated migration block comment")
}

func skipSQLSingleQuote(sql string, start int, backslashEscapes bool) (int, error) {
	for index := start + 1; index < len(sql); index++ {
		if backslashEscapes && sql[index] == '\\' {
			index++
			continue
		}
		if sql[index] != '\'' {
			continue
		}
		if index+1 < len(sql) && sql[index+1] == '\'' {
			index++
			continue
		}
		return index + 1, nil
	}
	return 0, errors.New("unterminated single-quoted migration text")
}

func skipSQLDoubleQuote(sql string, start int) (int, error) {
	for index := start + 1; index < len(sql); index++ {
		if sql[index] != '"' {
			continue
		}
		if index+1 < len(sql) && sql[index+1] == '"' {
			index++
			continue
		}
		return index + 1, nil
	}
	return 0, errors.New("unterminated double-quoted migration identifier")
}

func sqlDollarDelimiter(sql string, start int) (string, int) {
	index := start + 1
	if index < len(sql) && sql[index] != '$' {
		if !isSQLIdentifierStart(sql[index]) {
			return "", 0
		}
		index++
		for index < len(sql) && isSQLDollarTagContinue(sql[index]) {
			index++
		}
	}
	if index >= len(sql) || sql[index] != '$' {
		return "", 0
	}
	delimiter := sql[start : index+1]
	return delimiter, index + 1
}

func isEscapeStringPrefix(sql string, quote int) bool {
	if quote == 0 || (sql[quote-1] != 'e' && sql[quote-1] != 'E') {
		return false
	}
	return quote == 1 || !isSQLIdentifierContinue(sql[quote-2])
}

func isSQLSpace(value byte) bool {
	switch value {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	default:
		return false
	}
}

func isSQLIdentifierStart(value byte) bool {
	return value == '_' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func isSQLIdentifierContinue(value byte) bool {
	return isSQLIdentifierStart(value) || value >= '0' && value <= '9' || value == '$'
}

func isSQLDollarTagContinue(value byte) bool {
	return isSQLIdentifierStart(value) || value >= '0' && value <= '9'
}

func migrationRoleIdentifier(role string) (string, error) {
	if !migrationRolePattern.MatchString(role) {
		return "", errors.New("migration role must be a lowercase PostgreSQL identifier")
	}
	return pgx.Identifier{role}.Sanitize(), nil
}

func migrateFS(ctx context.Context, conn *pgx.Conn, config Config) error {
	return withMigrationSession(ctx, conn, config, func(
		plan migrationPlan,
		migrations []migration,
		applied []appliedMigration,
	) error {
		if err := verifyMigrationHistory(applied, migrations); err != nil {
			return err
		}
		for _, migration := range migrations[len(applied):] {
			if applyErr := applyMigration(ctx, conn, plan, migration); applyErr != nil {
				return applyErr
			}
		}
		return nil
	})
}

func withMigrationSession(
	ctx context.Context,
	conn *pgx.Conn,
	config Config,
	action func(plan migrationPlan, migrations []migration, applied []appliedMigration) error,
) (returnErr error) {
	plan, err := newMigrationPlan(config)
	if err != nil {
		return err
	}
	if conn == nil {
		return errors.New("migration connection is required")
	}
	if config.Files == nil {
		return errors.New("migration files are required")
	}
	migrations, err := loadMigrations(config.Files)
	if err != nil {
		return err
	}

	authorized, err := migrationIdentityAuthorized(ctx, conn, plan.role)
	if err != nil {
		return err
	}
	if !authorized {
		return errors.New(
			"migration identity must be a least-privileged login separated from the configured owner role",
		)
	}

	unlockNeeded := false
	defer func() {
		if !unlockNeeded {
			return
		}
		unlockCtx, cancel := context.WithTimeout(context.Background(), migrationCleanupTimeout)
		defer cancel()
		var unlocked bool
		unlockErr := conn.QueryRow(unlockCtx, `SELECT pg_advisory_unlock($1)`, migrationLockKey).Scan(&unlocked)
		if unlockErr != nil && returnErr == nil {
			returnErr = fmt.Errorf("release migration lock: %w", unlockErr)
		} else if unlockErr == nil && !unlocked && returnErr == nil {
			returnErr = errors.New("migration lock was not held at release")
		}
	}()
	if _, execErr := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); execErr != nil {
		return fmt.Errorf("acquire migration lock: %w", execErr)
	}
	unlockNeeded = true

	applied, err := readMigrationHistory(ctx, conn, plan)
	if err != nil {
		return err
	}
	return action(plan, migrations, applied)
}

func migrationIdentityAuthorized(ctx context.Context, conn *pgx.Conn, role string) (bool, error) {
	var authorized bool
	if err := conn.QueryRow(ctx, `
		SELECT database.datdba = owner_role.oid
		   AND current_user = session_user
		   AND migrator_role.oid <> owner_role.oid
		   AND migrator_role.rolcanlogin
		   AND NOT migrator_role.rolinherit
		   AND NOT migrator_role.rolsuper
		   AND NOT migrator_role.rolcreatedb
		   AND NOT migrator_role.rolcreaterole
		   AND NOT migrator_role.rolreplication
		   AND NOT migrator_role.rolbypassrls
		   AND NOT owner_role.rolcanlogin
		   AND NOT owner_role.rolinherit
		   AND NOT owner_role.rolsuper
		   AND NOT owner_role.rolcreatedb
		   AND NOT owner_role.rolcreaterole
		   AND NOT owner_role.rolreplication
		   AND NOT owner_role.rolbypassrls
		   AND has_database_privilege(migrator_role.oid, database.oid, 'CONNECT')
		   AND NOT has_database_privilege(migrator_role.oid, database.oid, 'CREATE')
		   AND NOT has_database_privilege(migrator_role.oid, database.oid, 'TEMP')
		   AND (
				SELECT count(*) = 1
				   AND COALESCE(bool_and(
					   membership.roleid = owner_role.oid
					   AND NOT membership.admin_option
					   AND NOT membership.inherit_option
					   AND membership.set_option
				   ), false)
				FROM pg_auth_members membership
				WHERE membership.member = migrator_role.oid
		   )
		   AND NOT EXISTS (
				SELECT 1 FROM pg_auth_members membership
				WHERE membership.roleid = migrator_role.oid
		   )
		   AND NOT EXISTS (
				SELECT 1 FROM pg_auth_members membership
				WHERE membership.member = owner_role.oid
		   )
		   AND (
				SELECT count(*) = 1
				   AND COALESCE(bool_and(
					   membership.member = migrator_role.oid
					   AND NOT membership.admin_option
					   AND NOT membership.inherit_option
					   AND membership.set_option
				   ), false)
				FROM pg_auth_members membership
				WHERE membership.roleid = owner_role.oid
		   )
		   AND pg_has_role(migrator_role.oid, owner_role.oid, 'SET')
		FROM pg_database database
		JOIN pg_roles owner_role ON owner_role.rolname = $1
		JOIN pg_roles migrator_role ON migrator_role.rolname = session_user
		WHERE database.datname = current_database()
	`, role).Scan(&authorized); err != nil {
		return false, fmt.Errorf("verify migration role: %w", err)
	}
	return authorized, nil
}

type appliedMigration struct {
	Version   int64
	Name      string
	Checksum  []byte
	AppliedAt time.Time
}

func readMigrationHistory(
	ctx context.Context,
	conn *pgx.Conn,
	plan migrationPlan,
) ([]appliedMigration, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin migration metadata transaction: %w", err)
	}
	defer rollbackMigration(tx)
	if _, execErr := tx.Exec(ctx, "SET LOCAL ROLE "+plan.roleIdentifier); execErr != nil {
		return nil, fmt.Errorf("set migration role: %w", execErr)
	}
	if prepareMigrationMetadataErr := prepareMigrationMetadata(ctx, tx, plan); prepareMigrationMetadataErr != nil {
		return nil, prepareMigrationMetadataErr
	}
	if verifyMigrationMetadataErr := verifyMigrationMetadata(
		ctx,
		tx,
		plan.role,
		plan.metaSchema,
	); verifyMigrationMetadataErr != nil {
		return nil, verifyMigrationMetadataErr
	}
	if plan.appSchema != "" {
		runtimeRole, accessErr := verifyAppSchemaAccess(ctx, tx, plan.role, plan.appSchema)
		if accessErr != nil {
			return nil, accessErr
		}
		if verifyMigrationDatabaseAccessErr := verifyMigrationDatabaseAccess(
			ctx,
			tx,
			runtimeRole,
		); verifyMigrationDatabaseAccessErr != nil {
			return nil, verifyMigrationDatabaseAccessErr
		}
	}
	applied, err := readAppliedMigrations(ctx, tx, plan.metaTable)
	if err != nil {
		return nil, err
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return nil, fmt.Errorf("commit migration metadata: %w", commitErr)
	}
	return applied, nil
}

func prepareMigrationMetadata(ctx context.Context, tx pgx.Tx, plan migrationPlan) error {
	for _, statement := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + plan.metaSchema,
		"CREATE TABLE IF NOT EXISTS " + plan.metaTable + ` (
			version bigint PRIMARY KEY CHECK (version > 0),
			name text NOT NULL CHECK (name <> ''),
			checksum bytea NOT NULL CHECK (octet_length(checksum) = 32),
			applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
		)`,
	} {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("prepare migration metadata: %w", err)
		}
	}
	return nil
}

func verifyMigrationMetadata(ctx context.Context, tx pgx.Tx, role, metaSchema string) error {
	var schemaOwner, tableOwner string
	if err := tx.QueryRow(ctx, `
		SELECT pg_get_userbyid(namespace.nspowner), pg_get_userbyid(class.relowner)
		FROM pg_namespace namespace
		JOIN pg_class class ON class.relnamespace = namespace.oid
		WHERE namespace.nspname = $1
		  AND class.relname = 'schema_migrations'
		  AND class.relkind = 'r'
	`, metaSchema).Scan(&schemaOwner, &tableOwner); err != nil {
		return fmt.Errorf("verify migration metadata ownership: %w", err)
	}
	if schemaOwner != role || tableOwner != role {
		return errors.New("migration metadata is not owned by the migration role")
	}
	return verifyMigrationMetadataACL(ctx, tx, metaSchema)
}

func verifyMigrationMetadataACL(ctx context.Context, tx pgx.Tx, metaSchema string) error {
	var metadataACLValid bool
	if err := tx.QueryRow(ctx, `
		SELECT
			NOT EXISTS (
				SELECT 1
				FROM pg_namespace namespace
				CROSS JOIN LATERAL aclexplode(
					COALESCE(namespace.nspacl, acldefault('n', namespace.nspowner))
				) privilege
				WHERE namespace.nspname = $1
				  AND privilege.grantee <> namespace.nspowner
			)
			AND NOT EXISTS (
				SELECT 1
				FROM pg_class class
				JOIN pg_namespace namespace ON namespace.oid = class.relnamespace
				CROSS JOIN LATERAL aclexplode(
					COALESCE(class.relacl, acldefault('r', class.relowner))
				) privilege
				WHERE namespace.nspname = $1
				  AND class.relname = 'schema_migrations'
				  AND class.relkind = 'r'
				  AND privilege.grantee <> class.relowner
			)
			AND NOT EXISTS (
				SELECT 1
				FROM pg_attribute attribute
				JOIN pg_class class ON class.oid = attribute.attrelid
				JOIN pg_namespace namespace ON namespace.oid = class.relnamespace
				CROSS JOIN LATERAL aclexplode(attribute.attacl) privilege
				WHERE namespace.nspname = $1
				  AND class.relname = 'schema_migrations'
				  AND attribute.attnum > 0
				  AND NOT attribute.attisdropped
				  AND privilege.grantee <> class.relowner
			)
	`, metaSchema).Scan(&metadataACLValid); err != nil {
		return fmt.Errorf("verify migration metadata ACL: %w", err)
	}
	if !metadataACLValid {
		return errors.New("migration metadata has non-owner privileges")
	}
	return nil
}

func verifyAppSchemaAccess(ctx context.Context, tx pgx.Tx, role, appSchema string) (string, error) {
	var appOwner, runtimeRole string
	var runtimeGrantCount int
	var runtimeGrantValid bool
	if err := tx.QueryRow(ctx, `
		SELECT owner.rolname,
			max(grantee.rolname) FILTER (WHERE privilege.grantee <> namespace.nspowner),
			count(*) FILTER (WHERE privilege.grantee <> namespace.nspowner),
			COALESCE(bool_and(
				grantee.oid IS NOT NULL
				AND privilege.privilege_type = 'USAGE'
				AND NOT privilege.is_grantable
				AND grantee.rolcanlogin
				AND NOT grantee.rolinherit
				AND NOT grantee.rolsuper
				AND NOT grantee.rolcreatedb
				AND NOT grantee.rolcreaterole
				AND NOT grantee.rolreplication
				AND NOT grantee.rolbypassrls
				AND grantee.oid <> owner.oid
				AND grantee.rolname <> session_user
				AND has_database_privilege(grantee.oid, current_database(), 'CONNECT')
				AND NOT has_database_privilege(grantee.oid, current_database(), 'CREATE')
				AND NOT has_database_privilege(grantee.oid, current_database(), 'TEMP')
				AND NOT EXISTS (
					SELECT 1
					FROM pg_auth_members membership
					WHERE membership.member = grantee.oid
					   OR membership.roleid = grantee.oid
				)
				AND NOT pg_has_role((session_user)::regrole, grantee.oid, 'SET')
				AND NOT pg_has_role(grantee.oid, owner.oid, 'SET')
			) FILTER (WHERE privilege.grantee <> namespace.nspowner), false)
		FROM pg_namespace namespace
		JOIN pg_roles owner ON owner.oid = namespace.nspowner
		CROSS JOIN LATERAL aclexplode(
			COALESCE(namespace.nspacl, acldefault('n', namespace.nspowner))
		) privilege
		LEFT JOIN pg_roles grantee ON grantee.oid = privilege.grantee
		WHERE namespace.nspname = $1
		GROUP BY owner.rolname
	`, appSchema).Scan(&appOwner, &runtimeRole, &runtimeGrantCount, &runtimeGrantValid); err != nil {
		return "", fmt.Errorf("verify app schema ACL: %w", err)
	}
	if appOwner != role || runtimeGrantCount != 1 || !runtimeGrantValid {
		return "", errors.New("app schema ownership or runtime ACL is invalid")
	}
	return runtimeRole, nil
}

func verifyMigrationDatabaseAccess(ctx context.Context, tx pgx.Tx, runtimeRole string) error {
	var databaseACLValid bool
	if err := tx.QueryRow(ctx, `
		SELECT count(*) = 5
		   AND COALESCE(bool_and(
			   privilege.grantor = database.datdba
			   AND NOT privilege.is_grantable
			   AND (
				   privilege.grantee = database.datdba
				   AND privilege.privilege_type IN ('CONNECT', 'CREATE', 'TEMPORARY')
				   OR privilege.grantee = (session_user)::regrole
				   AND privilege.privilege_type = 'CONNECT'
				   OR privilege.grantee = runtime_role.oid
				   AND privilege.privilege_type = 'CONNECT'
			   )
		   ), false)
		FROM pg_database database
		JOIN pg_roles runtime_role ON runtime_role.rolname = $1
		CROSS JOIN LATERAL aclexplode(
			COALESCE(database.datacl, acldefault('d', database.datdba))
		) privilege
		WHERE database.datname = current_database()
		GROUP BY database.datdba, runtime_role.oid
	`, runtimeRole).Scan(&databaseACLValid); err != nil {
		return fmt.Errorf("verify database ACL: %w", err)
	}
	if !databaseACLValid {
		return errors.New("database ACL is not exact")
	}
	return nil
}

func readAppliedMigrations(ctx context.Context, tx pgx.Tx, metaTable string) ([]appliedMigration, error) {
	rows, err := tx.Query(ctx, `
		SELECT version, name, checksum, applied_at
		FROM `+metaTable+`
		ORDER BY version
	`)
	if err != nil {
		return nil, fmt.Errorf("read migration history: %w", err)
	}
	var applied []appliedMigration
	for rows.Next() {
		var migration appliedMigration
		if scanErr := rows.Scan(&migration.Version, &migration.Name, &migration.Checksum, &migration.AppliedAt); scanErr != nil {
			rows.Close()
			return nil, fmt.Errorf("read migration history: %w", scanErr)
		}
		applied = append(applied, migration)
	}
	if errCause := rows.Err(); errCause != nil {
		rows.Close()
		return nil, fmt.Errorf("read migration history: %w", errCause)
	}
	rows.Close()
	return applied, nil
}

func verifyMigrationHistory(applied []appliedMigration, migrations []migration) error {
	if len(applied) > len(migrations) {
		return fmt.Errorf("database contains unknown migration %06d", applied[len(migrations)].Version)
	}
	for index, recorded := range applied {
		expected := migrations[index]
		if recorded.Version != expected.Version {
			return fmt.Errorf("database contains unknown or out-of-order migration %06d", recorded.Version)
		}
		if recorded.Name != expected.Name {
			return fmt.Errorf("migration %06d name drift", expected.Version)
		}
		if len(recorded.Checksum) != sha256.Size || !bytes.Equal(recorded.Checksum, expected.Checksum[:]) {
			return fmt.Errorf("migration %06d checksum drift", expected.Version)
		}
	}
	return nil
}

func applyMigration(ctx context.Context, conn *pgx.Conn, plan migrationPlan, migration migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %06d: %w", migration.Version, err)
	}
	defer rollbackMigration(tx)
	if _, execErr := tx.Exec(ctx, "SET LOCAL ROLE "+plan.roleIdentifier); execErr != nil {
		return fmt.Errorf("set role for migration %06d: %w", migration.Version, execErr)
	}
	if _, execErr2 := tx.Exec(ctx, `SET LOCAL standard_conforming_strings = on`); execErr2 != nil {
		return fmt.Errorf("set string mode for migration %06d: %w", migration.Version, execErr2)
	}
	if _, execErr3 := tx.Exec(ctx, migration.SQL, pgx.QueryExecModeSimpleProtocol); execErr3 != nil {
		return fmt.Errorf("apply migration %06d: %w", migration.Version, execErr3)
	}
	if _, execErr4 := tx.Exec(ctx, `
		INSERT INTO `+plan.metaTable+` (version, name, checksum)
		VALUES ($1, $2, $3)
	`, migration.Version, migration.Name, migration.Checksum[:]); execErr4 != nil {
		return fmt.Errorf("record migration %06d: %w", migration.Version, execErr4)
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return fmt.Errorf("commit migration %06d: %w", migration.Version, commitErr)
	}
	return nil
}

func rollbackMigration(tx pgx.Tx) {
	rollbackCtx, cancel := context.WithTimeout(context.Background(), migrationCleanupTimeout)
	defer cancel()
	_ = tx.Rollback(rollbackCtx)
}
