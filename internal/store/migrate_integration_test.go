package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// TestMigrateAppliesOnceAndIsIdempotent is the check that phase 3's whole
// premise rests on: `conduit migrate` has to be safe to run on every boot,
// including against a database the old entrypoint mount already set up.
//
// The migrations here are throwaway files in a temp directory rather than the
// real migrations/, so the test proves the runner's behaviour without depending
// on what the shipped schema happens to contain. Both the created table and the
// schema_migrations rows are cleaned up, since schema_migrations is shared with
// the database the rest of the integration tests use.
//
// Runs only with POSTGRES_TEST_DSN set:
//
//	POSTGRES_TEST_DSN='postgres://conduit:conduit@localhost:5434/conduit?sslmode=disable' go test ./internal/store
func TestMigrateAppliesOnceAndIsIdempotent(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set POSTGRES_TEST_DSN to run")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	table := "migrate_test_" + suffix
	dir := t.TempDir()
	// Two files, so the test also covers ordering and the multi-file loop. The
	// second depends on the first having run, which a single file cannot show.
	versions := []string{"001_create_" + suffix + ".sql", "002_index_" + suffix + ".sql"}
	writeMigration(t, dir, versions[0],
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id TEXT PRIMARY KEY)", table))
	writeMigration(t, dir, versions[1],
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s ON %s (id)", table, table))

	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		_, _ = pool.Exec(context.Background(),
			"DELETE FROM schema_migrations WHERE version = ANY($1)", versions)
	})

	if err := Migrate(ctx, pool, dir, zerolog.Nop()); err != nil {
		t.Fatalf("first migrate: %v", err)
	}

	if got := countMigrations(t, pool, versions); got != 2 {
		t.Fatalf("recorded versions = %d, want 2", got)
	}
	var exists bool
	if err := pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
		table).Scan(&exists); err != nil {
		t.Fatalf("check table: %v", err)
	}
	if !exists {
		t.Fatalf("migration did not create %s", table)
	}

	// A second run must be a no-op. If it re-applied, the INSERT into
	// schema_migrations would violate the primary key and this would error.
	if err := Migrate(ctx, pool, dir, zerolog.Nop()); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if got := countMigrations(t, pool, versions); got != 2 {
		t.Fatalf("recorded versions after rerun = %d, want 2", got)
	}
}

// TestMigrateRejectsAnEmptyDirectory guards the mistake that would otherwise be
// silent: a wrong CONDUIT_POSTGRES_MIGRATIONS_PATH reporting "up to date" with no
// schema applied at all.
func TestMigrateRejectsAnEmptyDirectory(t *testing.T) {
	if _, err := migrationFiles(t.TempDir()); err == nil {
		t.Fatal("expected an error for a directory with no .sql files")
	}
}

func writeMigration(t *testing.T, dir, name, sqlText string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(sqlText), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func countMigrations(t *testing.T, pool *pgxpool.Pool, versions []string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM schema_migrations WHERE version = ANY($1)", versions).Scan(&count); err != nil {
		t.Fatalf("count schema_migrations: %v", err)
	}
	return count
}
