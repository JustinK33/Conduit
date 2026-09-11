package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// migrationLockKey is an arbitrary constant, shared by every process running
// migrations against one database. Two instances starting at once serialise on
// it instead of both trying to create the same table.
const migrationLockKey int64 = 0x636F6E6475697431 // "conduit1"

// Migrate applies every .sql file in dir that is not already recorded in
// schema_migrations, in filename order, each in its own transaction.
//
// This exists because the only previous way to get the schema in was mounting
// migrations/ into the Postgres entrypoint, which runs exactly once, on first
// boot, on an empty volume. Any migration added after a deployment existed
// simply never reached it.
//
// Applying a file and recording it happen in the same transaction, so a crash
// mid-run either applies a migration and remembers it or does neither. The
// files that ship with Conduit are all independently idempotent
// (CREATE ... IF NOT EXISTS), which is what makes it safe to run against a
// database that was already set up by the old entrypoint mount.
func Migrate(ctx context.Context, pool *pgxpool.Pool, dir string, log zerolog.Logger) error {
	files, err := migrationFiles(dir)
	if err != nil {
		return err
	}

	// One dedicated connection for the whole run: an advisory lock is scoped to
	// its session, so it has to be taken and released on the same connection.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("store: acquire connection for migrations: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return fmt.Errorf("store: take migration lock: %w", err)
	}
	defer func() {
		// Best effort: a failure here means the connection is already gone, which
		// releases the lock anyway.
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationLockKey)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT        PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("store: create schema_migrations: %w", err)
	}

	applied := map[string]bool{}
	rows, err := conn.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return fmt.Errorf("store: read schema_migrations: %w", err)
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return fmt.Errorf("store: scan schema_migrations: %w", err)
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: read schema_migrations: %w", err)
	}

	ran := 0
	for _, file := range files {
		version := filepath.Base(file)
		if applied[version] {
			continue
		}

		sqlText, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("store: read migration %s: %w", version, err)
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("store: begin migration %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, string(sqlText)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("store: apply migration %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", version); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("store: record migration %s: %w", version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("store: commit migration %s: %w", version, err)
		}

		log.Info().Str("version", version).Msg("applied migration")
		ran++
	}

	log.Info().Int("applied", ran).Int("total", len(files)).Msg("migrations up to date")
	return nil
}

// migrationFiles lists dir's .sql files in filename order, which is why they are
// numbered. Sorting is lexical, so 010 sorts after 009 but before 10 - keep the
// zero padding.
func migrationFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("store: read migrations directory %s: %w", dir, err)
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		files = append(files, filepath.Join(dir, e.Name()))
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("store: no .sql files in %s", dir)
	}
	sort.Strings(files)
	return files, nil
}
