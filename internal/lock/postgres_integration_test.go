package lock

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// Advisory replaces Redlock as the default guard, so the properties that make it
// a real guard have to be checked against a real Postgres. A fake would only
// prove the held map works, and the held map is not the interesting half: one
// session can take the same key twice, so it is the combination of the map and
// pg_try_advisory_lock that excludes both a process from itself and one instance
// from another.
//
// Runs only with POSTGRES_TEST_DSN set:
//
//	POSTGRES_TEST_DSN='postgres://conduit:conduit@localhost:5434/conduit?sslmode=disable' go test ./internal/lock
func TestAdvisoryExcludesSelfAndOtherInstances(t *testing.T) {
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

	// Two Advisory instances on separate pools, which is what two Conduit
	// processes look like to Postgres. Sharing one pool would work too, but a
	// separate pool makes it unambiguous that the exclusion is server-side.
	other, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect second pool: %v", err)
	}
	t.Cleanup(other.Close)

	first := NewAdvisory(pool, zerolog.Nop())
	t.Cleanup(first.Close)
	second := NewAdvisory(other, zerolog.Nop())
	t.Cleanup(second.Close)

	const resource = "job:advisory-integration-test"

	held, err := first.Acquire(ctx, resource)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	if _, err := first.Acquire(ctx, resource); err == nil {
		t.Fatal("the same instance acquired one resource twice")
	}
	if _, err := second.Acquire(ctx, resource); err == nil {
		t.Fatal("a second instance acquired a held resource")
	}

	if err := first.Release(ctx, held); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Released means available, to the other instance and to this one. A lock that
	// could not be re-taken would wedge the queue after one pass.
	reacquired, err := second.Acquire(ctx, resource)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if err := second.Release(ctx, reacquired); err != nil {
		t.Fatalf("release second: %v", err)
	}
}

// TestAdvisoryReleaseOfAnUnheldResourceIsQuiet covers the worker's error path:
// it releases in a defer, so a release after a failed acquire is normal and must
// not be reported as a problem.
func TestAdvisoryReleaseOfAnUnheldResourceIsQuiet(t *testing.T) {
	advisory := NewAdvisory(nil, zerolog.Nop())
	// A nil pool is safe here precisely because an unheld resource returns before
	// touching the session, which is the behaviour being asserted.
	if err := advisory.Release(context.Background(), Lock{Resource: "never-held"}); err != nil {
		t.Fatalf("release of an unheld resource: %v", err)
	}
}

func TestNoOpAlwaysGrants(t *testing.T) {
	var locker NoOp
	held, err := locker.Acquire(context.Background(), "anything")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if held.Resource != "anything" {
		t.Fatalf("resource = %q, want %q", held.Resource, "anything")
	}
	if err := locker.Release(context.Background(), held); err != nil {
		t.Fatalf("release: %v", err)
	}
}
