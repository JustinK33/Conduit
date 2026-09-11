package lock

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// NoOp hands out every lock it is asked for.
//
// That is the correct choice with the Postgres transport, where every dispatch
// goes through ClaimNextJob and its FOR UPDATE SKIP LOCKED claim is already
// atomic across every instance. It is the wrong choice with the Kafka
// transport, where a job arrives PENDING from the topic and two instances can
// both decide to run it.
type NoOp struct{}

func (NoOp) Acquire(_ context.Context, resource string) (Lock, error) {
	return Lock{Resource: resource}, nil
}

func (NoOp) Release(context.Context, Lock) error { return nil }

// Advisory guards execution with Postgres session-scoped advisory locks, which
// removes the Redis requirement without giving up the guard.
//
// It buys one property Redlock does not have: an advisory lock is owned by its
// connection, so a process that is SIGKILLed releases every lock it held the
// moment the server notices the socket is gone. A Redlock key survives with its
// full TTL, which the README's crash measurement shows costing a restarted
// process hundreds of failed acquisitions.
//
// All locks live on one dedicated connection, for two reasons. A session lock
// has to be released on the session that took it, so a pooled connection cannot
// be handed back while holding one. And one connection per in-flight job would
// tie up CONDUIT_WORKER_CONCURRENCY connections doing nothing, starving the pool
// that the same jobs need to write their results.
//
// One session can take the same key twice, so the connection cannot exclude the
// process from itself; held is what does that.
type Advisory struct {
	pool *pgxpool.Pool
	log  zerolog.Logger

	mu   sync.Mutex
	conn *pgxpool.Conn
	held map[string]struct{}
}

func NewAdvisory(pool *pgxpool.Pool, log zerolog.Logger) *Advisory {
	return &Advisory{pool: pool, log: log, held: map[string]struct{}{}}
}

// advisoryTimeout bounds a lock round trip. It is generous for a statement that
// does no I/O beyond the network hop, and it exists so a wedged connection
// cannot stall the worker holding the mutex.
const advisoryTimeout = 3 * time.Second

func (a *Advisory) Acquire(ctx context.Context, resource string) (Lock, error) {
	ctx, cancel := a.sessionContext(ctx)
	defer cancel()

	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := a.held[resource]; ok {
		return Lock{}, fmt.Errorf("lock: %s is already held by this process", resource)
	}

	conn, err := a.session(ctx)
	if err != nil {
		return Lock{}, err
	}

	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", advisoryKey(resource)).Scan(&acquired); err != nil {
		a.discard()
		return Lock{}, fmt.Errorf("lock: try advisory lock on %s: %w", resource, err)
	}
	if !acquired {
		return Lock{}, fmt.Errorf("lock: %s is held by another instance", resource)
	}

	a.held[resource] = struct{}{}
	return Lock{Resource: resource}, nil
}

func (a *Advisory) Release(ctx context.Context, l Lock) error {
	ctx, cancel := a.sessionContext(ctx)
	defer cancel()

	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := a.held[l.Resource]; !ok {
		return nil
	}
	delete(a.held, l.Resource)

	conn, err := a.session(ctx)
	if err != nil {
		return err
	}
	// A false result means the session that took this lock is gone, which
	// released it server-side already. Worth a line, not an error.
	var unlocked bool
	if err := conn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", advisoryKey(l.Resource)).Scan(&unlocked); err != nil {
		a.discard()
		return fmt.Errorf("lock: release advisory lock on %s: %w", l.Resource, err)
	}
	if !unlocked {
		a.log.Debug().Str("resource", l.Resource).Msg("advisory lock was already gone")
	}
	return nil
}

// Close hands the dedicated connection back, which releases every lock still on
// it. Callers should stop the worker pool first.
func (a *Advisory) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.discard()
}

// sessionContext detaches the lock round trip from the caller's cancellation.
// pgx closes a connection whose query is cancelled mid-flight, to avoid leaving
// the protocol out of sync - and closing this connection would drop every
// advisory lock on it, not just this one. A job whose context is cancelled must
// not take its neighbours' guards with it.
func (a *Advisory) sessionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), advisoryTimeout)
}

// session returns the dedicated connection, opening one if there is none.
// Callers must hold a.mu.
func (a *Advisory) session(ctx context.Context) (*pgxpool.Conn, error) {
	if a.conn != nil {
		return a.conn, nil
	}
	conn, err := a.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("lock: acquire advisory lock session: %w", err)
	}
	a.conn = conn
	return conn, nil
}

// discard ends the session, which is what releases every advisory lock on it.
//
// Hijack and Close rather than Release: the connection may still hold locks, and
// handing it back to the pool would leave them held by a connection some
// unrelated caller now owns, with nothing left that knows to unlock them. The
// close runs off the mutex because the usual reason to discard a connection is
// that it stopped answering.
func (a *Advisory) discard() {
	if a.conn == nil {
		return
	}
	raw := a.conn.Hijack()
	a.conn = nil
	a.held = map[string]struct{}{}
	go func() { _ = raw.Close(context.Background()) }()
}

// advisoryKey hashes a resource name into the bigint that pg_try_advisory_lock
// takes. A collision costs two unrelated jobs a needless serialisation, not
// correctness, and at 64 bits it is not going to happen.
func advisoryKey(resource string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(resource))
	return int64(h.Sum64())
}
