package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// NotifyChannel is the LISTEN/NOTIFY channel the Postgres transport uses. It is
// a constant rather than a setting: both sides of it are one deployment talking
// to itself over the same database, and two instances that disagreed on the name
// would still be sharing the jobs table anyway.
const NotifyChannel = "conduit_jobs"

// PostgresNotifier is the Postgres transport's publish side. It satisfies the
// same Publisher contract as KafkaClient, so JobService does not know or care
// which one it holds.
//
// The notification carries the job id and no authority. Postgres already has the
// job as PENDING by the time this runs, and the reconciler claims PENDING work
// whether a notification arrives or not, so a dropped notification costs
// dispatch latency and nothing else. That is the same deal Kafka gets.
type PostgresNotifier struct {
	pool *pgxpool.Pool
}

func NewPostgresNotifier(pool *pgxpool.Pool) *PostgresNotifier {
	return &PostgresNotifier{pool: pool}
}

// Publish sends the job id on the given channel. pg_notify is used rather than
// the NOTIFY statement because the channel is a config value and pg_notify takes
// it as text, so there is no identifier to quote and nothing to inject.
func (n *PostgresNotifier) Publish(ctx context.Context, channel string, job models.Job) error {
	if _, err := n.pool.Exec(ctx, "SELECT pg_notify($1, $2)", channel, job.ID); err != nil {
		return fmt.Errorf("queue: notify %s: %w", channel, err)
	}
	return nil
}

func (n *PostgresNotifier) Close() error { return nil }

// PostgresListener is the Postgres transport's consume side. It holds one
// connection outside the pool doing nothing but LISTEN, and calls onWake for
// each notification.
//
// It deliberately does not deliver the job. The reconciler's ClaimNextJob is
// already the correct dispatch path - atomic across instances, queue-filtered,
// and the thing that decides who executes - so all a notification has to do is
// get the reconciler to run one pass now instead of at the end of its idle
// interval. That keeps the transport at zero risk of double-dispatching, needs
// no queue filter here, and has no 8000-byte payload ceiling to design around.
type PostgresListener struct {
	dsn     string
	channel string
	log     zerolog.Logger
}

func NewPostgresListener(dsn, channel string, log zerolog.Logger) *PostgresListener {
	return &PostgresListener{dsn: dsn, channel: channel, log: log}
}

// listenerRetryDelay is how long to wait before reconnecting a dropped listener.
// The reconciler keeps dispatching on its own timer throughout, so this window
// costs latency and never a job.
const listenerRetryDelay = 2 * time.Second

// Run listens until ctx is done, reconnecting on failure. A dedicated connection
// rather than a pooled one: this one blocks indefinitely in WaitForNotification,
// so a pooled connection would be permanently checked out, and LISTEN
// registrations belong to a session that has to outlive any single query.
func (l *PostgresListener) Run(ctx context.Context, onWake func()) {
	for {
		if err := l.listen(ctx, onWake); err != nil && ctx.Err() == nil {
			l.log.Warn().Err(err).Dur("retry_in", listenerRetryDelay).
				Msg("notify listener dropped, reconnecting")
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(listenerRetryDelay):
		}
	}
}

func (l *PostgresListener) listen(ctx context.Context, onWake func()) error {
	conn, err := pgx.Connect(ctx, l.dsn)
	if err != nil {
		return fmt.Errorf("queue: connect listener: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	// The channel name is an identifier here, so it has to be quoted rather than
	// parameterised: LISTEN takes no bind parameters.
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{l.channel}.Sanitize()); err != nil {
		return fmt.Errorf("queue: listen on %s: %w", l.channel, err)
	}
	l.log.Info().Str("channel", l.channel).Msg("listening for job notifications")

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("queue: wait for notification: %w", err)
		}
		l.log.Debug().Str("job_id", notification.Payload).Msg("job notification")
		onWake()
	}
}
