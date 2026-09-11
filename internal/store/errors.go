package store

import "errors"

var (
	ErrJobNotFound             = errors.New("store: job not found")
	ErrInvalidTransition       = errors.New("store: invalid state transition")
	ErrDuplicateIdempotencyKey = errors.New("store: duplicate idempotency key")
	ErrLeaseLost               = errors.New("store: lease no longer held")
)
