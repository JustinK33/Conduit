package retry

import (
	"errors"
	"math/rand"
	"time"
)

// ErrNoRetry short-circuits the retry loop for permanent failures (bad input, missing record, etc).
var ErrNoRetry = errors.New("retry: permanent failure, no retry")

// DefaultMaxAttempts is the retry budget a job gets when it doesn't set its own.
// Migration 008 backfills old rows with the same number.
const DefaultMaxAttempts = 5

type Config struct {
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
	MaxAttempts int
	Jitter      float64
}

type Engine struct {
	Config Config
}

// NewEngine returns an Engine with invalid fields normalised to sensible defaults.
func NewEngine(cfg Config) *Engine {
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = time.Second
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 30 * time.Second
	}
	if cfg.Multiplier <= 1 {
		cfg.Multiplier = 2
	}
	if cfg.MaxDelay < cfg.BaseDelay {
		cfg.MaxDelay = cfg.BaseDelay
	}
	if cfg.Jitter < 0 {
		cfg.Jitter = 0
	}
	if cfg.Jitter > 1 {
		cfg.Jitter = 1
	}
	return &Engine{Config: cfg}
}

// Delay returns the wait time before retrying after `attempt` failures.
func (e *Engine) Delay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	delay := float64(e.Config.BaseDelay)
	for i := 0; i < attempt; i++ {
		delay *= e.Config.Multiplier
		if delay > float64(e.Config.MaxDelay) {
			delay = float64(e.Config.MaxDelay)
			break
		}
	}
	if delay > float64(e.Config.MaxDelay) {
		delay = float64(e.Config.MaxDelay)
	}

	if e.Config.Jitter > 0 {
		delay += delay * e.Config.Jitter * rand.Float64()
		if delay > float64(e.Config.MaxDelay) {
			delay = float64(e.Config.MaxDelay)
		}
	}

	return time.Duration(delay)
}

// ShouldRetry returns false once MaxAttempts is reached or err is ErrNoRetry.
func (e *Engine) ShouldRetry(attempt int, err error) bool {
	if errors.Is(err, ErrNoRetry) {
		return false
	}
	if e.Config.MaxAttempts > 0 && attempt >= e.Config.MaxAttempts {
		return false
	}
	return true
}
