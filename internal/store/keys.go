package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrKeyNotFound   = errors.New("store: api key not found")
	ErrInvalidTenant = errors.New("store: tenant must be 1-64 characters of a-z, 0-9, _ or -")
)

var tenantPattern = regexp.MustCompile(`^[a-z0-9_-]{1,64}$`)

// APIKey is a key as listed. The key itself is never stored, so it is only
// ever seen once, in the return value of CreateKey.
type APIKey struct {
	ID        string     `json:"id"`
	TenantID  string     `json:"tenant_id"`
	Name      string     `json:"name"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

type KeyStore struct {
	Pool *pgxpool.Pool
}

func NewKeyStore(pool *pgxpool.Pool) *KeyStore {
	return &KeyStore{Pool: pool}
}

// CreateKey mints a key for tenant and returns its id and the key.
func (s *KeyStore) CreateKey(ctx context.Context, tenant, name string) (string, string, error) {
	if !tenantPattern.MatchString(tenant) {
		return "", "", ErrInvalidTenant
	}
	id, err := newID()
	if err != nil {
		return "", "", err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	key := "ck_" + base64.RawURLEncoding.EncodeToString(raw)

	_, err = s.Pool.Exec(ctx,
		`INSERT INTO api_keys (id, tenant_id, name, key_hash) VALUES ($1, $2, $3, $4)`,
		id, tenant, name, hashKey(key))
	if err != nil {
		return "", "", err
	}
	return id, key, nil
}

func (s *KeyStore) ListKeys(ctx context.Context) ([]APIKey, error) {
	rows, err := s.Pool.Query(ctx,
		`SELECT id, tenant_id, name, created_at, revoked_at FROM api_keys ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (APIKey, error) {
		var k APIKey
		err := row.Scan(&k.ID, &k.TenantID, &k.Name, &k.CreatedAt, &k.RevokedAt)
		return k, err
	})
}

func (s *KeyStore) RevokeKey(ctx context.Context, id string) error {
	tag, err := s.Pool.Exec(ctx,
		`UPDATE api_keys SET revoked_at = NOW() WHERE id = $1 AND revoked_at IS NULL`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrKeyNotFound
	}
	return nil
}

// TenantForKey returns the tenant a live key acts for, or ErrKeyNotFound.
func (s *KeyStore) TenantForKey(ctx context.Context, key string) (string, error) {
	var tenant string
	err := s.Pool.QueryRow(ctx,
		`SELECT tenant_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`,
		hashKey(key)).Scan(&tenant)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrKeyNotFound
	}
	if err != nil {
		return "", fmt.Errorf("store: look up api key: %w", err)
	}
	return tenant, nil
}

// HasActiveKeys reports whether any key is live, which is what turns auth on
// for an instance with no CONDUIT_API_KEYS.
func (s *KeyStore) HasActiveKeys(ctx context.Context) (bool, error) {
	var ok bool
	err := s.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM api_keys WHERE revoked_at IS NULL)`).Scan(&ok)
	return ok, err
}

func hashKey(key string) []byte {
	sum := sha256.Sum256([]byte(key))
	return sum[:]
}
