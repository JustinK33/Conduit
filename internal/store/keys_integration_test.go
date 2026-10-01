package store

import (
	"context"
	"testing"
)

func TestKeyLifecycle(t *testing.T) {
	ctx, pool := testPool(t)
	s := NewKeyStore(pool)

	if _, _, err := s.CreateKey(ctx, "Not A Tenant", ""); err != ErrInvalidTenant {
		t.Fatalf("CreateKey(bad tenant): got %v, want ErrInvalidTenant", err)
	}

	id, key, err := s.CreateKey(ctx, "acme", "ci")
	if err != nil {
		t.Fatalf("CreateKey: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM api_keys WHERE id = $1`, id)
	})

	tenant, err := s.TenantForKey(ctx, key)
	if err != nil || tenant != "acme" {
		t.Fatalf("TenantForKey = %q, %v, want acme", tenant, err)
	}
	if _, err := s.TenantForKey(ctx, key+"x"); err != ErrKeyNotFound {
		t.Errorf("TenantForKey(wrong key): got %v, want ErrKeyNotFound", err)
	}
	if ok, err := s.HasActiveKeys(ctx); err != nil || !ok {
		t.Errorf("HasActiveKeys = %v, %v, want true", ok, err)
	}

	if err := s.RevokeKey(ctx, id); err != nil {
		t.Fatalf("RevokeKey: %v", err)
	}
	if _, err := s.TenantForKey(ctx, key); err != ErrKeyNotFound {
		t.Errorf("TenantForKey(revoked): got %v, want ErrKeyNotFound", err)
	}
	if err := s.RevokeKey(ctx, id); err != ErrKeyNotFound {
		t.Errorf("second RevokeKey: got %v, want ErrKeyNotFound", err)
	}
}
