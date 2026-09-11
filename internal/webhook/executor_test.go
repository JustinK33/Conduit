package webhook

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"testing"

	"github.com/JustinK33/Conduit/internal/retry"
	"github.com/JustinK33/Conduit/pkg/models"
)

func TestHandlerSuccess(t *testing.T) {
	var gotJobID string
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotJobID = r.Header.Get("X-Conduit-Job-ID")
		return response(http.StatusNoContent, ""), nil
	})}

	executor := newTestExecutor(client)
	err := executor.Handler(context.Background(), models.Job{
		ID:      "job-1",
		Attempt: 1,
		Task: models.Task{
			Name:     TaskName(),
			Metadata: map[string]string{"url": "https://example.com/webhook"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotJobID != "job-1" {
		t.Fatalf("X-Conduit-Job-ID = %q, want job-1", gotJobID)
	}
}

func TestHandlerUsesConfiguredMethod(t *testing.T) {
	var gotMethod string
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotMethod = r.Method
		return response(http.StatusNoContent, ""), nil
	})}

	executor := newTestExecutor(client)
	err := executor.Handler(context.Background(), models.Job{
		Task: models.Task{
			Name: TaskName(),
			Metadata: map[string]string{
				"url":    "https://example.com/webhook",
				"method": http.MethodPatch,
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotMethod != http.MethodPatch {
		t.Fatalf("method = %q, want PATCH", gotMethod)
	}
}

func TestHandlerRejectsUnsupportedMethod(t *testing.T) {
	executor := newTestExecutor(nil)
	err := executor.Handler(context.Background(), models.Job{
		Task: models.Task{
			Name: TaskName(),
			Metadata: map[string]string{
				"url":    "https://example.com/webhook",
				"method": http.MethodGet,
			},
		},
	})
	if !errors.Is(err, retry.ErrNoRetry) {
		t.Fatalf("expected permanent error, got %v", err)
	}
}

func TestHandlerRetryableStatus(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return response(http.StatusInternalServerError, "try later"), nil
	})}

	executor := newTestExecutor(client)
	err := executor.Handler(context.Background(), models.Job{
		Task: models.Task{
			Name:     TaskName(),
			Metadata: map[string]string{"url": "https://example.com/webhook"},
		},
	})
	if err == nil {
		t.Fatal("expected retryable error")
	}
	if errors.Is(err, retry.ErrNoRetry) {
		t.Fatalf("expected retryable error, got permanent error: %v", err)
	}
}

func TestHandlerPermanentStatus(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return response(http.StatusBadRequest, "bad request"), nil
	})}

	executor := newTestExecutor(client)
	err := executor.Handler(context.Background(), models.Job{
		Task: models.Task{
			Name:     TaskName(),
			Metadata: map[string]string{"url": "https://example.com/webhook"},
		},
	})
	if !errors.Is(err, retry.ErrNoRetry) {
		t.Fatalf("expected permanent error, got %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func response(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func TestHandlerRejectsMissingURL(t *testing.T) {
	executor := newTestExecutor(nil)
	err := executor.Handler(context.Background(), models.Job{
		Task: models.Task{Name: TaskName()},
	})
	if !errors.Is(err, retry.ErrNoRetry) {
		t.Fatalf("expected permanent error, got %v", err)
	}
}

func TestHandlerRejectsPrivateTargets(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "localhost", url: "http://localhost/webhook"},
		{name: "loopback IPv4", url: "http://127.0.0.1/webhook"},
		{name: "private IPv4", url: "http://10.0.0.1/webhook"},
		{name: "link local IPv4", url: "http://169.254.169.254/webhook"},
		{name: "loopback IPv6", url: "http://[::1]/webhook"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			executor := newTestExecutor(nil)
			err := executor.Handler(context.Background(), models.Job{
				Task: models.Task{
					Name:     TaskName(),
					Metadata: map[string]string{"url": tc.url},
				},
			})
			if !errors.Is(err, retry.ErrNoRetry) {
				t.Fatalf("expected permanent error, got %v", err)
			}
		})
	}
}

func TestHandlerRejectsHostnamesResolvingPrivate(t *testing.T) {
	executor := NewExecutorWithConfig(Config{
		Client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) { return response(http.StatusNoContent, ""), nil })},
		Resolver: resolverFor(map[string][]netip.Addr{
			"internal.example": {netip.MustParseAddr("10.0.0.5")},
		}),
	})

	err := executor.Handler(context.Background(), models.Job{
		Task: models.Task{
			Name:     TaskName(),
			Metadata: map[string]string{"url": "https://internal.example/webhook"},
		},
	})
	if !errors.Is(err, retry.ErrNoRetry) {
		t.Fatalf("expected permanent error, got %v", err)
	}
}

func newTestExecutor(client *http.Client) *Executor {
	return NewExecutorWithConfig(Config{
		Client: client,
		Resolver: resolverFor(map[string][]netip.Addr{
			"example.com": {netip.MustParseAddr("93.184.216.34")},
		}),
	})
}

func resolverFor(records map[string][]netip.Addr) Resolver {
	return func(_ context.Context, hostname string) ([]netip.Addr, error) {
		if addrs, ok := records[hostname]; ok {
			return addrs, nil
		}
		return nil, errors.New("host not found")
	}
}
