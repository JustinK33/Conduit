package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/JustinK33/Conduit/internal/retry"
	"github.com/JustinK33/Conduit/pkg/models"
)

const (
	taskName          = "webhook"
	metadataURLKey    = "url"
	metadataMethodKey = "method"
	maxErrorBodyBytes = 4096
)

type Executor struct {
	client               *http.Client
	resolver             Resolver
	allowPrivateNetworks bool
	maxRedirects         int
}

type Resolver func(context.Context, string) ([]netip.Addr, error)

type Config struct {
	Client               *http.Client
	Resolver             Resolver
	Timeout              time.Duration
	MaxRedirects         int
	AllowPrivateNetworks bool
}

func NewExecutor(client *http.Client) *Executor {
	return NewExecutorWithConfig(Config{Client: client})
}

func NewExecutorWithConfig(cfg Config) *Executor {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.Resolver == nil {
		cfg.Resolver = defaultResolver
	}
	executor := &Executor{
		resolver:             cfg.Resolver,
		allowPrivateNetworks: cfg.AllowPrivateNetworks,
		maxRedirects:         cfg.MaxRedirects,
	}
	if cfg.Client != nil {
		executor.client = cfg.Client
		return executor
	}
	executor.client = &http.Client{
		Timeout: cfg.Timeout,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= executor.maxRedirects {
				return http.ErrUseLastResponse
			}
			if err := executor.validateURL(request.Context(), request.URL); err != nil {
				return err
			}
			return nil
		},
	}
	return executor
}

func (e *Executor) Handler(ctx context.Context, job models.Job) error {
	targetURL := strings.TrimSpace(job.Task.Metadata[metadataURLKey])
	if targetURL == "" {
		return fmt.Errorf("webhook: task.metadata.url is required: %w", retry.ErrNoRetry)
	}
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return fmt.Errorf("webhook: invalid url: %w: %w", err, retry.ErrNoRetry)
	}
	if err := e.validateURL(ctx, parsedURL); err != nil {
		return fmt.Errorf("webhook: unsafe url: %w: %w", err, retry.ErrNoRetry)
	}

	method := strings.ToUpper(strings.TrimSpace(job.Task.Metadata[metadataMethodKey]))
	if method == "" {
		method = http.MethodPost
	}
	if method != http.MethodPost && method != http.MethodPut && method != http.MethodPatch {
		return fmt.Errorf("webhook: unsupported method %q: %w", method, retry.ErrNoRetry)
	}

	body, err := json.Marshal(requestBody{
		JobID:    job.ID,
		TaskID:   job.Task.ID,
		TaskName: job.Task.Name,
		Attempt:  job.Attempt,
		Payload:  job.Task.Payload,
		Metadata: job.Metadata,
	})
	if err != nil {
		return fmt.Errorf("webhook: marshal request body: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, method, targetURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("webhook: create request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "Conduit webhook executor")
	request.Header.Set("X-Conduit-Job-ID", job.ID)
	request.Header.Set("X-Conduit-Attempt", fmt.Sprintf("%d", job.Attempt))

	response, err := e.client.Do(request)
	if err != nil {
		return fmt.Errorf("webhook: request failed: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode >= 200 && response.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, response.Body)
		return nil
	}

	responseBody, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorBodyBytes))
	message := fmt.Sprintf("webhook: status %d: %s", response.StatusCode, strings.TrimSpace(string(responseBody)))
	if response.StatusCode == http.StatusRequestTimeout ||
		response.StatusCode == http.StatusTooManyRequests ||
		response.StatusCode >= 500 {
		return fmt.Errorf("%s", message)
	}
	return fmt.Errorf("%s: %w", message, retry.ErrNoRetry)
}

type requestBody struct {
	JobID    string            `json:"job_id"`
	TaskID   string            `json:"task_id,omitempty"`
	TaskName string            `json:"task_name"`
	Attempt  int               `json:"attempt"`
	Payload  []byte            `json:"payload,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

func TaskName() string {
	return taskName
}

func (e *Executor) validateURL(ctx context.Context, parsedURL *url.URL) error {
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("url scheme must be http or https")
	}
	if parsedURL.User != nil {
		return fmt.Errorf("url userinfo is not allowed")
	}
	hostname := parsedURL.Hostname()
	if hostname == "" {
		return fmt.Errorf("url host is required")
	}
	if strings.EqualFold(hostname, "localhost") {
		return fmt.Errorf("localhost is not allowed")
	}

	if addr, err := netip.ParseAddr(hostname); err == nil {
		return e.validateAddr(addr)
	}

	addrs, err := e.resolver(ctx, hostname)
	if err != nil {
		return fmt.Errorf("resolve host %q: %w", hostname, err)
	}
	if len(addrs) == 0 {
		return fmt.Errorf("host %q resolved to no addresses", hostname)
	}
	for _, addr := range addrs {
		if err := e.validateAddr(addr); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) validateAddr(addr netip.Addr) error {
	if e.allowPrivateNetworks {
		return nil
	}
	if addr.IsLoopback() ||
		addr.IsPrivate() ||
		addr.IsLinkLocalUnicast() ||
		addr.IsLinkLocalMulticast() ||
		addr.IsMulticast() ||
		addr.IsUnspecified() {
		return fmt.Errorf("address %s is not public", addr)
	}
	return nil
}

func defaultResolver(ctx context.Context, hostname string) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, "ip", hostname)
}
