package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/JustinK33/Conduit/pkg/models"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name         string
		tickInterval time.Duration
	}{
		{name: "creates scheduler with tick interval", tickInterval: time.Second},
		{name: "creates scheduler with short interval", tickInterval: 100 * time.Millisecond},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.tickInterval)
			if s == nil {
				t.Fatal("New returned nil")
			}
			if s.tickInterval != tc.tickInterval {
				t.Errorf("tickInterval = %v, want %v", s.tickInterval, tc.tickInterval)
			}
			if s.entries == nil {
				t.Error("entries map is nil; it should be initialized")
			}
		})
	}
}

func TestSchedulerRegister(t *testing.T) {
	tests := []struct {
		name    string
		entry   Entry
		wantErr bool
	}{
		{
			name:    "registers valid entry",
			entry:   Entry{Name: "nightly", Cron: "0 0 * * *", Job: models.Job{ID: "1"}, Enabled: true},
			wantErr: false,
		},
		{
			name:    "rejects empty name",
			entry:   Entry{Name: "", Cron: "0 0 * * *"},
			wantErr: true,
		},
		{
			name:    "rejects invalid cron expression",
			entry:   Entry{Name: "bad-cron", Cron: "not a cron"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(time.Second)
			if s == nil {
				t.Skip("New not yet implemented")
			}
			err := s.Register(tc.entry)
			if tc.wantErr && err == nil {
				t.Error("expected error but got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tc.wantErr {
				if _, ok := s.entries[tc.entry.Name]; !ok {
					t.Errorf("entry %q not found in registry after Register", tc.entry.Name)
				}
			}
		})
	}
}

func TestSchedulerRegisterReplaces(t *testing.T) {
	t.Run("duplicate name replaces existing entry", func(t *testing.T) {
		s := New(time.Second)
		if s == nil {
			t.Skip("New not yet implemented")
		}

		e1 := Entry{Name: "job", Cron: "0 * * * *", Job: models.Job{ID: "1"}, Enabled: true}
		e2 := Entry{Name: "job", Cron: "0 0 * * *", Job: models.Job{ID: "2"}, Enabled: true}

		if err := s.Register(e1); err != nil {
			t.Fatalf("Register(e1) error: %v", err)
		}
		if err := s.Register(e2); err != nil {
			t.Fatalf("Register(e2) error: %v", err)
		}

		if len(s.entries) != 1 {
			t.Errorf("len(entries) = %d, want 1 (second register should replace)", len(s.entries))
		}
		if s.entries["job"].Job.ID != "2" {
			t.Errorf("entry job ID = %q, want %q", s.entries["job"].Job.ID, "2")
		}
	})
}

func TestSchedulerStart(t *testing.T) {
	t.Run("starts and stops without panic", func(t *testing.T) {
		s := New(50 * time.Millisecond)
		if s == nil {
			t.Skip("New not yet implemented")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		s.Start(ctx)
		<-ctx.Done()
		s.Stop()
	})
}

func TestSchedulerStop(t *testing.T) {
	t.Run("stop is safe to call before start", func(t *testing.T) {
		s := New(time.Second)
		if s == nil {
			t.Skip("New not yet implemented")
		}
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Stop panicked: %v", r)
			}
		}()
		s.Stop()
	})

	t.Run("stop is safe to call twice", func(t *testing.T) {
		s := New(50 * time.Millisecond)
		if s == nil {
			t.Skip("New not yet implemented")
		}
		ctx, cancel := context.WithCancel(context.Background())
		s.Start(ctx)
		cancel()
		s.Stop()

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("second Stop panicked: %v", r)
			}
		}()
		s.Stop()
	})
}
