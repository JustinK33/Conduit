package etl

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/JustinK33/Conduit/internal/retry"
	"github.com/JustinK33/Conduit/pkg/models"
)

func TestParseSpec(t *testing.T) {
	payload, err := json.Marshal(PipelineSpec{
		ExtractSQL:    "SELECT id, total FROM orders",
		TargetTable:   "analytics.order_totals",
		TargetColumns: []string{"order_id", "total"},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	spec, err := ParseSpec(payload)
	if err != nil {
		t.Fatalf("ParseSpec returned error: %v", err)
	}
	if spec.TargetTable != "analytics.order_totals" {
		t.Fatalf("target table = %q, want analytics.order_totals", spec.TargetTable)
	}
	if len(spec.TargetColumns) != 2 {
		t.Fatalf("target columns = %d, want 2", len(spec.TargetColumns))
	}
}

func TestParseSpecRejectsMissingFields(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "empty payload", payload: ""},
		{name: "missing extract", payload: `{"target_table":"analytics.daily_orders","target_columns":["day"]}`},
		{name: "missing target table", payload: `{"extract_sql":"SELECT now()","target_columns":["day"]}`},
		{name: "missing target columns", payload: `{"extract_sql":"SELECT now()","target_table":"analytics.daily_orders"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseSpec([]byte(tc.payload))
			if err == nil {
				t.Fatal("ParseSpec returned nil error")
			}
		})
	}
}

func TestBuildInsertSQL(t *testing.T) {
	sql, err := BuildInsertSQL(PipelineSpec{
		ExtractSQL:    "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id",
		TargetTable:   "analytics.customer_order_counts",
		TargetColumns: []string{"customer_id", "order_count"},
	})
	if err != nil {
		t.Fatalf("BuildInsertSQL returned error: %v", err)
	}

	want := `INSERT INTO "analytics"."customer_order_counts" ("customer_id", "order_count")
SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id`
	if sql != want {
		t.Fatalf("sql = %q, want %q", sql, want)
	}
}

func TestBuildInsertSQLAllowsCTE(t *testing.T) {
	_, err := BuildInsertSQL(PipelineSpec{
		ExtractSQL:    "WITH recent AS (SELECT id FROM orders) SELECT id FROM recent",
		TargetTable:   "analytics.recent_orders",
		TargetColumns: []string{"order_id"},
	})
	if err != nil {
		t.Fatalf("BuildInsertSQL returned error: %v", err)
	}
}

func TestBuildInsertSQLUpsert(t *testing.T) {
	sql, err := BuildInsertSQL(PipelineSpec{
		ExtractSQL:      "SELECT ordered_at::date, COUNT(*), SUM(order_total) FROM raw.orders GROUP BY ordered_at::date",
		TargetTable:     "analytics.daily_revenue",
		TargetColumns:   []string{"revenue_day", "order_count", "gross_revenue"},
		WriteMode:       "upsert",
		ConflictColumns: []string{"revenue_day"},
	})
	if err != nil {
		t.Fatalf("BuildInsertSQL returned error: %v", err)
	}

	want := `INSERT INTO "analytics"."daily_revenue" ("revenue_day", "order_count", "gross_revenue")
SELECT ordered_at::date, COUNT(*), SUM(order_total) FROM raw.orders GROUP BY ordered_at::date
ON CONFLICT ("revenue_day") DO UPDATE SET "order_count" = EXCLUDED."order_count", "gross_revenue" = EXCLUDED."gross_revenue"`
	if sql != want {
		t.Fatalf("sql = %q, want %q", sql, want)
	}
}

func TestBuildInsertSQLRejectsUpsertWithoutConflictColumns(t *testing.T) {
	_, err := BuildInsertSQL(PipelineSpec{
		ExtractSQL:    "SELECT id FROM orders",
		TargetTable:   "analytics.orders",
		TargetColumns: []string{"id"},
		WriteMode:     "upsert",
	})
	if err == nil {
		t.Fatal("BuildInsertSQL returned nil error")
	}
}

func TestBuildInsertSQLRejectsUnsafeIdentifiers(t *testing.T) {
	tests := []struct {
		name string
		spec PipelineSpec
	}{
		{
			name: "target table injection",
			spec: PipelineSpec{
				ExtractSQL:    "SELECT id FROM orders",
				TargetTable:   "orders; DROP TABLE users",
				TargetColumns: []string{"id"},
			},
		},
		{
			name: "qualified target column",
			spec: PipelineSpec{
				ExtractSQL:    "SELECT id FROM orders",
				TargetTable:   "analytics.orders",
				TargetColumns: []string{"orders.id"},
			},
		},
		{
			name: "target column injection",
			spec: PipelineSpec{
				ExtractSQL:    "SELECT id FROM orders",
				TargetTable:   "analytics.orders",
				TargetColumns: []string{"id) VALUES (1); DROP TABLE users; --"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildInsertSQL(tc.spec)
			if err == nil {
				t.Fatal("BuildInsertSQL returned nil error")
			}
		})
	}
}

func TestBuildInsertSQLRejectsWriteStatements(t *testing.T) {
	tests := []string{
		"UPDATE orders SET total = 0",
		"DELETE FROM orders",
		"SELECT id FROM orders; DROP TABLE users",
		"WITH removed AS (DELETE FROM orders RETURNING id) SELECT id FROM removed",
	}

	for _, query := range tests {
		t.Run(query, func(t *testing.T) {
			_, err := BuildInsertSQL(PipelineSpec{
				ExtractSQL:    query,
				TargetTable:   "analytics.orders",
				TargetColumns: []string{"id"},
			})
			if err == nil {
				t.Fatal("BuildInsertSQL returned nil error")
			}
		})
	}
}

func TestHandlerMarksBadPayloadPermanent(t *testing.T) {
	executor := NewExecutor(nil)
	err := executor.Handler(nil, models.Job{
		Task: models.Task{
			Name:    TaskName(),
			Payload: []byte(`{"extract_sql":"SELECT id FROM orders; DROP TABLE users","target_table":"analytics.orders","target_columns":["id"]}`),
		},
	})
	if !errors.Is(err, retry.ErrNoRetry) {
		t.Fatalf("expected permanent error, got %v", err)
	}
	if !strings.Contains(err.Error(), "forbidden token") {
		t.Fatalf("error = %q, want forbidden token", err.Error())
	}
}
