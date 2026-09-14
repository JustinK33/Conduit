package models

import (
	"encoding/json"
	"testing"
	"time"
)

func TestJobStateConstants(t *testing.T) {
	tests := []struct {
		name  string
		state JobState
		want  string
	}{
		{name: "pending", state: JobStatePending, want: "PENDING"},
		{name: "running", state: JobStateRunning, want: "RUNNING"},
		{name: "completed", state: JobStateCompleted, want: "COMPLETED"},
		{name: "failed", state: JobStateFailed, want: "FAILED"},
		{name: "dead", state: JobStateDead, want: "DEAD"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if string(tc.state) != tc.want {
				t.Errorf("JobState value = %q, want %q", tc.state, tc.want)
			}
		})
	}
}

func TestJobAndTaskShapes(t *testing.T) {
	t.Run("job timestamps are nullable pointers", func(t *testing.T) {
		j := Job{}
		if j.ScheduledAt != nil {
			t.Error("ScheduledAt should be nil on zero-value Job")
		}
		if j.StartedAt != nil {
			t.Error("StartedAt should be nil on zero-value Job")
		}
		if j.CompletedAt != nil {
			t.Error("CompletedAt should be nil on zero-value Job")
		}
	})

	t.Run("task payload round-trips through job", func(t *testing.T) {
		payload := []byte(`{"email":"user@example.com"}`)
		j := Job{
			Task:  Task{Name: "send-email", Payload: payload, MaxRetries: 3, Queue: "default"},
			State: JobStatePending,
		}

		if j.Task.Name != "send-email" {
			t.Errorf("Task.Name = %q, want %q", j.Task.Name, "send-email")
		}
		if string(j.Task.Payload) != string(payload) {
			t.Errorf("Task.Payload = %q, want %q", j.Task.Payload, payload)
		}
		if j.State != JobStatePending {
			t.Errorf("State = %q, want %q", j.State, JobStatePending)
		}
	})
}

func TestDurationJSON(t *testing.T) {
	t.Run("marshals as a duration string", func(t *testing.T) {
		cases := map[Duration]string{
			Duration(15 * time.Second):       `"15s"`,
			Duration(90 * time.Second):       `"1m30s"`,
			Duration(2 * time.Hour):          `"2h0m0s"`,
			Duration(500 * time.Millisecond): `"500ms"`,
			0:                                `"0s"`,
		}
		for d, want := range cases {
			got, err := json.Marshal(d)
			if err != nil {
				t.Fatalf("marshal %d: %v", d, err)
			}
			if string(got) != want {
				t.Errorf("marshal = %s, want %s", got, want)
			}
		}
	})

	t.Run("round-trips through a task", func(t *testing.T) {
		encoded, err := json.Marshal(Task{Name: "webhook", Timeout: Duration(15 * time.Second)})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var decoded Task
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatalf("unmarshal %s: %v", encoded, err)
		}
		if time.Duration(decoded.Timeout) != 15*time.Second {
			t.Errorf("round-tripped timeout = %s, want 15s", time.Duration(decoded.Timeout))
		}
	})

	t.Run("rejects the pre-v0.2.0 nanosecond form and other garbage", func(t *testing.T) {
		// 15000000000 was the documented form through v0.1.0. Accepting it
		// silently would be fine; guessing at 15 would not, and there is no way
		// to tell the two apart, so both are errors.
		for _, body := range []string{`15000000000`, `15`, `"banana"`, `null`, `{}`, `true`} {
			var d Duration
			if err := json.Unmarshal([]byte(body), &d); err == nil {
				t.Errorf("unmarshal %s succeeded as %s, want an error", body, time.Duration(d))
			}
		}
	})
}

func TestPayloadJSON(t *testing.T) {
	t.Run("carries JSON inline in both directions", func(t *testing.T) {
		for _, body := range []string{
			`{"order_id":1234}`,
			`[1,2,3]`,
			`"a bare string"`,
			`42`,
			`null`,
		} {
			var p Payload
			if err := json.Unmarshal([]byte(body), &p); err != nil {
				t.Fatalf("unmarshal %s: %v", body, err)
			}
			got, err := json.Marshal(p)
			if err != nil {
				t.Fatalf("marshal %s: %v", body, err)
			}
			if string(got) != body {
				t.Errorf("round-trip = %s, want %s", got, body)
			}
		}
	})

	t.Run("empty marshals as null", func(t *testing.T) {
		got, err := json.Marshal(Payload(nil))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if string(got) != "null" {
			t.Errorf("marshal = %s, want null", got)
		}
	})

	t.Run("non-JSON bytes fall back to base64 rather than emitting invalid JSON", func(t *testing.T) {
		// A row written before v0.2.0 can hold anything. Returning it verbatim
		// would put a broken response on the wire, so this is the one case that
		// must not round-trip.
		got, err := json.Marshal(Payload([]byte{0x00, 0xff, 0xfe}))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if want := `"AP/+"`; string(got) != want {
			t.Errorf("marshal = %s, want %s", got, want)
		}
		if !json.Valid(got) {
			t.Errorf("marshal produced invalid JSON: %s", got)
		}
	})
}
