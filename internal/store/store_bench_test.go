package store

import (
	"testing"

	"github.com/JustinK33/Conduit/pkg/models"
)

// BenchmarkCanTransition measures the state machine transition check.
func BenchmarkCanTransition(b *testing.B) {
	pairs := []struct {
		from models.JobState
		to   models.JobState
	}{
		{models.JobStatePending, models.JobStateRunning},
		{models.JobStateRunning, models.JobStateCompleted},
		{models.JobStateRunning, models.JobStateFailed},
		{models.JobStateFailed, models.JobStatePending},
		{models.JobStatePending, models.JobStateDead},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := pairs[i%len(pairs)]
		CanTransition(p.from, p.to)
	}
}
