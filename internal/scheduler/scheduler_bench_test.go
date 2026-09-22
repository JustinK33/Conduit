package scheduler

import (
	"testing"
	"time"
)

var benchExpressions = []struct {
	name string
	expr string
}{
	{"every-minute", "* * * * *"},
	{"every-5-min", "*/5 * * * *"},
	{"daily-9am", "0 9 * * *"},
	{"weekdays-noon", "0 12 * * 1-5"},
	{"monthly-first", "0 0 1 * *"},
	{"complex", "15,45 9-17 * * 1-5"},
}

// BenchmarkParseCron measures cron expression compilation time.
func BenchmarkParseCron(b *testing.B) {
	for _, tc := range benchExpressions {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = Parse(tc.expr)
			}
		})
	}
}

// BenchmarkCronNext measures schedule evaluation (zero allocations target).
func BenchmarkCronNext(b *testing.B) {
	for _, tc := range benchExpressions {
		sched, _ := Parse(tc.expr)
		now := time.Now()

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sched.Next(now)
			}
		})
	}
}

// BenchmarkCronNextSequential simulates the scheduler tick: 100 consecutive Next() calls.
func BenchmarkCronNextSequential(b *testing.B) {
	sched, _ := Parse("*/5 * * * *")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := time.Now()
		for j := 0; j < 100; j++ {
			t = sched.Next(t)
		}
	}
}
