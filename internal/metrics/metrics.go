package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// TaskOther is the label value every task name that is not a registered handler
// collapses into.
//
// Task names arrive from callers, so an unfiltered task label is unbounded
// cardinality driven by untrusted input: one series per typo, forever, in a
// process that cannot forget them. Folding the unknown into one bucket keeps
// /metrics bounded by the handler table. This is the same argument the per-task
// circuit breakers already settled, and the two stay consistent on purpose.
const TaskOther = "other"

type Registry struct {
	Namespace    string
	Subsystem    string
	JobEnqueued  *prometheus.CounterVec
	JobStarted   *prometheus.CounterVec
	JobCompleted *prometheus.CounterVec
	JobFailed    *prometheus.CounterVec
	JobCancelled *prometheus.CounterVec
	// WorkerInFlight stays unlabelled: it describes the pool, not a task.
	WorkerInFlight prometheus.Gauge
	JobDuration    *prometheus.HistogramVec
	JobBacklog     *prometheus.GaugeVec
	HTTPRequests   *prometheus.CounterVec

	// knownTasks is written once by SetKnownTasks at boot and only read after.
	knownTasks map[string]struct{}
}

func NewRegistry(namespace, subsystem string) *Registry {
	r := &Registry{
		Namespace: namespace,
		Subsystem: subsystem,
	}

	newCounter := func(name, help string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      name,
			Help:      help,
		}, []string{"task"})
	}

	r.JobEnqueued = newCounter("jobs_enqueued_total", "Total number of jobs enqueued, by task name.")
	r.JobStarted = newCounter("jobs_started_total", "Total number of jobs started, by task name.")
	r.JobCompleted = newCounter("jobs_completed_total", "Total number of jobs completed successfully, by task name.")
	r.JobFailed = newCounter("jobs_failed_total", "Total number of jobs that failed, by task name.")
	r.JobCancelled = newCounter("jobs_cancelled_total", "Total number of jobs cancelled, by task name.")

	r.WorkerInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "worker_in_flight",
		Help:      "Number of jobs currently being processed.",
	})
	r.JobDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "job_duration_seconds",
		Help:      "Execution duration of jobs in seconds, by task name.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"task"})
	r.JobBacklog = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "jobs_backlog",
		Help:      "Jobs currently in each state, sampled periodically. Every instance reports the same database-wide numbers, so aggregate with max by (state).",
	}, []string{"state"})
	r.HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "http_requests_total",
		Help:      "Total HTTP requests partitioned by method, path, and status.",
	}, []string{"method", "path", "status"})

	return r
}

// SetKnownTasks declares the task names allowed to appear as label values.
// Everything else reports as TaskOther.
//
// Call it once at boot, before anything serves traffic: the set is read without
// a lock on every counter increment, because a mutex on that path would buy
// nothing but contention for a value that never changes after startup.
//
// Until it is called, every task reports as TaskOther. That is deliberate - an
// unset allow-list should lose detail, not bound.
func (r *Registry) SetKnownTasks(names []string) {
	known := make(map[string]struct{}, len(names))
	for _, n := range names {
		if n != "" {
			known[n] = struct{}{}
		}
	}
	r.knownTasks = known
}

// TaskLabel maps a task name onto its bounded label value.
func (r *Registry) TaskLabel(task string) string {
	if _, ok := r.knownTasks[task]; ok {
		return task
	}
	return TaskOther
}

func (r *Registry) IncJobEnqueued(task string)  { r.JobEnqueued.WithLabelValues(r.TaskLabel(task)).Inc() }
func (r *Registry) IncJobStarted(task string)   { r.JobStarted.WithLabelValues(r.TaskLabel(task)).Inc() }
func (r *Registry) IncJobCompleted(task string) { r.JobCompleted.WithLabelValues(r.TaskLabel(task)).Inc() }
func (r *Registry) IncJobFailed(task string)    { r.JobFailed.WithLabelValues(r.TaskLabel(task)).Inc() }
func (r *Registry) IncJobCancelled(task string) { r.JobCancelled.WithLabelValues(r.TaskLabel(task)).Inc() }

func (r *Registry) ObserveJobDuration(task string, seconds float64) {
	r.JobDuration.WithLabelValues(r.TaskLabel(task)).Observe(seconds)
}

// SetBacklog records how many jobs are in one state. State is a closed set in
// the schema, so this label needs no allow-list.
func (r *Registry) SetBacklog(state string, count float64) {
	r.JobBacklog.WithLabelValues(state).Set(count)
}

func (r *Registry) Register(registerer prometheus.Registerer) error {
	collectors := []prometheus.Collector{
		r.JobEnqueued,
		r.JobStarted,
		r.JobCompleted,
		r.JobFailed,
		r.JobCancelled,
		r.WorkerInFlight,
		r.JobDuration,
		r.JobBacklog,
		r.HTTPRequests,
	}
	for _, c := range collectors {
		if err := registerer.Register(c); err != nil {
			return err
		}
	}
	return nil
}

func (r *Registry) Handler() http.Handler {
	return promhttp.Handler()
}
