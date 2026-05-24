package workload

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// Metrics holds the Prometheus collectors that prove cluster health. Three
// groups correspond to the three roles:
//
//   - Writer  → master accepts INCR + SET
//   - Reader  → master serves GET
//   - Observer → each replica serves GET, and its value tracks the master's
//
// Together these answer:
//   - Is the leader accepting writes and serving reads?
//   - Is every follower reachable and serving reads?
//   - Is replication propagating? (replica_lag_count, replica_value_age_seconds)
type Metrics struct {
	// Writer
	Writes        *prometheus.CounterVec   // status (ok|error)
	WriteDuration prometheus.Histogram
	WriteErrors   *prometheus.CounterVec   // kind

	// Master reader
	MasterReads        *prometheus.CounterVec // status
	MasterReadDuration prometheus.Histogram
	MasterReadErrors   *prometheus.CounterVec // kind
	MasterValue        prometheus.Gauge       // latest counter value seen by the master reader
	MasterValueAge     prometheus.Gauge       // seconds since the master's stored value was originally written

	// Replica observer
	ReplicasObserved    prometheus.Gauge
	ReplicaReads        *prometheus.CounterVec   // replica, status
	ReplicaReadDuration *prometheus.HistogramVec // replica
	ReplicaReadErrors   *prometheus.CounterVec   // replica, kind
	ReplicaValue        *prometheus.GaugeVec     // replica
	ReplicaValueAge     *prometheus.GaugeVec     // replica
	ReplicaLagCount     *prometheus.GaugeVec     // replica — master counter minus this replica's counter

	// Internal: the last counter value the master reader saw. Used by the
	// observer to compute per-replica lag in "writes behind" terms.
	lastMasterValue atomic.Int64
}

func NewMetrics() *Metrics {
	buckets := prometheus.ExponentialBuckets(0.0005, 2, 12)
	return &Metrics{
		Writes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "russ_client_writes_total",
			Help: "Master writes attempted by the writer role.",
		}, []string{"status"}),
		WriteDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "russ_client_write_duration_seconds",
			Help:    "Duration of writer-role operations.",
			Buckets: buckets,
		}),
		WriteErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "russ_client_write_errors_total",
			Help: "Writer-role errors, classified by kind.",
		}, []string{"kind"}),

		MasterReads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "russ_client_master_reads_total",
			Help: "Master reads attempted by the reader role.",
		}, []string{"status"}),
		MasterReadDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "russ_client_master_read_duration_seconds",
			Help:    "Duration of master-read operations.",
			Buckets: buckets,
		}),
		MasterReadErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "russ_client_master_read_errors_total",
			Help: "Master-read errors, classified by kind.",
		}, []string{"kind"}),
		MasterValue: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "russ_client_master_value",
			Help: "Latest counter value read from the master.",
		}),
		MasterValueAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "russ_client_master_value_age_seconds",
			Help: "Seconds since the master's currently-stored value was originally written by the writer.",
		}),

		ReplicasObserved: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "russ_client_replicas_observed",
			Help: "Number of replicas the observer is currently polling.",
		}),
		ReplicaReads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "russ_client_replica_reads_total",
			Help: "Replica reads attempted by the observer role.",
		}, []string{"replica", "status"}),
		ReplicaReadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "russ_client_replica_read_duration_seconds",
			Help:    "Duration of observer reads against each replica.",
			Buckets: buckets,
		}, []string{"replica"}),
		ReplicaReadErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "russ_client_replica_read_errors_total",
			Help: "Observer-role errors per replica, classified by kind.",
		}, []string{"replica", "kind"}),
		ReplicaValue: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "russ_client_replica_value",
			Help: "Latest counter value read from each replica.",
		}, []string{"replica"}),
		ReplicaValueAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "russ_client_replica_value_age_seconds",
			Help: "Seconds since each replica's currently-visible value was originally written by the master.",
		}, []string{"replica"}),
		ReplicaLagCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "russ_client_replica_lag_count",
			Help: "Counter difference: master_value - replica_value. Positive = replica behind. Steady non-zero or growing = replication problem.",
		}, []string{"replica"}),
	}
}

// Register attaches the collectors to reg, plus the standard process and go
// collectors.
func (m *Metrics) Register(reg prometheus.Registerer) {
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(
		m.Writes, m.WriteDuration, m.WriteErrors,
		m.MasterReads, m.MasterReadDuration, m.MasterReadErrors,
		m.MasterValue, m.MasterValueAge,
		m.ReplicasObserved,
		m.ReplicaReads, m.ReplicaReadDuration, m.ReplicaReadErrors,
		m.ReplicaValue, m.ReplicaValueAge, m.ReplicaLagCount,
	)
}

// --- recording helpers ---

func (m *Metrics) recordWrite(dur time.Duration, err error) {
	status := "ok"
	if err != nil {
		if kind := classifyError(err); kind != "" {
			status = "error"
			m.WriteErrors.WithLabelValues(kind).Inc()
		}
	}
	m.Writes.WithLabelValues(status).Inc()
	m.WriteDuration.Observe(dur.Seconds())
}

func (m *Metrics) recordMasterRead(dur time.Duration, err error) {
	status := "ok"
	if err != nil {
		if kind := classifyError(err); kind != "" {
			status = "error"
			m.MasterReadErrors.WithLabelValues(kind).Inc()
		}
	}
	m.MasterReads.WithLabelValues(status).Inc()
	m.MasterReadDuration.Observe(dur.Seconds())
}

func (m *Metrics) recordMasterValue(n int64, writtenAt time.Time) {
	m.lastMasterValue.Store(n)
	m.MasterValue.Set(float64(n))
	m.MasterValueAge.Set(time.Since(writtenAt).Seconds())
}

func (m *Metrics) recordReplicaRead(replica string, dur time.Duration, err error) {
	status := "ok"
	if err != nil {
		if kind := classifyError(err); kind != "" {
			status = "error"
			m.ReplicaReadErrors.WithLabelValues(replica, kind).Inc()
		}
	}
	m.ReplicaReads.WithLabelValues(replica, status).Inc()
	m.ReplicaReadDuration.WithLabelValues(replica).Observe(dur.Seconds())
}

func (m *Metrics) recordReplicaValue(replica string, n int64, writtenAt time.Time) {
	m.ReplicaValue.WithLabelValues(replica).Set(float64(n))
	m.ReplicaValueAge.WithLabelValues(replica).Set(time.Since(writtenAt).Seconds())
	if masterN := m.lastMasterValue.Load(); masterN > 0 {
		m.ReplicaLagCount.WithLabelValues(replica).Set(float64(masterN - n))
	}
}

func (m *Metrics) forgetReplica(replica string) {
	m.ReplicaValue.DeleteLabelValues(replica)
	m.ReplicaValueAge.DeleteLabelValues(replica)
	m.ReplicaLagCount.DeleteLabelValues(replica)
}

// classifyError reduces a Redis or network error to a small, stable label set.
func classifyError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, redis.Nil) {
		return ""
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	s := err.Error()
	switch {
	case strings.HasPrefix(s, "READONLY"):
		return "readonly"
	case strings.HasPrefix(s, "LOADING"):
		return "loading"
	case strings.HasPrefix(s, "MASTERDOWN"):
		return "master_down"
	case strings.Contains(s, "deadline exceeded"), strings.Contains(s, "i/o timeout"):
		return "timeout"
	case strings.Contains(s, "connection refused"),
		strings.Contains(s, "EOF"),
		strings.Contains(s, "broken pipe"),
		strings.Contains(s, "reset by peer"):
		return "network"
	default:
		return "other"
	}
}

// ServeMetrics serves /metrics on addr until ctx is cancelled.
func ServeMetrics(ctx context.Context, addr string, reg *prometheus.Registry) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	return srv.ListenAndServe()
}
