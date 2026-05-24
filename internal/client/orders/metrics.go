package orders

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics is the russ-client orders-analytics Prometheus surface.
type Metrics struct {
	// Writer
	WriterEnqueued             prometheus.Counter
	WriterEnqueueErrors        *prometheus.CounterVec // kind
	WriterEnqueuersActive      prometheus.Gauge
	WriterWaveTarget           prometheus.Gauge
	WriterRevenueCentsEnqueued prometheus.Counter

	// Processor
	OrdersProcessed        prometheus.Counter
	RevenueCentsProcessed  prometheus.Counter
	LineItemsProcessed     prometheus.Counter
	ProcessorApplyDuration prometheus.Histogram
	ProcessorErrors        *prometheus.CounterVec // task, kind

	// API
	APIRequests        *prometheus.CounterVec   // route, status
	APIRequestDuration *prometheus.HistogramVec // route
}

// NewMetrics constructs the metric set.
func NewMetrics() *Metrics {
	dur := prometheus.ExponentialBuckets(0.0005, 2, 14) // ~0.5 ms → ~4 s

	return &Metrics{
		WriterEnqueued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "russ_client_orders_enqueued_total",
			Help: "order:process tasks successfully enqueued by the writer pool.",
		}),
		WriterEnqueueErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "russ_client_orders_enqueue_errors_total",
			Help: "Enqueue failures, classified by kind.",
		}, []string{"kind"}),
		WriterEnqueuersActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "russ_client_writer_enqueuers_active",
			Help: "Currently running enqueuer goroutines in the writer pool.",
		}),
		WriterWaveTarget: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "russ_client_writer_wave_target",
			Help: "Current wave target (number of enqueuers the coordinator is driving toward).",
		}),
		WriterRevenueCentsEnqueued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "russ_client_writer_revenue_cents_enqueued_total",
			Help: "Cumulative order total (in cents) on the write side. Compared against the processor counter, divergence = backlog growth.",
		}),

		OrdersProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "russ_client_orders_processed_total",
			Help: "Orders fully applied by the processor (Lua script returned OK).",
		}),
		RevenueCentsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "russ_client_revenue_cents_processed_total",
			Help: "Cumulative order total (in cents) on the processor side.",
		}),
		LineItemsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "russ_client_line_items_processed_total",
			Help: "Cumulative line item quantities applied (sum of qty across all processed orders).",
		}),
		ProcessorApplyDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "russ_client_processor_apply_duration_seconds",
			Help:    "Wall-clock time for ApplyOrder (the Lua script round-trip).",
			Buckets: dur,
		}),
		ProcessorErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "russ_client_processor_errors_total",
			Help: "Asynq task failures by task type and error kind.",
		}, []string{"task", "kind"}),

		APIRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "russ_client_api_requests_total",
			Help: "HTTP requests served by the API, by route and status code class.",
		}, []string{"route", "status"}),
		APIRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "russ_client_api_request_duration_seconds",
			Help:    "HTTP request latency by route.",
			Buckets: dur,
		}, []string{"route"}),
	}
}

// Register adds all collectors to the registry along with the standard
// process_/go_ collectors.
func (m *Metrics) Register(reg prometheus.Registerer) {
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(
		m.WriterEnqueued, m.WriterEnqueueErrors, m.WriterEnqueuersActive, m.WriterWaveTarget, m.WriterRevenueCentsEnqueued,
		m.OrdersProcessed, m.RevenueCentsProcessed, m.LineItemsProcessed, m.ProcessorApplyDuration, m.ProcessorErrors,
		m.APIRequests, m.APIRequestDuration,
	)
}
