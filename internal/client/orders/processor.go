package orders

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/rs/zerolog/log"
)

// ProcessorConfig governs the asynq Server that consumes orders_analytics.
type ProcessorConfig struct {
	SentinelAddrs []string
	MasterName    string

	Concurrency int           // worker goroutine ceiling; 0 = 100
	Timeout     time.Duration // per-task timeout; 0 = 30s
	MaxRetry    int           // asynq.MaxRetry; 0 = 3
}

// Processor wires an asynq Server to the order:process handler. There is
// only one queue (orders_analytics) — the previous two-queue persons design
// is gone because we don't need a separate stats roll-up step: every
// analytics aggregate is updated atomically by the per-order Lua script.
type Processor struct {
	cfg     ProcessorConfig
	store   *Store
	srv     *asynq.Server
	mux     *asynq.ServeMux
	metrics *Metrics
}

// NewProcessor constructs (but does not start) the server. Use Run.
func NewProcessor(cfg ProcessorConfig, store *Store, metrics *Metrics) *Processor {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 100
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MaxRetry <= 0 {
		cfg.MaxRetry = 3
	}

	rOpt := asynq.RedisFailoverClientOpt{
		MasterName:    cfg.MasterName,
		SentinelAddrs: cfg.SentinelAddrs,
	}
	srv := asynq.NewServer(rOpt, asynq.Config{
		Concurrency: cfg.Concurrency,
		Queues:      map[string]int{QueueOrdersAnalytics: 10},
		LogLevel:    asynq.WarnLevel,
		ErrorHandler: asynq.ErrorHandlerFunc(func(_ context.Context, task *asynq.Task, err error) {
			metrics.ProcessorErrors.WithLabelValues(task.Type(), classifyEnqueueErr(err)).Inc()
			log.Warn().Str("task", task.Type()).Err(err).Msg("task failed")
		}),
	})

	p := &Processor{
		cfg:     cfg,
		store:   store,
		srv:     srv,
		mux:     asynq.NewServeMux(),
		metrics: metrics,
	}
	p.mux.HandleFunc(TaskProcessOrder, p.handleOrder)
	return p
}

// Run blocks until ctx is cancelled.
func (p *Processor) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() { errCh <- p.srv.Run(p.mux) }()
	select {
	case <-ctx.Done():
		log.Info().Msg("processor: shutting down")
		p.srv.Shutdown()
		return nil
	case err := <-errCh:
		return err
	}
}

// handleOrder decodes the order payload and hands it to the Lua-driven
// store. Single round-trip per order, every aggregate either reflects the
// order or doesn't — no partial-update windows.
func (p *Processor) handleOrder(ctx context.Context, t *asynq.Task) error {
	start := time.Now()
	var o Order
	if err := json.Unmarshal(t.Payload(), &o); err != nil {
		return fmt.Errorf("decode order:process: %w", err)
	}
	if err := p.store.ApplyOrder(ctx, o); err != nil {
		p.metrics.ProcessorApplyDuration.Observe(time.Since(start).Seconds())
		return fmt.Errorf("apply order: %w", err)
	}
	p.metrics.ProcessorApplyDuration.Observe(time.Since(start).Seconds())
	p.metrics.OrdersProcessed.Inc()
	p.metrics.RevenueCentsProcessed.Add(float64(o.TotalCents))
	p.metrics.LineItemsProcessed.Add(float64(totalLineItems(o)))
	return nil
}

func totalLineItems(o Order) int {
	n := 0
	for _, li := range o.LineItems {
		n += li.Quantity
	}
	return n
}
