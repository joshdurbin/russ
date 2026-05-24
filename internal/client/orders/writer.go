package orders

import (
	"context"
	"encoding/json"
	"math"
	"math/rand"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog/log"
)

// WriterConfig governs the wave-scaled enqueuer pool.
type WriterConfig struct {
	SentinelAddrs []string
	MasterName    string

	MinClients    int           // pool size at wave trough
	MaxClients    int           // pool size at wave peak
	WavePeriod    time.Duration // full cosine cycle (trough → peak → trough)
	MinClientTTL  time.Duration // minimum per-enqueuer lifetime
	MaxClientTTL  time.Duration // maximum per-enqueuer lifetime
	Tick          time.Duration // per-enqueuer interval between enqueue attempts; 0 = no throttle
	Burst         bool          // skip wave, jump straight to MaxClients
	CustomerPool  int           // pre-generated customer pool size (drives repeat-customer rate)
	CatalogSize   int           // pre-generated product catalog size
	MinLineItems  int           // per-order minimum line item count
	MaxLineItems  int           // per-order maximum line item count
	TaskRetention time.Duration // asynq.Retention duration for completed order:process tasks
	TaskTimeout   time.Duration // asynq.Timeout for a single order:process task
	TaskMaxRetry  int           // asynq.MaxRetry for order:process tasks
}

// Writer emits order:process tasks onto the orders_analytics queue.
type Writer struct {
	cfg       WriterConfig
	client    *asynq.Client
	metrics   *Metrics
	rng       *rand.Rand
	customers []Customer
	cat       *catalog
}

// NewWriter constructs a Writer (does not start it). Use Run.
func NewWriter(cfg WriterConfig, metrics *Metrics) *Writer {
	if cfg.MinClients < 1 {
		cfg.MinClients = 1
	}
	if cfg.MaxClients < cfg.MinClients {
		cfg.MaxClients = cfg.MinClients
	}
	if cfg.WavePeriod <= 0 {
		cfg.WavePeriod = 10 * time.Minute
	}
	if cfg.MinClientTTL <= 0 {
		cfg.MinClientTTL = 2 * time.Second
	}
	if cfg.MaxClientTTL <= 0 || cfg.MaxClientTTL < cfg.MinClientTTL {
		cfg.MaxClientTTL = 30 * time.Second
	}
	if cfg.Tick < 0 {
		cfg.Tick = 0
	}
	if cfg.CustomerPool <= 0 {
		cfg.CustomerPool = 5000
	}
	if cfg.CatalogSize <= 0 {
		cfg.CatalogSize = 500
	}
	if cfg.MinLineItems <= 0 {
		cfg.MinLineItems = 1
	}
	if cfg.MaxLineItems <= 0 || cfg.MaxLineItems < cfg.MinLineItems {
		cfg.MaxLineItems = 5
	}
	if cfg.TaskRetention <= 0 {
		cfg.TaskRetention = 15 * time.Minute
	}
	if cfg.TaskTimeout <= 0 {
		cfg.TaskTimeout = 30 * time.Second
	}

	client := asynq.NewClient(asynq.RedisFailoverClientOpt{
		MasterName:    cfg.MasterName,
		SentinelAddrs: cfg.SentinelAddrs,
	})

	w := &Writer{
		cfg:     cfg,
		client:  client,
		metrics: metrics,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
		cat:     newCatalog(cfg.CatalogSize),
	}
	w.customers = w.buildCustomerPool(cfg.CustomerPool)
	return w
}

// Close releases the asynq client.
func (w *Writer) Close() error { return w.client.Close() }

// Run blocks until ctx is cancelled. Coordinator goroutine ticks every
// 200 ms to recompute the wave target and submits enqueuer tasks into a
// pond pool whose max-concurrency is cfg.MaxClients.
func (w *Writer) Run(ctx context.Context) {
	pool := pond.NewPool(w.cfg.MaxClients)
	defer pool.StopAndWait()

	start := time.Now()
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()

	id := 0
	lastTarget := -1
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("writer: shutting down")
			return
		case <-t.C:
			target := w.targetClients(time.Since(start))
			if target != lastTarget {
				log.Info().
					Int("target", target).
					Int64("running", pool.RunningWorkers()).
					Msg("writer: wave target")
				w.metrics.WriterWaveTarget.Set(float64(target))
				lastTarget = target
			}
			for int(pool.RunningWorkers()) < target {
				id++
				myID := id
				pool.Submit(func() { w.runEnqueuer(ctx, myID) })
			}
		}
	}
}

func (w *Writer) targetClients(elapsed time.Duration) int {
	if w.cfg.Burst {
		return w.cfg.MaxClients
	}
	phase := 2 * math.Pi * float64(elapsed) / float64(w.cfg.WavePeriod)
	span := float64(w.cfg.MaxClients - w.cfg.MinClients)
	return w.cfg.MinClients + int(span*0.5*(1-math.Cos(phase)))
}

func (w *Writer) runEnqueuer(ctx context.Context, id int) {
	cctx := ctx
	if !w.cfg.Burst {
		lifetime := randDur(w.cfg.MinClientTTL, w.cfg.MaxClientTTL)
		var cancel context.CancelFunc
		cctx, cancel = context.WithTimeout(ctx, lifetime)
		defer cancel()
		log.Debug().Int("id", id).Dur("lifetime", lifetime).Msg("enqueuer up")
	}
	defer log.Debug().Int("id", id).Msg("enqueuer down")
	w.metrics.WriterEnqueuersActive.Inc()
	defer w.metrics.WriterEnqueuersActive.Dec()

	emit := func() {
		order := w.newOrder()
		body, err := json.Marshal(order)
		if err != nil {
			log.Error().Int("id", id).Err(err).Msg("marshal err")
			return
		}
		task := asynq.NewTask(
			TaskProcessOrder,
			body,
			asynq.Queue(QueueOrdersAnalytics),
			asynq.Retention(w.cfg.TaskRetention),
			asynq.Timeout(w.cfg.TaskTimeout),
			asynq.MaxRetry(w.cfg.TaskMaxRetry),
		)
		if _, err := w.client.EnqueueContext(cctx, task); err != nil {
			if cctx.Err() == nil {
				log.Warn().Int("id", id).Err(err).Msg("enqueue err")
				w.metrics.WriterEnqueueErrors.WithLabelValues(classifyEnqueueErr(err)).Inc()
			}
			return
		}
		w.metrics.WriterEnqueued.Inc()
		w.metrics.WriterRevenueCentsEnqueued.Add(float64(order.TotalCents))
	}

	if w.cfg.Tick == 0 {
		for cctx.Err() == nil {
			emit()
		}
		return
	}
	t := time.NewTicker(w.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-cctx.Done():
			return
		case <-t.C:
			emit()
		}
	}
}

// buildCustomerPool generates a fixed pool of pseudo-realistic customers
// at startup. Each enqueued order picks one at random — the pool's size
// determines repeat-customer rate, which in turn determines HyperLogLog
// drift vs raw order count.
func (w *Writer) buildCustomerPool(n int) []Customer {
	out := make([]Customer, n)
	for i := range out {
		out[i] = Customer{
			ID:    gofakeit.SSN(), // SSN-shaped string, stable per pool entry
			Name:  gofakeit.Name(),
			Email: gofakeit.Email(),
			Phone: gofakeit.PhoneFormatted(),
		}
	}
	return out
}

// newOrder synthesizes one ephemeral Order. Line items are picked from the
// fixed catalog so SKUs repeat across orders (top-N analytics light up
// quickly), but ship-to address is fresh per order (so by-state stays
// distributed). Tax = 8% of subtotal; shipping = $4.99 flat — both kept
// simple because they're not the point.
func (w *Writer) newOrder() Order {
	now := time.Now().UTC()
	cust := w.customers[w.rng.Intn(len(w.customers))]
	addr := gofakeit.Address()
	count := w.cfg.MinLineItems + w.rng.Intn(w.cfg.MaxLineItems-w.cfg.MinLineItems+1)
	lineItems := make([]LineItem, count)
	var subtotal int64
	for i := 0; i < count; i++ {
		prod, unit := w.cat.pick(w.rng)
		qty := 1 + w.rng.Intn(3) // 1..3 of each
		sub := unit * int64(qty)
		lineItems[i] = LineItem{
			Product:        prod,
			Quantity:       qty,
			UnitPriceCents: unit,
			SubtotalCents:  sub,
		}
		subtotal += sub
	}
	tax := subtotal * 8 / 100   // flat 8%
	shipping := int64(499)      // $4.99 flat
	total := subtotal + tax + shipping
	return Order{
		ID:       uuid.NewString(),
		Customer: cust,
		ShipTo: Address{
			Street:    addr.Street,
			City:      addr.City,
			State:     addr.State,
			Zip:       addr.Zip,
			Country:   addr.Country,
			Latitude:  addr.Latitude,
			Longitude: addr.Longitude,
		},
		LineItems:     lineItems,
		SubtotalCents: subtotal,
		TaxCents:      tax,
		ShippingCents: shipping,
		TotalCents:    total,
		CreatedAt:     now,
	}
}

func randDur(lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rand.Int63n(int64(hi-lo)))
}

func classifyEnqueueErr(err error) string {
	s := err.Error()
	switch {
	case contains(s, "context"):
		return "context"
	case contains(s, "EOF"), contains(s, "connection"):
		return "network"
	default:
		return "other"
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
