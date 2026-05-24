package orders

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

// API serves the JSON analytics endpoints + Prometheus /metrics.
type API struct {
	store    *Store
	metrics  *Metrics
	registry *prometheus.Registry
}

// NewAPI wires the read-only API to the store + metrics registry.
func NewAPI(store *Store, metrics *Metrics, registry *prometheus.Registry) *API {
	return &API{store: store, metrics: metrics, registry: registry}
}

// Routes returns an http.Handler with every endpoint mounted.
//
//	GET /api/analytics/summary                  → totals + AOV + unique customers
//	GET /api/analytics/top-products             → ?by=units|revenue (default units), ?limit=N
//	GET /api/analytics/top-categories           → ?by=units|revenue (default units), ?limit=N
//	GET /api/analytics/by-state                 → ?by=count|revenue (default count), ?limit=N
//	GET /api/analytics/timeseries               → ?hours=N (default 24, max 72)
//	GET /api/analytics/value-distribution       → fixed-bucket histogram
//	GET /api/analytics/top-orders/by-total      → top 10 orders by total
//	GET /api/analytics/top-orders/by-line-items → top 10 orders by cart qty
//	GET /api/analytics/top-orders/by-max-item   → top 10 orders by highest unit price
//	GET /api/analytics/cart-size-distribution   → qty-per-order histogram
//	GET /api/analytics/hour-of-day              → 24-bucket UTC heatmap
//	GET /api/analytics/day-of-week              → 7-bucket UTC heatmap
//	GET /api/analytics/top-customers            → ?by=spend|orders (default spend), ?limit=N
//	GET /api/analytics/aov-by-state             → ?limit=N (default all)
//	GET /api/analytics/top-zips                 → ?by=count|revenue (default count), ?limit=N
//	GET /api/analytics/revenue-concentration    → Pareto curve over the catalog
//	GET /api/analytics/top-pairs                → ?limit=N (default 20)
//	GET /api/analytics/new-vs-returning         → new vs repeat customer split
//	GET /metrics                                → Prometheus exposition
func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /api/analytics/summary", a.instrument("summary", a.summary))
	mux.Handle("GET /api/analytics/top-products", a.instrument("top_products", a.topProducts))
	mux.Handle("GET /api/analytics/top-categories", a.instrument("top_categories", a.topCategories))
	mux.Handle("GET /api/analytics/by-state", a.instrument("by_state", a.byState))
	mux.Handle("GET /api/analytics/timeseries", a.instrument("timeseries", a.timeseries))
	mux.Handle("GET /api/analytics/value-distribution", a.instrument("value_distribution", a.valueDistribution))
	mux.Handle("GET /api/analytics/top-orders/by-total", a.instrument("top_orders_total", a.topOrders("total")))
	mux.Handle("GET /api/analytics/top-orders/by-line-items", a.instrument("top_orders_line_items", a.topOrders("line_items")))
	mux.Handle("GET /api/analytics/top-orders/by-max-item", a.instrument("top_orders_max_item", a.topOrders("max_item")))
	mux.Handle("GET /api/analytics/cart-size-distribution", a.instrument("cart_size_distribution", a.cartSizeDistribution))
	mux.Handle("GET /api/analytics/hour-of-day", a.instrument("hour_of_day", a.hourOfDay))
	mux.Handle("GET /api/analytics/day-of-week", a.instrument("day_of_week", a.dayOfWeek))
	mux.Handle("GET /api/analytics/top-customers", a.instrument("top_customers", a.topCustomers))
	mux.Handle("GET /api/analytics/aov-by-state", a.instrument("aov_by_state", a.aovByState))
	mux.Handle("GET /api/analytics/top-zips", a.instrument("top_zips", a.topZips))
	mux.Handle("GET /api/analytics/revenue-concentration", a.instrument("revenue_concentration", a.revenueConcentration))
	mux.Handle("GET /api/analytics/top-pairs", a.instrument("top_pairs", a.topPairs))
	mux.Handle("GET /api/analytics/new-vs-returning", a.instrument("new_vs_returning", a.newVsReturning))
	mux.Handle("GET /metrics", promhttp.HandlerFor(a.registry, promhttp.HandlerOpts{Registry: a.registry}))
	return mux
}

// Serve blocks running an http.Server on addr until ctx is cancelled.
func (a *API) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: a.Routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Info().Str("addr", addr).Msg("api: listening")
	return srv.ListenAndServe()
}

// --- handlers ---

func (a *API) summary(w http.ResponseWriter, r *http.Request) {
	s, err := a.store.Summary(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, s)
}

func (a *API) topProducts(w http.ResponseWriter, r *http.Request) {
	by := r.URL.Query().Get("by")
	if by == "" {
		by = "units"
	}
	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 64)
	out, err := a.store.TopProducts(r.Context(), by, limit)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"by": by, "products": out})
}

func (a *API) topCategories(w http.ResponseWriter, r *http.Request) {
	by := r.URL.Query().Get("by")
	if by == "" {
		by = "units"
	}
	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 64)
	out, err := a.store.TopCategories(r.Context(), by, limit)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"by": by, "categories": out})
}

func (a *API) byState(w http.ResponseWriter, r *http.Request) {
	by := r.URL.Query().Get("by")
	if by == "" {
		by = "count"
	}
	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 64)
	out, err := a.store.ByState(r.Context(), by, limit)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"by": by, "states": out})
}

func (a *API) timeseries(w http.ResponseWriter, r *http.Request) {
	hours, _ := strconv.Atoi(r.URL.Query().Get("hours"))
	out, err := a.store.Timeseries(r.Context(), hours)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"hours": len(out), "buckets": out})
}

func (a *API) topOrders(by string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		out, err := a.store.TopOrders(r.Context(), by)
		if err != nil {
			jsonError(w, http.StatusInternalServerError, err.Error())
			return
		}
		jsonOK(w, map[string]any{"by": by, "orders": out})
	}
}

func (a *API) valueDistribution(w http.ResponseWriter, r *http.Request) {
	out, err := a.store.ValueDistribution(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"buckets": out})
}

func (a *API) cartSizeDistribution(w http.ResponseWriter, r *http.Request) {
	out, err := a.store.CartSizeDistribution(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"buckets": out})
}

func (a *API) hourOfDay(w http.ResponseWriter, r *http.Request) {
	out, err := a.store.HourOfDay(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"hours": out})
}

func (a *API) dayOfWeek(w http.ResponseWriter, r *http.Request) {
	out, err := a.store.DayOfWeek(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"days": out})
}

func (a *API) topCustomers(w http.ResponseWriter, r *http.Request) {
	by := r.URL.Query().Get("by")
	if by == "" {
		by = "spend"
	}
	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 64)
	out, err := a.store.TopCustomers(r.Context(), by, limit)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"by": by, "customers": out})
}

func (a *API) aovByState(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 64)
	out, err := a.store.AOVByState(r.Context(), limit)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"states": out})
}

func (a *API) topZips(w http.ResponseWriter, r *http.Request) {
	by := r.URL.Query().Get("by")
	if by == "" {
		by = "count"
	}
	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 64)
	out, err := a.store.TopZips(r.Context(), by, limit)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"by": by, "zips": out})
}

func (a *API) revenueConcentration(w http.ResponseWriter, r *http.Request) {
	out, err := a.store.RevenueConcentration(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, out)
}

func (a *API) topPairs(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 64)
	out, err := a.store.TopPairs(r.Context(), limit)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"pairs": out})
}

func (a *API) newVsReturning(w http.ResponseWriter, r *http.Request) {
	out, err := a.store.NewVsReturningSplit(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, out)
}

// --- helpers ---

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }

func (a *API) instrument(route string, fn http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: 200}
		fn(rec, r)
		a.metrics.APIRequests.WithLabelValues(route, statusClass(rec.code)).Inc()
		a.metrics.APIRequestDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
	})
}

func statusClass(code int) string {
	switch {
	case code < 200:
		return "1xx"
	case code < 300:
		return "2xx"
	case code < 400:
		return "3xx"
	case code < 500:
		return "4xx"
	default:
		return "5xx"
	}
}

func jsonOK(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func jsonError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
