// Package orders implements the russ-client workload as an asynq-based
// e-commerce orders analytics engine.
//
// Asynq is used **only as the work-distribution mechanism** — order tasks
// are ephemeral, consumed and removed by asynq after the handler returns
// nil. Individual orders, customers, and line items are NEVER stored
// durably in Redis. Only the *aggregate analytics* survive:
//
//	analytics:orders_count            — counter (INCR)
//	analytics:revenue_cents           — counter (INCRBY); cents to avoid float
//	analytics:tax_cents               — counter
//	analytics:shipping_cents          — counter
//	analytics:line_items_count        — counter (sum of all qty)
//	analytics:by_state:count          — zset state → orders
//	analytics:by_state:revenue        — zset state → revenue (cents)
//	analytics:top_products:units      — zset SKU → units sold
//	analytics:top_products:revenue    — zset SKU → revenue (cents)
//	analytics:top_categories:units    — zset category → units
//	analytics:top_categories:revenue  — zset category → revenue (cents)
//	analytics:product_metadata        — hash SKU → JSON {name, category}
//	analytics:unique_customers        — HyperLogLog of customer IDs
//	analytics:hour:<YYYY-MM-DD-HH>    — hash {orders=N, revenue_cents=N},
//	                                    TTL 72h for a rolling timeseries
//	analytics:order_value_buckets     — zset value-bucket → count
//	                                    ($0-25, $25-50, $50-100, $100-250,
//	                                     $250-500, $500-1000, $1000+)
//	analytics:top_orders:by_total     — zset (snapshot JSON → total cents), top 10
//	analytics:top_orders:by_line_items — zset (snapshot JSON → qty sum), top 10
//	analytics:top_orders:by_max_item  — zset (snapshot JSON → max unit cents), top 10
//	analytics:cart_size_buckets       — zset (label → count); cart-size histogram
//	analytics:hour_of_day:count       — hash {00..23 → orders}; UTC-hour heatmap
//	analytics:hour_of_day:revenue     — hash {00..23 → cents}
//	analytics:day_of_week:count       — hash {Sun..Sat → orders}; UTC weekday
//	analytics:day_of_week:revenue     — hash {Sun..Sat → cents}
//	analytics:top_customers:spend     — zset (customerID → cumulative cents)
//	analytics:top_customers:orders    — zset (customerID → order count)
//	analytics:customer_metadata       — hash customerID → JSON {name,email}
//	analytics:by_zip:count            — zset (zip → orders)
//	analytics:by_zip:revenue          — zset (zip → cents)
//	analytics:zip_metadata            — hash zip → JSON {city,state}
//	analytics:top_pairs               — zset ("skuA|skuB" → co-occurrences),
//	                                    capped at 200
//	analytics:seen_customers          — SET of customer IDs (drives new/returning)
//	analytics:orders_new              — counter (orders from first-seen customers)
//	analytics:orders_returning        — counter
//	analytics:revenue_new             — counter (cents)
//	analytics:revenue_returning       — counter (cents)
//
// Writers generate Order tasks; the processor's Lua script applies the
// full aggregate-update set atomically per order; the REST API serves
// summary + top-N queries off those structures.
package orders

import (
	"fmt"
	"time"
)

// TaskTypes used on asynq queues.
const (
	TaskProcessOrder = "order:process" // queue: orders_analytics
)

// Queue name.
const QueueOrdersAnalytics = "orders_analytics"

// Redis key namespace. All flat (no hash tags) since russ runs single-node
// Sentinel-managed clusters, not Cluster Mode.
const (
	KeyOrdersCount         = "analytics:orders_count"
	KeyRevenueCents        = "analytics:revenue_cents"
	KeyTaxCents            = "analytics:tax_cents"
	KeyShippingCents       = "analytics:shipping_cents"
	KeyLineItemsCount      = "analytics:line_items_count"
	KeyByStateCount        = "analytics:by_state:count"
	KeyByStateRevenue      = "analytics:by_state:revenue"
	KeyTopProductsUnits    = "analytics:top_products:units"
	KeyTopProductsRevenue  = "analytics:top_products:revenue"
	KeyTopCategoriesUnits  = "analytics:top_categories:units"
	KeyTopCategoriesRev    = "analytics:top_categories:revenue"
	KeyProductMetadata     = "analytics:product_metadata"
	KeyUniqueCustomersHLL  = "analytics:unique_customers"
	KeyHourBucketPrefix    = "analytics:hour:" // + YYYY-MM-DD-HH; TTL'd
	KeyOrderValueBuckets   = "analytics:order_value_buckets"
	KeyTopOrdersByTotal     = "analytics:top_orders:by_total"
	KeyTopOrdersByLineItems = "analytics:top_orders:by_line_items"
	KeyTopOrdersByMaxItem   = "analytics:top_orders:by_max_item"
	KeyCartSizeBuckets      = "analytics:cart_size_buckets"
	KeyHourOfDayCount       = "analytics:hour_of_day:count"
	KeyHourOfDayRevenue     = "analytics:hour_of_day:revenue"
	KeyDayOfWeekCount       = "analytics:day_of_week:count"
	KeyDayOfWeekRevenue     = "analytics:day_of_week:revenue"
	KeyTopCustomersSpend    = "analytics:top_customers:spend"
	KeyTopCustomersOrders   = "analytics:top_customers:orders"
	KeyCustomerMetadata     = "analytics:customer_metadata"
	KeyByZipCount           = "analytics:by_zip:count"
	KeyByZipRevenue         = "analytics:by_zip:revenue"
	KeyZipMetadata          = "analytics:zip_metadata"
	KeyTopPairs             = "analytics:top_pairs"
	KeySeenCustomers        = "analytics:seen_customers"
	KeyOrdersNew            = "analytics:orders_new"
	KeyOrdersReturning      = "analytics:orders_returning"
	KeyRevenueNew           = "analytics:revenue_new"
	KeyRevenueReturning     = "analytics:revenue_returning"
)

// TopOrdersLimit is the per-list cap maintained by the processor's Lua
// script (ZREMRANGEBYRANK trims everything below the top N).
const TopOrdersLimit = 10

// TopPairsLimit caps the analytics:top_pairs ZSET. Cardinality of all
// possible SKU pairs is catalog_size² / 2 — bounded but large; capping at
// 200 keeps memory predictable. Pairs that fall out lose their accumulated
// history (re-entry starts at score = current order's contribution).
const TopPairsLimit = 200

// HourBucketTTL is how long each per-hour bucket key lives in Redis. 72h
// = three days of rolling timeseries data. Old buckets self-prune as Redis
// applies the TTL.
const HourBucketTTL = 72 * time.Hour

// --- ephemeral domain types (carried in asynq task payloads, never stored) ---

// Product is one item in the writer's pre-generated catalog. Carried inside
// each line item so the processor can update product/category aggregates
// without a separate catalog-lookup round-trip.
type Product struct {
	SKU      string `json:"sku"`
	Name     string `json:"name"`
	Category string `json:"category"`
}

// LineItem is one product on an order: a Product + a quantity + the captured
// unit price at order time. SubtotalCents = Quantity × UnitPriceCents,
// pre-computed by the writer so the processor never has to divide.
type LineItem struct {
	Product        Product `json:"product"`
	Quantity       int     `json:"quantity"`
	UnitPriceCents int64   `json:"unit_price_cents"`
	SubtotalCents  int64   `json:"subtotal_cents"`
}

// Customer is the ordering party. ID is what gets fed into the HyperLogLog
// for unique-customer counting; it should be deterministic per real customer
// (we use a pre-generated pool to drive repeat-customer rate naturally).
type Customer struct {
	ID    string `json:"id"`    // pseudo-ID (e.g. SSN-shaped); HLL member
	Name  string `json:"name"`
	Email string `json:"email"`
	Phone string `json:"phone"`
}

// Address is a shipping address. Only the state is used by the analytics
// pipeline today — the rest is included so future API endpoints could
// surface "where this order shipped" without a schema change to the task
// payload.
type Address struct {
	Street    string  `json:"street"`
	City      string  `json:"city"`
	State     string  `json:"state"`
	Zip       string  `json:"zip"`
	Country   string  `json:"country"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// Order is the ephemeral task payload. Monetary amounts are integers in
// cents — pricing math has no business in floating point.
type Order struct {
	ID             string     `json:"id"`
	Customer       Customer   `json:"customer"`
	ShipTo         Address    `json:"ship_to"`
	LineItems      []LineItem `json:"line_items"`
	SubtotalCents  int64      `json:"subtotal_cents"`
	TaxCents       int64      `json:"tax_cents"`
	ShippingCents  int64      `json:"shipping_cents"`
	TotalCents     int64      `json:"total_cents"`
	CreatedAt      time.Time  `json:"created_at"`
}

// --- response shapes for the API ---

// AnalyticsSummary is the response shape for GET /api/analytics/summary.
type AnalyticsSummary struct {
	OrdersTotal           int64   `json:"orders_total"`
	RevenueUSDTotal       float64 `json:"revenue_usd_total"`
	TaxUSDTotal           float64 `json:"tax_usd_total"`
	ShippingUSDTotal      float64 `json:"shipping_usd_total"`
	LineItemsTotal        int64   `json:"line_items_total"`
	UniqueCustomers       int64   `json:"unique_customers"`
	AverageOrderValueUSD  float64 `json:"average_order_value_usd"`
}

// ProductBucket is one row in /api/analytics/top-products. Both `units` and
// `revenue_usd` are populated regardless of the sort key, so the caller
// always sees both dimensions.
type ProductBucket struct {
	SKU         string  `json:"sku"`
	Name        string  `json:"name"`
	Category    string  `json:"category"`
	Units       int64   `json:"units"`
	RevenueUSD  float64 `json:"revenue_usd"`
}

// CategoryBucket is one row in /api/analytics/top-categories.
type CategoryBucket struct {
	Category   string  `json:"category"`
	Units      int64   `json:"units"`
	RevenueUSD float64 `json:"revenue_usd"`
}

// StateBucket is one row in /api/analytics/by-state.
type StateBucket struct {
	State      string  `json:"state"`
	Orders     int64   `json:"orders"`
	RevenueUSD float64 `json:"revenue_usd"`
}

// HourBucket is one row in /api/analytics/timeseries.
type HourBucket struct {
	Hour       string  `json:"hour"` // YYYY-MM-DDTHH (UTC)
	Orders     int64   `json:"orders"`
	RevenueUSD float64 `json:"revenue_usd"`
}

// ValueBucket is one row in /api/analytics/value-distribution.
type ValueBucket struct {
	Label  string `json:"label"`  // e.g., "$0-25"
	Orders int64  `json:"orders"`
}

// OrderSnapshot is the compact per-order blob stored as the ZSET member in
// each of the analytics:top_orders:* lists. Stored as JSON so a single
// snapshot can populate all three lists; consumers parse + reshape at the
// API boundary. Monetary fields are integer cents.
type OrderSnapshot struct {
	ID           string `json:"id"`
	CustomerName string `json:"customer_name"`
	State        string `json:"state"`
	TotalCents   int64  `json:"total_cents"`
	LineItems    int    `json:"line_items"`     // sum of qty across the order
	MaxItemCents int64  `json:"max_item_cents"` // highest unit price in the order
	MaxItemName  string `json:"max_item_name"`  // product name at that unit price
	CreatedAt    string `json:"created_at"`     // RFC3339
}

// OrderBucket is one row in the /api/analytics/top-orders/* responses. Same
// fields as OrderSnapshot but with cents already converted to USD.
type OrderBucket struct {
	ID           string  `json:"id"`
	CustomerName string  `json:"customer_name"`
	State        string  `json:"state"`
	TotalUSD     float64 `json:"total_usd"`
	LineItems    int     `json:"line_items"`
	MaxItemUSD   float64 `json:"max_item_usd"`
	MaxItemName  string  `json:"max_item_name"`
	CreatedAt    string  `json:"created_at"`
}

// CartSizeBucket is one row in /api/analytics/cart-size-distribution.
type CartSizeBucket struct {
	Label  string `json:"label"`
	Orders int64  `json:"orders"`
}

// HourOfDayBucket is one row in /api/analytics/hour-of-day. Hours are
// labelled "00".."23" in UTC, returned in stable ascending order.
type HourOfDayBucket struct {
	Hour       string  `json:"hour"`
	Orders     int64   `json:"orders"`
	RevenueUSD float64 `json:"revenue_usd"`
}

// DayOfWeekBucket is one row in /api/analytics/day-of-week. Days are
// labelled "Sun".."Sat" in UTC and returned starting at Sunday.
type DayOfWeekBucket struct {
	Day        string  `json:"day"`
	Orders     int64   `json:"orders"`
	RevenueUSD float64 `json:"revenue_usd"`
}

// CustomerBucket is one row in /api/analytics/top-customers.
type CustomerBucket struct {
	CustomerID string  `json:"customer_id"`
	Name       string  `json:"name"`
	Email      string  `json:"email"`
	SpendUSD   float64 `json:"spend_usd"`
	Orders     int64   `json:"orders"`
}

// StateAOVBucket is one row in /api/analytics/aov-by-state.
type StateAOVBucket struct {
	State                string  `json:"state"`
	Orders               int64   `json:"orders"`
	RevenueUSD           float64 `json:"revenue_usd"`
	AverageOrderValueUSD float64 `json:"average_order_value_usd"`
}

// ZipBucket is one row in /api/analytics/top-zips. City/State are
// hydrated from analytics:zip_metadata at the API boundary.
type ZipBucket struct {
	Zip        string  `json:"zip"`
	City       string  `json:"city"`
	State      string  `json:"state"`
	Orders     int64   `json:"orders"`
	RevenueUSD float64 `json:"revenue_usd"`
}

// ConcentrationPoint is one row on the revenue-concentration curve.
// (top_products_pct → revenue_share_pct), e.g. "top 5% of products = 28% of revenue".
type ConcentrationPoint struct {
	TopProductsPct  float64 `json:"top_products_pct"`  // 1.0, 5.0, 10.0, …
	TopProducts     int     `json:"top_products"`      // count at that percentile
	RevenueSharePct float64 `json:"revenue_share_pct"` // cumulative share, 0–100
}

// RevenueConcentration is the response for /api/analytics/revenue-concentration.
type RevenueConcentration struct {
	TotalProducts   int                  `json:"total_products"`
	TotalRevenueUSD float64              `json:"total_revenue_usd"`
	Curve           []ConcentrationPoint `json:"curve"`
}

// PairBucket is one row in /api/analytics/top-pairs (market-basket
// co-occurrence). Names are hydrated from analytics:product_metadata.
type PairBucket struct {
	SKUA          string `json:"sku_a"`
	NameA         string `json:"name_a"`
	SKUB          string `json:"sku_b"`
	NameB         string `json:"name_b"`
	CoOccurrences int64  `json:"co_occurrences"`
}

// NewVsReturning is the response for /api/analytics/new-vs-returning.
// Shares are computed at the API boundary off the four underlying counters.
type NewVsReturning struct {
	NewOrders           int64   `json:"new_orders"`
	ReturningOrders     int64   `json:"returning_orders"`
	NewRevenueUSD       float64 `json:"new_revenue_usd"`
	ReturningRevenueUSD float64 `json:"returning_revenue_usd"`
	NewOrderSharePct    float64 `json:"new_order_share_pct"`
	NewRevenueSharePct  float64 `json:"new_revenue_share_pct"`
}

// customerMeta is the JSON shape stored in analytics:customer_metadata.
type customerMeta struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// zipMeta is the JSON shape stored in analytics:zip_metadata.
type zipMeta struct {
	City  string `json:"city"`
	State string `json:"state"`
}

// productMeta is the JSON shape stored in the analytics:product_metadata
// hash. The writer fills it in; the API reads it back for human-friendly
// product display.
type productMeta struct {
	Name     string `json:"name"`
	Category string `json:"category"`
}

// centsToUSD converts cents (int64) to dollars (float64). Used at the API
// boundary — internally we never divide.
func centsToUSD(cents int64) float64 { return float64(cents) / 100.0 }

// ValueBucketLabel returns the bucket label for an order total in cents.
// Buckets: $0-25, $25-50, $50-100, $100-250, $250-500, $500-1000, $1000+.
func ValueBucketLabel(totalCents int64) string {
	switch {
	case totalCents < 2500:
		return "$0-25"
	case totalCents < 5000:
		return "$25-50"
	case totalCents < 10000:
		return "$50-100"
	case totalCents < 25000:
		return "$100-250"
	case totalCents < 50000:
		return "$250-500"
	case totalCents < 100000:
		return "$500-1000"
	default:
		return "$1000+"
	}
}

// HourBucketKey returns the analytics:hour:<bucket> key for a given time
// (truncated to the hour, UTC).
func HourBucketKey(t time.Time) string {
	return KeyHourBucketPrefix + t.UTC().Format("2006-01-02T15")
}

// CartSizeLabels are the stable, ascending-order bucket labels for the
// cart-size histogram. Sized for the writer's defaults (MaxLineItems=5,
// qty 1–3 ⇒ max ~15 units per order) with one open-ended bucket on top.
var CartSizeLabels = []string{"1", "2-3", "4-5", "6-10", "11-15", "16+"}

// CartSizeBucketLabel returns the bucket label for a per-order quantity sum.
func CartSizeBucketLabel(qty int) string {
	switch {
	case qty <= 1:
		return "1"
	case qty <= 3:
		return "2-3"
	case qty <= 5:
		return "4-5"
	case qty <= 10:
		return "6-10"
	case qty <= 15:
		return "11-15"
	default:
		return "16+"
	}
}

// HoursOfDay lists the 24 hour labels in ascending order (UTC).
var HoursOfDay = func() []string {
	out := make([]string, 24)
	for i := 0; i < 24; i++ {
		out[i] = fmt.Sprintf("%02d", i)
	}
	return out
}()

// HourOfDayLabel returns the "00".."23" UTC-hour label for a time.
func HourOfDayLabel(t time.Time) string {
	return fmt.Sprintf("%02d", t.UTC().Hour())
}

// DaysOfWeek lists the 7 weekday labels starting at Sunday (UTC).
var DaysOfWeek = []string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}

// DayOfWeekLabel returns the "Sun".."Sat" UTC-weekday label for a time.
func DayOfWeekLabel(t time.Time) string {
	return DaysOfWeek[t.UTC().Weekday()]
}
