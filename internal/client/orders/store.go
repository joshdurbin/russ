package orders

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// applyOrderScript applies an entire order's analytics deltas in one atomic
// Redis round-trip. Doing it server-side means N orders processed
// concurrently can never partially-update one structure while leaving
// another behind — every counter, zset, HLL, and per-hour bucket either
// reflects the order or doesn't.
//
// KEYS:
//
//	[1]  analytics:orders_count
//	[2]  analytics:revenue_cents
//	[3]  analytics:tax_cents
//	[4]  analytics:shipping_cents
//	[5]  analytics:line_items_count
//	[6]  analytics:by_state:count
//	[7]  analytics:by_state:revenue
//	[8]  analytics:top_products:units
//	[9]  analytics:top_products:revenue
//	[10] analytics:top_categories:units
//	[11] analytics:top_categories:revenue
//	[12] analytics:product_metadata          (hash)
//	[13] analytics:unique_customers          (HyperLogLog)
//	[14] analytics:hour:<bucket>             (per-hour hash, TTL'd)
//	[15] analytics:order_value_buckets       (zset of value-bucket labels)
//	[16] analytics:top_orders:by_total       (zset, top 10 by order total)
//	[17] analytics:top_orders:by_line_items  (zset, top 10 by qty sum)
//	[18] analytics:top_orders:by_max_item    (zset, top 10 by max line item unit price)
//	[19] analytics:cart_size_buckets         (zset, label → count)
//	[20] analytics:hour_of_day:count         (hash, "00".."23" → orders)
//	[21] analytics:hour_of_day:revenue       (hash, "00".."23" → cents)
//	[22] analytics:day_of_week:count         (hash, "Sun".."Sat" → orders)
//	[23] analytics:day_of_week:revenue       (hash, "Sun".."Sat" → cents)
//	[24] analytics:top_customers:spend       (zset, customerID → cumulative cents)
//	[25] analytics:top_customers:orders      (zset, customerID → order count)
//	[26] analytics:customer_metadata         (hash, customerID → JSON)
//	[27] analytics:by_zip:count              (zset, zip → orders)
//	[28] analytics:by_zip:revenue            (zset, zip → cents)
//	[29] analytics:zip_metadata              (hash, zip → JSON)
//	[30] analytics:top_pairs                 (zset, "skuA|skuB" → count, capped)
//	[31] analytics:seen_customers            (SET of customer IDs)
//	[32] analytics:orders_new                (counter)
//	[33] analytics:orders_returning          (counter)
//	[34] analytics:revenue_new               (counter, cents)
//	[35] analytics:revenue_returning         (counter, cents)
//
// ARGV:
//
//	[1]  revenue_cents          (total)
//	[2]  tax_cents
//	[3]  shipping_cents
//	[4]  state
//	[5]  customer_id            (HLL member + new/returning probe)
//	[6]  hour_bucket_ttl_seconds
//	[7]  value_bucket_label
//	[8]  line_items_count       (= sum of qty across all line items)
//	[9]  line_items_json        (JSON array of {sku,name,category,qty,subtotal_cents})
//	[10] order_snapshot_json    (compact OrderSnapshot JSON for top-orders ZSETs)
//	[11] max_item_unit_cents    (score for analytics:top_orders:by_max_item)
//	[12] top_orders_limit       (cap; ZREMRANGEBYRANK trims below this)
//	[13] cart_size_bucket_label
//	[14] hour_of_day_label      ("00".."23")
//	[15] day_of_week_label      ("Sun".."Sat")
//	[16] customer_name
//	[17] customer_email
//	[18] zip
//	[19] city
//	[20] top_pairs_limit        (cap for analytics:top_pairs)
const applyOrderScript = `
local ordersCountKey = KEYS[1]
local revKey = KEYS[2]
local taxKey = KEYS[3]
local shipKey = KEYS[4]
local liCountKey = KEYS[5]
local byStateCountKey = KEYS[6]
local byStateRevKey = KEYS[7]
local topProdUnitsKey = KEYS[8]
local topProdRevKey = KEYS[9]
local topCatUnitsKey = KEYS[10]
local topCatRevKey = KEYS[11]
local prodMetaKey = KEYS[12]
local hllKey = KEYS[13]
local hourKey = KEYS[14]
local valueBucketsKey = KEYS[15]
local topOrdersByTotalKey = KEYS[16]
local topOrdersByLineItemsKey = KEYS[17]
local topOrdersByMaxItemKey = KEYS[18]
local cartSizeBucketsKey = KEYS[19]
local hourOfDayCountKey = KEYS[20]
local hourOfDayRevenueKey = KEYS[21]
local dayOfWeekCountKey = KEYS[22]
local dayOfWeekRevenueKey = KEYS[23]
local topCustomersSpendKey = KEYS[24]
local topCustomersOrdersKey = KEYS[25]
local customerMetadataKey = KEYS[26]
local byZipCountKey = KEYS[27]
local byZipRevenueKey = KEYS[28]
local zipMetadataKey = KEYS[29]
local topPairsKey = KEYS[30]
local seenCustomersKey = KEYS[31]
local ordersNewKey = KEYS[32]
local ordersReturningKey = KEYS[33]
local revenueNewKey = KEYS[34]
local revenueReturningKey = KEYS[35]

local revenueCents = tonumber(ARGV[1])
local taxCents = tonumber(ARGV[2])
local shippingCents = tonumber(ARGV[3])
local state = ARGV[4]
local customerID = ARGV[5]
local hourTTL = tonumber(ARGV[6])
local valueBucketLabel = ARGV[7]
local liCount = tonumber(ARGV[8])
local lineItems = cjson.decode(ARGV[9])
local orderSnapshot = ARGV[10]
local maxItemCents = tonumber(ARGV[11])
local topOrdersLimit = tonumber(ARGV[12])
local cartSizeLabel = ARGV[13]
local hourOfDayLabel = ARGV[14]
local dayOfWeekLabel = ARGV[15]
local customerName = ARGV[16]
local customerEmail = ARGV[17]
local zip = ARGV[18]
local city = ARGV[19]
local topPairsLimit = tonumber(ARGV[20])

-- Top-level counters
redis.call("INCR", ordersCountKey)
redis.call("INCRBY", revKey, revenueCents)
redis.call("INCRBY", taxKey, taxCents)
redis.call("INCRBY", shipKey, shippingCents)
redis.call("INCRBY", liCountKey, liCount)

-- By-state aggregates (count + revenue)
if state ~= "" then
	redis.call("ZINCRBY", byStateCountKey, 1, state)
	redis.call("ZINCRBY", byStateRevKey, revenueCents, state)
end

-- Per-line-item aggregates
for i = 1, #lineItems do
	local li = lineItems[i]
	local sku = li.sku
	local cat = li.category
	local qty = tonumber(li.quantity)
	local sub = tonumber(li.subtotal_cents)
	if sku and sku ~= "" then
		redis.call("ZINCRBY", topProdUnitsKey, qty, sku)
		redis.call("ZINCRBY", topProdRevKey, sub, sku)
		-- Cache product metadata for the API to render. HSETNX so we don't
		-- overwrite on every order; the first sighting wins.
		if li.name then
			redis.call("HSETNX", prodMetaKey, sku,
				cjson.encode({name = li.name, category = cat or ""}))
		end
	end
	if cat and cat ~= "" then
		redis.call("ZINCRBY", topCatUnitsKey, qty, cat)
		redis.call("ZINCRBY", topCatRevKey, sub, cat)
	end
end

-- Unique customer count via HyperLogLog
if customerID ~= "" then
	redis.call("PFADD", hllKey, customerID)
end

-- Per-hour rolling timeseries bucket
redis.call("HINCRBY", hourKey, "orders", 1)
redis.call("HINCRBY", hourKey, "revenue_cents", revenueCents)
redis.call("EXPIRE", hourKey, hourTTL)

-- Order-value distribution histogram
redis.call("ZINCRBY", valueBucketsKey, 1, valueBucketLabel)

-- Top-N order leaderboards. Each ZSET keeps only the top topOrdersLimit
-- entries by score; after each ZADD we trim the bottom via
-- ZREMRANGEBYRANK 0 -(topOrdersLimit+1).
local trimLow = -(topOrdersLimit + 1)
redis.call("ZADD", topOrdersByTotalKey, revenueCents, orderSnapshot)
redis.call("ZREMRANGEBYRANK", topOrdersByTotalKey, 0, trimLow)
redis.call("ZADD", topOrdersByLineItemsKey, liCount, orderSnapshot)
redis.call("ZREMRANGEBYRANK", topOrdersByLineItemsKey, 0, trimLow)
redis.call("ZADD", topOrdersByMaxItemKey, maxItemCents, orderSnapshot)
redis.call("ZREMRANGEBYRANK", topOrdersByMaxItemKey, 0, trimLow)

-- Cart-size histogram
redis.call("ZINCRBY", cartSizeBucketsKey, 1, cartSizeLabel)

-- Hour-of-day and day-of-week heatmaps (UTC)
redis.call("HINCRBY", hourOfDayCountKey, hourOfDayLabel, 1)
redis.call("HINCRBY", hourOfDayRevenueKey, hourOfDayLabel, revenueCents)
redis.call("HINCRBY", dayOfWeekCountKey, dayOfWeekLabel, 1)
redis.call("HINCRBY", dayOfWeekRevenueKey, dayOfWeekLabel, revenueCents)

-- Per-customer cumulative leaderboards (uncapped; bounded by writer's pool).
-- Metadata is HSETNX so the first sighting wins; cheaper than re-encoding.
if customerID ~= "" then
	redis.call("ZINCRBY", topCustomersSpendKey, revenueCents, customerID)
	redis.call("ZINCRBY", topCustomersOrdersKey, 1, customerID)
	if customerName ~= "" then
		redis.call("HSETNX", customerMetadataKey, customerID,
			cjson.encode({name = customerName, email = customerEmail}))
	end
end

-- Per-zip aggregates (uncapped; gofakeit's address pool is bounded in practice).
if zip ~= "" then
	redis.call("ZINCRBY", byZipCountKey, 1, zip)
	redis.call("ZINCRBY", byZipRevenueKey, revenueCents, zip)
	if city ~= "" then
		redis.call("HSETNX", zipMetadataKey, zip,
			cjson.encode({city = city, state = state}))
	end
end

-- Market-basket co-occurrence. Each unordered pair of distinct SKUs in the
-- order gets +1; we cap the ZSET at topPairsLimit. Lexicographic ordering
-- of the (a,b) members prevents double-counting.
local pairsTrimLow = -(topPairsLimit + 1)
for i = 1, #lineItems do
	for j = i + 1, #lineItems do
		local a = lineItems[i].sku
		local b = lineItems[j].sku
		if a and b and a ~= "" and b ~= "" and a ~= b then
			if a > b then a, b = b, a end
			redis.call("ZINCRBY", topPairsKey, 1, a .. "|" .. b)
		end
	end
end
redis.call("ZREMRANGEBYRANK", topPairsKey, 0, pairsTrimLow)

-- New-vs-returning split. SADD returns 1 on first sighting of customerID.
if customerID ~= "" then
	local isNew = redis.call("SADD", seenCustomersKey, customerID)
	if isNew == 1 then
		redis.call("INCR", ordersNewKey)
		redis.call("INCRBY", revenueNewKey, revenueCents)
	else
		redis.call("INCR", ordersReturningKey)
		redis.call("INCRBY", revenueReturningKey, revenueCents)
	end
end

return 1
`

// Store wraps the Redis client + the cached Lua script handle.
type Store struct {
	rdb         redis.UniversalClient
	applyScript *redis.Script
}

// NewStore returns a Store wired to the given Redis client.
func NewStore(rdb redis.UniversalClient) *Store {
	return &Store{rdb: rdb, applyScript: redis.NewScript(applyOrderScript)}
}

// liArg is the per-line-item shape the Lua script ingests. Pulled out into
// its own type so the JSON encoding is fast + predictable.
type liArg struct {
	SKU           string `json:"sku"`
	Name          string `json:"name"`
	Category      string `json:"category"`
	Quantity      int    `json:"quantity"`
	SubtotalCents int64  `json:"subtotal_cents"`
}

// ApplyOrder atomically applies all of one order's analytics deltas.
func (s *Store) ApplyOrder(ctx context.Context, o Order) error {
	lis := make([]liArg, len(o.LineItems))
	for i, li := range o.LineItems {
		lis[i] = liArg{
			SKU:           li.Product.SKU,
			Name:          li.Product.Name,
			Category:      li.Product.Category,
			Quantity:      li.Quantity,
			SubtotalCents: li.SubtotalCents,
		}
	}
	liJSON, err := json.Marshal(lis)
	if err != nil {
		return fmt.Errorf("marshal line items: %w", err)
	}

	totalLineItems := 0
	var maxItemCents int64
	var maxItemName string
	for _, li := range o.LineItems {
		totalLineItems += li.Quantity
		if li.UnitPriceCents > maxItemCents {
			maxItemCents = li.UnitPriceCents
			maxItemName = li.Product.Name
		}
	}

	snapshot := OrderSnapshot{
		ID:           o.ID,
		CustomerName: o.Customer.Name,
		State:        o.ShipTo.State,
		TotalCents:   o.TotalCents,
		LineItems:    totalLineItems,
		MaxItemCents: maxItemCents,
		MaxItemName:  maxItemName,
		CreatedAt:    o.CreatedAt.UTC().Format(time.RFC3339),
	}
	snapshotJSON, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal order snapshot: %w", err)
	}

	keys := []string{
		KeyOrdersCount, KeyRevenueCents, KeyTaxCents, KeyShippingCents, KeyLineItemsCount,
		KeyByStateCount, KeyByStateRevenue,
		KeyTopProductsUnits, KeyTopProductsRevenue,
		KeyTopCategoriesUnits, KeyTopCategoriesRev,
		KeyProductMetadata, KeyUniqueCustomersHLL,
		HourBucketKey(o.CreatedAt), KeyOrderValueBuckets,
		KeyTopOrdersByTotal, KeyTopOrdersByLineItems, KeyTopOrdersByMaxItem,
		KeyCartSizeBuckets,
		KeyHourOfDayCount, KeyHourOfDayRevenue,
		KeyDayOfWeekCount, KeyDayOfWeekRevenue,
		KeyTopCustomersSpend, KeyTopCustomersOrders, KeyCustomerMetadata,
		KeyByZipCount, KeyByZipRevenue, KeyZipMetadata,
		KeyTopPairs, KeySeenCustomers,
		KeyOrdersNew, KeyOrdersReturning, KeyRevenueNew, KeyRevenueReturning,
	}
	args := []any{
		o.TotalCents, o.TaxCents, o.ShippingCents,
		o.ShipTo.State, o.Customer.ID,
		int(HourBucketTTL / time.Second),
		ValueBucketLabel(o.TotalCents),
		totalLineItems,
		string(liJSON),
		string(snapshotJSON),
		maxItemCents,
		TopOrdersLimit,
		CartSizeBucketLabel(totalLineItems),
		HourOfDayLabel(o.CreatedAt),
		DayOfWeekLabel(o.CreatedAt),
		o.Customer.Name, o.Customer.Email,
		o.ShipTo.Zip, o.ShipTo.City,
		TopPairsLimit,
	}
	return s.applyScript.Run(ctx, s.rdb, keys, args...).Err()
}

// --- read paths for the API ---

// Summary returns top-level totals for /api/analytics/summary.
func (s *Store) Summary(ctx context.Context) (AnalyticsSummary, error) {
	pipe := s.rdb.Pipeline()
	orders := pipe.Get(ctx, KeyOrdersCount)
	rev := pipe.Get(ctx, KeyRevenueCents)
	tax := pipe.Get(ctx, KeyTaxCents)
	ship := pipe.Get(ctx, KeyShippingCents)
	li := pipe.Get(ctx, KeyLineItemsCount)
	uniq := pipe.PFCount(ctx, KeyUniqueCustomersHLL)
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return AnalyticsSummary{}, err
	}

	o := parseInt64OrZero(orders.Val())
	r := parseInt64OrZero(rev.Val())
	t := parseInt64OrZero(tax.Val())
	sh := parseInt64OrZero(ship.Val())
	lc := parseInt64OrZero(li.Val())
	u := uniq.Val()

	var aov float64
	if o > 0 {
		aov = centsToUSD(r) / float64(o)
	}
	return AnalyticsSummary{
		OrdersTotal:          o,
		RevenueUSDTotal:      centsToUSD(r),
		TaxUSDTotal:          centsToUSD(t),
		ShippingUSDTotal:     centsToUSD(sh),
		LineItemsTotal:       lc,
		UniqueCustomers:      u,
		AverageOrderValueUSD: round2(aov),
	}, nil
}

// TopProducts returns the top-N products sorted by either units or revenue,
// with the *other* dimension hydrated from the alternate zset so the caller
// always sees both numbers. Names + categories pulled from the metadata
// hash.
func (s *Store) TopProducts(ctx context.Context, by string, limit int64) ([]ProductBucket, error) {
	if limit <= 0 {
		limit = 10
	}
	primaryKey := KeyTopProductsUnits
	otherKey := KeyTopProductsRevenue
	if by == "revenue" {
		primaryKey, otherKey = KeyTopProductsRevenue, KeyTopProductsUnits
	}
	prim, err := s.rdb.ZRevRangeWithScores(ctx, primaryKey, 0, limit-1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]ProductBucket, 0, len(prim))
	skus := make([]string, len(prim))
	for i, p := range prim {
		skus[i] = fmt.Sprint(p.Member)
	}
	// Hydrate the alternate dimension in one pipeline.
	otherScores := make(map[string]float64, len(skus))
	if len(skus) > 0 {
		pipe := s.rdb.Pipeline()
		scoreCmds := make(map[string]*redis.FloatCmd, len(skus))
		for _, sku := range skus {
			scoreCmds[sku] = pipe.ZScore(ctx, otherKey, sku)
		}
		_, _ = pipe.Exec(ctx)
		for sku, c := range scoreCmds {
			if v, err := c.Result(); err == nil {
				otherScores[sku] = v
			}
		}
	}
	// Pull metadata in one HMGET.
	metas := map[string]productMeta{}
	if len(skus) > 0 {
		raw, _ := s.rdb.HMGet(ctx, KeyProductMetadata, skus...).Result()
		for i, sku := range skus {
			if i >= len(raw) || raw[i] == nil {
				continue
			}
			s, ok := raw[i].(string)
			if !ok {
				continue
			}
			var m productMeta
			if err := json.Unmarshal([]byte(s), &m); err == nil {
				metas[sku] = m
			}
		}
	}
	for _, p := range prim {
		sku, _ := p.Member.(string)
		var units int64
		var revCents int64
		if by == "revenue" {
			revCents = int64(p.Score)
			units = int64(otherScores[sku])
		} else {
			units = int64(p.Score)
			revCents = int64(otherScores[sku])
		}
		out = append(out, ProductBucket{
			SKU:        sku,
			Name:       metas[sku].Name,
			Category:   metas[sku].Category,
			Units:      units,
			RevenueUSD: centsToUSD(revCents),
		})
	}
	return out, nil
}

// TopCategories mirrors TopProducts for the category zsets.
func (s *Store) TopCategories(ctx context.Context, by string, limit int64) ([]CategoryBucket, error) {
	if limit <= 0 {
		limit = 10
	}
	primaryKey := KeyTopCategoriesUnits
	otherKey := KeyTopCategoriesRev
	if by == "revenue" {
		primaryKey, otherKey = KeyTopCategoriesRev, KeyTopCategoriesUnits
	}
	prim, err := s.rdb.ZRevRangeWithScores(ctx, primaryKey, 0, limit-1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]CategoryBucket, 0, len(prim))
	for _, p := range prim {
		cat, _ := p.Member.(string)
		other, _ := s.rdb.ZScore(ctx, otherKey, cat).Result()
		var units int64
		var revCents int64
		if by == "revenue" {
			revCents = int64(p.Score)
			units = int64(other)
		} else {
			units = int64(p.Score)
			revCents = int64(other)
		}
		out = append(out, CategoryBucket{Category: cat, Units: units, RevenueUSD: centsToUSD(revCents)})
	}
	return out, nil
}

// ByState returns the top-N states by count or revenue, hydrating the other
// dimension so the response always carries both.
func (s *Store) ByState(ctx context.Context, by string, limit int64) ([]StateBucket, error) {
	if limit <= 0 {
		limit = 20
	}
	primaryKey := KeyByStateCount
	otherKey := KeyByStateRevenue
	if by == "revenue" {
		primaryKey, otherKey = KeyByStateRevenue, KeyByStateCount
	}
	prim, err := s.rdb.ZRevRangeWithScores(ctx, primaryKey, 0, limit-1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]StateBucket, 0, len(prim))
	for _, p := range prim {
		st, _ := p.Member.(string)
		other, _ := s.rdb.ZScore(ctx, otherKey, st).Result()
		var orders int64
		var revCents int64
		if by == "revenue" {
			revCents = int64(p.Score)
			orders = int64(other)
		} else {
			orders = int64(p.Score)
			revCents = int64(other)
		}
		out = append(out, StateBucket{State: st, Orders: orders, RevenueUSD: centsToUSD(revCents)})
	}
	return out, nil
}

// Timeseries returns the most recent `hours` per-hour buckets. Missing
// hours (no data) are returned as zero-valued entries so the timeline is
// contiguous.
func (s *Store) Timeseries(ctx context.Context, hours int) ([]HourBucket, error) {
	if hours <= 0 {
		hours = 24
	}
	if hours > 72 {
		hours = 72 // matches the per-bucket TTL
	}
	now := time.Now().UTC().Truncate(time.Hour)
	buckets := make([]HourBucket, hours)
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, hours)
	keys := make([]string, hours)
	for i := 0; i < hours; i++ {
		t := now.Add(time.Duration(-(hours-1-i)) * time.Hour)
		keys[i] = HourBucketKey(t)
		buckets[i].Hour = t.Format("2006-01-02T15")
		cmds[i] = pipe.HGetAll(ctx, keys[i])
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	for i, c := range cmds {
		m, _ := c.Result()
		if v, ok := m["orders"]; ok {
			buckets[i].Orders = parseInt64OrZero(v)
		}
		if v, ok := m["revenue_cents"]; ok {
			buckets[i].RevenueUSD = centsToUSD(parseInt64OrZero(v))
		}
	}
	return buckets, nil
}

// TopOrders returns the top-N orders from one of the analytics:top_orders:*
// ZSETs (capped at TopOrdersLimit in Redis). The `by` argument selects which
// list:
//
//	"total"      → by_total (score = TotalCents)
//	"line_items" → by_line_items (score = sum of qty)
//	"max_item"   → by_max_item (score = highest unit price)
func (s *Store) TopOrders(ctx context.Context, by string) ([]OrderBucket, error) {
	key := KeyTopOrdersByTotal
	switch by {
	case "line_items":
		key = KeyTopOrdersByLineItems
	case "max_item":
		key = KeyTopOrdersByMaxItem
	}
	members, err := s.rdb.ZRevRange(ctx, key, 0, int64(TopOrdersLimit-1)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]OrderBucket, 0, len(members))
	for _, m := range members {
		var snap OrderSnapshot
		if err := json.Unmarshal([]byte(m), &snap); err != nil {
			continue
		}
		out = append(out, OrderBucket{
			ID:           snap.ID,
			CustomerName: snap.CustomerName,
			State:        snap.State,
			TotalUSD:     centsToUSD(snap.TotalCents),
			LineItems:    snap.LineItems,
			MaxItemUSD:   centsToUSD(snap.MaxItemCents),
			MaxItemName:  snap.MaxItemName,
			CreatedAt:    snap.CreatedAt,
		})
	}
	return out, nil
}

// ValueDistribution returns the order-value histogram as a stable-ordered
// list of buckets (matching the labels in ValueBucketLabel). Buckets with
// zero orders are included so the histogram is contiguous.
func (s *Store) ValueDistribution(ctx context.Context) ([]ValueBucket, error) {
	labels := []string{"$0-25", "$25-50", "$50-100", "$100-250", "$250-500", "$500-1000", "$1000+"}
	out := make([]ValueBucket, len(labels))
	for i, lbl := range labels {
		out[i].Label = lbl
		score, err := s.rdb.ZScore(ctx, KeyOrderValueBuckets, lbl).Result()
		if err == nil {
			out[i].Orders = int64(score)
		}
	}
	return out, nil
}

// CartSizeDistribution returns the cart-size histogram in stable label order.
// Empty buckets are returned as zeros so the histogram is contiguous.
func (s *Store) CartSizeDistribution(ctx context.Context) ([]CartSizeBucket, error) {
	out := make([]CartSizeBucket, len(CartSizeLabels))
	for i, lbl := range CartSizeLabels {
		out[i].Label = lbl
		score, err := s.rdb.ZScore(ctx, KeyCartSizeBuckets, lbl).Result()
		if err == nil {
			out[i].Orders = int64(score)
		}
	}
	return out, nil
}

// HourOfDay returns 24 buckets (00..23 UTC) of orders + revenue.
func (s *Store) HourOfDay(ctx context.Context) ([]HourOfDayBucket, error) {
	out := make([]HourOfDayBucket, len(HoursOfDay))
	pipe := s.rdb.Pipeline()
	countCmds := make([]*redis.StringCmd, len(HoursOfDay))
	revCmds := make([]*redis.StringCmd, len(HoursOfDay))
	for i, h := range HoursOfDay {
		out[i].Hour = h
		countCmds[i] = pipe.HGet(ctx, KeyHourOfDayCount, h)
		revCmds[i] = pipe.HGet(ctx, KeyHourOfDayRevenue, h)
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	for i := range HoursOfDay {
		out[i].Orders = parseInt64OrZero(countCmds[i].Val())
		out[i].RevenueUSD = centsToUSD(parseInt64OrZero(revCmds[i].Val()))
	}
	return out, nil
}

// DayOfWeek returns 7 buckets (Sun..Sat UTC) of orders + revenue.
func (s *Store) DayOfWeek(ctx context.Context) ([]DayOfWeekBucket, error) {
	out := make([]DayOfWeekBucket, len(DaysOfWeek))
	pipe := s.rdb.Pipeline()
	countCmds := make([]*redis.StringCmd, len(DaysOfWeek))
	revCmds := make([]*redis.StringCmd, len(DaysOfWeek))
	for i, d := range DaysOfWeek {
		out[i].Day = d
		countCmds[i] = pipe.HGet(ctx, KeyDayOfWeekCount, d)
		revCmds[i] = pipe.HGet(ctx, KeyDayOfWeekRevenue, d)
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	for i := range DaysOfWeek {
		out[i].Orders = parseInt64OrZero(countCmds[i].Val())
		out[i].RevenueUSD = centsToUSD(parseInt64OrZero(revCmds[i].Val()))
	}
	return out, nil
}

// TopCustomers returns the top-N customers from either the spend or orders
// ZSET, with the other dimension + name/email hydrated.
func (s *Store) TopCustomers(ctx context.Context, by string, limit int64) ([]CustomerBucket, error) {
	if limit <= 0 {
		limit = 10
	}
	primaryKey := KeyTopCustomersSpend
	otherKey := KeyTopCustomersOrders
	if by == "orders" {
		primaryKey, otherKey = KeyTopCustomersOrders, KeyTopCustomersSpend
	}
	prim, err := s.rdb.ZRevRangeWithScores(ctx, primaryKey, 0, limit-1).Result()
	if err != nil {
		return nil, err
	}
	if len(prim) == 0 {
		return []CustomerBucket{}, nil
	}
	ids := make([]string, len(prim))
	for i, p := range prim {
		ids[i], _ = p.Member.(string)
	}
	pipe := s.rdb.Pipeline()
	otherCmds := make([]*redis.FloatCmd, len(ids))
	for i, id := range ids {
		otherCmds[i] = pipe.ZScore(ctx, otherKey, id)
	}
	_, _ = pipe.Exec(ctx)
	metas := map[string]customerMeta{}
	raw, _ := s.rdb.HMGet(ctx, KeyCustomerMetadata, ids...).Result()
	for i, id := range ids {
		if i >= len(raw) || raw[i] == nil {
			continue
		}
		str, ok := raw[i].(string)
		if !ok {
			continue
		}
		var m customerMeta
		if err := json.Unmarshal([]byte(str), &m); err == nil {
			metas[id] = m
		}
	}
	out := make([]CustomerBucket, 0, len(prim))
	for i, p := range prim {
		id := ids[i]
		other, _ := otherCmds[i].Result()
		var spendCents int64
		var orderCount int64
		if by == "orders" {
			orderCount = int64(p.Score)
			spendCents = int64(other)
		} else {
			spendCents = int64(p.Score)
			orderCount = int64(other)
		}
		out = append(out, CustomerBucket{
			CustomerID: id,
			Name:       metas[id].Name,
			Email:      metas[id].Email,
			SpendUSD:   centsToUSD(spendCents),
			Orders:     orderCount,
		})
	}
	return out, nil
}

// AOVByState returns the top-N states by average order value (revenue /
// orders). Computed off the existing analytics:by_state ZSETs — no new
// keys needed; just a different read path.
func (s *Store) AOVByState(ctx context.Context, limit int64) ([]StateAOVBucket, error) {
	counts, err := s.rdb.ZRangeWithScores(ctx, KeyByStateCount, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	if len(counts) == 0 {
		return []StateAOVBucket{}, nil
	}
	pipe := s.rdb.Pipeline()
	revCmds := make([]*redis.FloatCmd, len(counts))
	for i, p := range counts {
		st, _ := p.Member.(string)
		revCmds[i] = pipe.ZScore(ctx, KeyByStateRevenue, st)
	}
	_, _ = pipe.Exec(ctx)
	all := make([]StateAOVBucket, 0, len(counts))
	for i, p := range counts {
		st, _ := p.Member.(string)
		orders := int64(p.Score)
		revCents := int64(0)
		if v, err := revCmds[i].Result(); err == nil {
			revCents = int64(v)
		}
		var aov float64
		if orders > 0 {
			aov = centsToUSD(revCents) / float64(orders)
		}
		all = append(all, StateAOVBucket{
			State:                st,
			Orders:               orders,
			RevenueUSD:           centsToUSD(revCents),
			AverageOrderValueUSD: round2(aov),
		})
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].AverageOrderValueUSD > all[j].AverageOrderValueUSD
	})
	if limit > 0 && int64(len(all)) > limit {
		all = all[:limit]
	}
	return all, nil
}

// TopZips returns the top-N zip codes by either count or revenue, with the
// other dimension + city/state hydrated.
func (s *Store) TopZips(ctx context.Context, by string, limit int64) ([]ZipBucket, error) {
	if limit <= 0 {
		limit = 20
	}
	primaryKey := KeyByZipCount
	otherKey := KeyByZipRevenue
	if by == "revenue" {
		primaryKey, otherKey = KeyByZipRevenue, KeyByZipCount
	}
	prim, err := s.rdb.ZRevRangeWithScores(ctx, primaryKey, 0, limit-1).Result()
	if err != nil {
		return nil, err
	}
	if len(prim) == 0 {
		return []ZipBucket{}, nil
	}
	zips := make([]string, len(prim))
	for i, p := range prim {
		zips[i], _ = p.Member.(string)
	}
	pipe := s.rdb.Pipeline()
	otherCmds := make([]*redis.FloatCmd, len(zips))
	for i, z := range zips {
		otherCmds[i] = pipe.ZScore(ctx, otherKey, z)
	}
	_, _ = pipe.Exec(ctx)
	metas := map[string]zipMeta{}
	raw, _ := s.rdb.HMGet(ctx, KeyZipMetadata, zips...).Result()
	for i, z := range zips {
		if i >= len(raw) || raw[i] == nil {
			continue
		}
		str, ok := raw[i].(string)
		if !ok {
			continue
		}
		var m zipMeta
		if err := json.Unmarshal([]byte(str), &m); err == nil {
			metas[z] = m
		}
	}
	out := make([]ZipBucket, 0, len(prim))
	for i, p := range prim {
		z := zips[i]
		other, _ := otherCmds[i].Result()
		var orders int64
		var revCents int64
		if by == "revenue" {
			revCents = int64(p.Score)
			orders = int64(other)
		} else {
			orders = int64(p.Score)
			revCents = int64(other)
		}
		out = append(out, ZipBucket{
			Zip:        z,
			City:       metas[z].City,
			State:      metas[z].State,
			Orders:     orders,
			RevenueUSD: centsToUSD(revCents),
		})
	}
	return out, nil
}

// RevenueConcentration computes a Pareto curve off analytics:top_products:revenue.
// Returns cumulative revenue share at top-1%/5%/10%/20%/50%/80% of products
// (and a "100%" anchor). Pure read-side; no new keys required.
func (s *Store) RevenueConcentration(ctx context.Context) (RevenueConcentration, error) {
	all, err := s.rdb.ZRevRangeWithScores(ctx, KeyTopProductsRevenue, 0, -1).Result()
	if err != nil {
		return RevenueConcentration{}, err
	}
	total := int64(0)
	for _, p := range all {
		total += int64(p.Score)
	}
	out := RevenueConcentration{
		TotalProducts:   len(all),
		TotalRevenueUSD: centsToUSD(total),
		Curve:           []ConcentrationPoint{},
	}
	if len(all) == 0 || total == 0 {
		return out, nil
	}
	percentiles := []float64{1, 5, 10, 20, 50, 80, 100}
	for _, pct := range percentiles {
		n := int(float64(len(all)) * pct / 100.0)
		if n < 1 {
			n = 1
		}
		if n > len(all) {
			n = len(all)
		}
		var cum int64
		for i := 0; i < n; i++ {
			cum += int64(all[i].Score)
		}
		share := float64(cum) * 100.0 / float64(total)
		out.Curve = append(out.Curve, ConcentrationPoint{
			TopProductsPct:  pct,
			TopProducts:     n,
			RevenueSharePct: round2(share),
		})
	}
	return out, nil
}

// TopPairs returns the top-N SKU co-occurrence pairs with product names
// hydrated from analytics:product_metadata.
func (s *Store) TopPairs(ctx context.Context, limit int64) ([]PairBucket, error) {
	if limit <= 0 {
		limit = 20
	}
	prim, err := s.rdb.ZRevRangeWithScores(ctx, KeyTopPairs, 0, limit-1).Result()
	if err != nil {
		return nil, err
	}
	if len(prim) == 0 {
		return []PairBucket{}, nil
	}
	skuSet := map[string]struct{}{}
	parts := make([][2]string, len(prim))
	for i, p := range prim {
		m, _ := p.Member.(string)
		split := strings.SplitN(m, "|", 2)
		if len(split) != 2 {
			continue
		}
		parts[i] = [2]string{split[0], split[1]}
		skuSet[split[0]] = struct{}{}
		skuSet[split[1]] = struct{}{}
	}
	skus := make([]string, 0, len(skuSet))
	for sku := range skuSet {
		skus = append(skus, sku)
	}
	metas := map[string]productMeta{}
	if len(skus) > 0 {
		raw, _ := s.rdb.HMGet(ctx, KeyProductMetadata, skus...).Result()
		for i, sku := range skus {
			if i >= len(raw) || raw[i] == nil {
				continue
			}
			str, ok := raw[i].(string)
			if !ok {
				continue
			}
			var m productMeta
			if err := json.Unmarshal([]byte(str), &m); err == nil {
				metas[sku] = m
			}
		}
	}
	out := make([]PairBucket, 0, len(prim))
	for i, p := range prim {
		if parts[i][0] == "" {
			continue
		}
		out = append(out, PairBucket{
			SKUA:          parts[i][0],
			NameA:         metas[parts[i][0]].Name,
			SKUB:          parts[i][1],
			NameB:         metas[parts[i][1]].Name,
			CoOccurrences: int64(p.Score),
		})
	}
	return out, nil
}

// NewVsReturningSplit reads the four new/returning counters and computes
// the share percentages at the API boundary.
func (s *Store) NewVsReturningSplit(ctx context.Context) (NewVsReturning, error) {
	pipe := s.rdb.Pipeline()
	newOrd := pipe.Get(ctx, KeyOrdersNew)
	retOrd := pipe.Get(ctx, KeyOrdersReturning)
	newRev := pipe.Get(ctx, KeyRevenueNew)
	retRev := pipe.Get(ctx, KeyRevenueReturning)
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return NewVsReturning{}, err
	}
	nO := parseInt64OrZero(newOrd.Val())
	rO := parseInt64OrZero(retOrd.Val())
	nRcents := parseInt64OrZero(newRev.Val())
	rRcents := parseInt64OrZero(retRev.Val())
	totalOrd := nO + rO
	totalRev := nRcents + rRcents
	out := NewVsReturning{
		NewOrders:           nO,
		ReturningOrders:     rO,
		NewRevenueUSD:       centsToUSD(nRcents),
		ReturningRevenueUSD: centsToUSD(rRcents),
	}
	if totalOrd > 0 {
		out.NewOrderSharePct = round2(float64(nO) * 100.0 / float64(totalOrd))
	}
	if totalRev > 0 {
		out.NewRevenueSharePct = round2(float64(nRcents) * 100.0 / float64(totalRev))
	}
	return out, nil
}

// --- helpers ---

func parseInt64OrZero(s string) int64 {
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return v
}

// round2 trims a float to 2 decimal places at the response boundary —
// purely cosmetic, the underlying numbers are exact integer cents.
func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100.0
}
