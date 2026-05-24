package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bigcommerce/russ/internal/docker"
	"github.com/spf13/cobra"
)

// Analytics subcommand tree under `russ client`. Each leaf maps one-to-one
// to a /api/analytics/* endpoint on the running russ-client container.
// Defaults to http://127.0.0.1:9300 (the default --api-host-port); pass
// --cluster <name> to discover the host-published port via Docker — useful
// when multiple workload containers run on non-default ports.

var clientAnalyticsCmd = &cobra.Command{
	Use:   "analytics",
	Short: "Query the russ-client analytics REST API",
	Long: `Subcommand tree mirroring the /api/analytics/* REST endpoints exposed
by the russ-client container. The output is the raw pretty-printed JSON
response from the server.

By default targets http://127.0.0.1:9300 (the default --api-host-port).
Pass --cluster <name> to discover the workload container's host-published
port via Docker — useful when multiple clusters run on non-default ports.`,
}

// paramSpec drives the cobra flag set + query string assembly for one
// endpoint. allowed is reported in the flag usage line; the server does
// the actual validation.
type paramSpec struct {
	name    string
	def     string
	allowed []string
	desc    string
}

func (p paramSpec) usage() string {
	if len(p.allowed) > 0 {
		return fmt.Sprintf("%s (%s)", p.desc, strings.Join(p.allowed, "|"))
	}
	return p.desc
}

func newAnalyticsCmd(use, short, path string, params []paramSpec) *cobra.Command {
	c := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			base, err := resolveAnalyticsAddr(cmd)
			if err != nil {
				return err
			}
			q := url.Values{}
			for _, p := range params {
				v, _ := cmd.Flags().GetString(p.name)
				if v == "" || (p.def != "" && v == p.def) {
					// Only forward non-default values so the server uses its
					// own defaults. (Matches what a hand-written curl would do.)
					continue
				}
				q.Set(p.name, v)
			}
			u := base + path
			if len(q) > 0 {
				u += "?" + q.Encode()
			}
			return fetchAndPrintJSON(cmd.Context(), u)
		},
	}
	for _, p := range params {
		c.Flags().String(p.name, p.def, p.usage())
	}
	return c
}

func resolveAnalyticsAddr(cmd *cobra.Command) (string, error) {
	cluster, _ := cmd.Flags().GetString("cluster")
	if cluster != "" {
		ctx := cmd.Context()
		dm, err := docker.New()
		if err != nil {
			return "", err
		}
		defer dm.Close()
		addr, err := dm.WorkloadAPIAddr(ctx, cluster)
		if err != nil {
			return "", err
		}
		if addr == "" {
			return "", fmt.Errorf("cluster %q has no host-published API port (started with --api-host-port=0?)", cluster)
		}
		return addr, nil
	}
	addr, _ := cmd.Flags().GetString("addr")
	return strings.TrimRight(addr, "/"), nil
}

func fetchAndPrintJSON(ctx context.Context, u string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", u, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, u, strings.TrimSpace(string(body)))
	}
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		// Fall through to raw output if it isn't JSON — shouldn't happen
		// from the analytics API, but keeps the tool useful if the user
		// points --addr at something else.
		fmt.Println(string(body))
		return nil
	}
	out, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(out))
	return nil
}

func init() {
	clientAnalyticsCmd.PersistentFlags().String("addr", "http://127.0.0.1:9300", "Base URL of the russ-client API")
	clientAnalyticsCmd.PersistentFlags().String("cluster", "", "Cluster name; overrides --addr by querying Docker for the workload container's host-published port")

	topOrdersCmd := &cobra.Command{
		Use:   "top-orders",
		Short: "Order leaderboards (top 10 by various metrics)",
	}
	topOrdersCmd.AddCommand(
		newAnalyticsCmd("by-total", "Top 10 most expensive orders", "/api/analytics/top-orders/by-total", nil),
		newAnalyticsCmd("by-line-items", "Top 10 orders by total quantity in cart", "/api/analytics/top-orders/by-line-items", nil),
		newAnalyticsCmd("by-max-item", "Top 10 orders by the single most expensive product (max unit price)", "/api/analytics/top-orders/by-max-item", nil),
	)

	clientAnalyticsCmd.AddCommand(
		newAnalyticsCmd("summary", "Top-level totals: orders, revenue/tax/shipping, line items, unique customers, AOV", "/api/analytics/summary", nil),
		newAnalyticsCmd("top-products", "Top-N products ranked by units sold or revenue", "/api/analytics/top-products", []paramSpec{
			{name: "by", def: "units", allowed: []string{"units", "revenue"}, desc: "sort dimension"},
			{name: "limit", def: "10", desc: "max rows to return"},
		}),
		newAnalyticsCmd("top-categories", "Top-N product categories", "/api/analytics/top-categories", []paramSpec{
			{name: "by", def: "units", allowed: []string{"units", "revenue"}, desc: "sort dimension"},
			{name: "limit", def: "10", desc: "max rows to return"},
		}),
		newAnalyticsCmd("by-state", "Orders + revenue per US state", "/api/analytics/by-state", []paramSpec{
			{name: "by", def: "count", allowed: []string{"count", "revenue"}, desc: "sort dimension"},
			{name: "limit", def: "20", desc: "max rows to return"},
		}),
		newAnalyticsCmd("timeseries", "Per-hour rolling timeseries (orders + revenue)", "/api/analytics/timeseries", []paramSpec{
			{name: "hours", def: "24", desc: "trailing window in hours (1-72)"},
		}),
		newAnalyticsCmd("value-distribution", "Order-value histogram across fixed dollar buckets", "/api/analytics/value-distribution", nil),
		topOrdersCmd,
		newAnalyticsCmd("cart-size-distribution", "Histogram of total quantity per order", "/api/analytics/cart-size-distribution", nil),
		newAnalyticsCmd("hour-of-day", "Orders + revenue bucketed by UTC hour of day", "/api/analytics/hour-of-day", nil),
		newAnalyticsCmd("day-of-week", "Orders + revenue bucketed by UTC weekday", "/api/analytics/day-of-week", nil),
		newAnalyticsCmd("top-customers", "Top-N customers ranked by cumulative spend or order count", "/api/analytics/top-customers", []paramSpec{
			{name: "by", def: "spend", allowed: []string{"spend", "orders"}, desc: "sort dimension"},
			{name: "limit", def: "10", desc: "max rows to return"},
		}),
		newAnalyticsCmd("aov-by-state", "States ranked by average order value", "/api/analytics/aov-by-state", []paramSpec{
			{name: "limit", def: "", desc: "max rows to return (omit for all states)"},
		}),
		newAnalyticsCmd("top-zips", "Top-N zip codes by order count or revenue", "/api/analytics/top-zips", []paramSpec{
			{name: "by", def: "count", allowed: []string{"count", "revenue"}, desc: "sort dimension"},
			{name: "limit", def: "20", desc: "max rows to return"},
		}),
		newAnalyticsCmd("revenue-concentration", "Pareto curve over the product catalog", "/api/analytics/revenue-concentration", nil),
		newAnalyticsCmd("top-pairs", "Market-basket co-occurrence: top SKU pairs", "/api/analytics/top-pairs", []paramSpec{
			{name: "limit", def: "20", desc: "max rows to return"},
		}),
		newAnalyticsCmd("new-vs-returning", "Order/revenue split between first-seen and repeat customers", "/api/analytics/new-vs-returning", nil),
	)

	clientCmd.AddCommand(clientAnalyticsCmd)
}
