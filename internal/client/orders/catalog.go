package orders

import (
	"fmt"
	"math/rand"

	"github.com/brianvoe/gofakeit/v7"
)

// catalog is a pre-generated fixed-size product pool the writer draws from.
// Realistic e-commerce stores have a stable SKU catalog; orders pick from
// it rather than inventing a new SKU per order. This drives meaningful
// top-N product analytics — the same SKUs appear repeatedly with varying
// quantities, which is exactly what the zsets aggregate.
type catalog struct {
	products []Product
	// Per-product unit price in cents, parallel to products[]. Generated
	// once at catalog-build time so the same SKU has the same price across
	// orders (modulo any per-order discounting we'd add later).
	priceCents []int64
}

// newCatalog generates `size` fake products spanning a handful of realistic
// e-commerce categories.
func newCatalog(size int) *catalog {
	if size <= 0 {
		size = 500
	}
	categories := []string{
		"Electronics", "Home & Kitchen", "Books", "Clothing",
		"Toys & Games", "Sports & Outdoors", "Beauty",
		"Grocery", "Office", "Garden",
	}
	c := &catalog{
		products:   make([]Product, size),
		priceCents: make([]int64, size),
	}
	for i := 0; i < size; i++ {
		cat := categories[i%len(categories)]
		c.products[i] = Product{
			SKU:      fmt.Sprintf("SKU-%06d", i),
			Name:     gofakeit.ProductName(),
			Category: cat,
		}
		// Realistic price spread: median ~$25, long tail to $500. We use
		// integer cents end-to-end, so generate the float once and convert.
		dollars := gofakeit.Price(1.99, 499.99)
		c.priceCents[i] = int64(dollars*100 + 0.5)
	}
	return c
}

// pick returns a random product + its unit price in cents.
func (c *catalog) pick(rng *rand.Rand) (Product, int64) {
	i := rng.Intn(len(c.products))
	return c.products[i], c.priceCents[i]
}

// size reports the catalog's product count.
func (c *catalog) size() int { return len(c.products) }
