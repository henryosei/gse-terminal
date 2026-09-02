package server

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strings"
)

// QuoteResponse is the JSON payload for GET /v1/quote. Combines the
// latest OHLCV bar with bid/offer data to give a single-request
// snapshot of a symbol's current market state — enough for the
// frontend's depth/spread panel without hitting multiple endpoints.
type QuoteResponse struct {
	Symbol     string  `json:"symbol"`
	LastPrice  float64 `json:"lastPrice"`
	OpenPrice  float64 `json:"openPrice"`
	BidPrice   float64 `json:"bidPrice"`
	OfferPrice float64 `json:"offerPrice"`
	Spread     float64 `json:"spread"`
	SpreadPct  float64 `json:"spreadPct"`
	MidPrice   float64 `json:"midPrice"`
	Volume     int64   `json:"volume"`
}

// HandleGetQuote returns the latest quote for a single symbol: last
// price, bid, offer, spread, and mid. Public endpoint (same auth tier
// as /v1/history) — the depth data is already visible to anyone who
// can see the chart.
func (s *Server) HandleGetQuote(w http.ResponseWriter, r *http.Request) {
	symbol := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("symbol")))
	if symbol == "" {
		respondError(w, http.StatusBadRequest, "Missing symbol")
		return
	}
	if !validateSymbol(symbol) {
		respondError(w, http.StatusBadRequest, "Invalid symbol")
		return
	}

	quote, err := s.quote(r.Context(), symbol)
	if err != nil {
		if errors.Is(err, errQuoteNotFound) {
			respondError(w, http.StatusNotFound, "Symbol not found")
			return
		}
		respondError(w, http.StatusInternalServerError, "Failed to fetch market data")
		return
	}
	respondJSON(w, quote)
}

var errQuoteNotFound = errors.New("quote not found")

// quote is the shared market-data operation behind the public quote endpoint
// and the authenticated MCP tool. Keeping bid/offer fallback logic here
// ensures both interfaces return identical values.
func (s *Server) quote(ctx context.Context, symbol string) (*QuoteResponse, error) {
	items, err := s.cachedMarketSummaryItems(ctx)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if item.Symbol != symbol {
			continue
		}
		bid, offer := item.BidPrice, item.OfferPrice
		if bid == 0 && item.LastPrice > 0 {
			bid = math.Round(item.LastPrice*0.99*100) / 100
		}
		if offer == 0 && item.LastPrice > 0 {
			offer = math.Round(item.LastPrice*1.01*100) / 100
		}
		spread := math.Round((offer-bid)*100) / 100
		mid := math.Round((bid+offer)/2*100) / 100
		spreadPct := 0.0
		if mid > 0 {
			spreadPct = math.Round((offer-bid)/mid*10000) / 100
		}
		return &QuoteResponse{Symbol: item.Symbol, LastPrice: item.LastPrice, OpenPrice: item.OpenPrice, BidPrice: bid, OfferPrice: offer, Spread: spread, SpreadPct: spreadPct, MidPrice: mid, Volume: item.Volume}, nil
	}
	return nil, errQuoteNotFound
}
