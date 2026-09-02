package repository

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	qdb "github.com/questdb/go-questdb-client/v3"
)

// validIntervals is the exhaustive allowlist of SAMPLE BY values
// accepted by GetOHLC and GetOHLCBatch. Any other value is rejected
// before it reaches the query string, preventing SQL injection via
// the fmt.Sprintf'd interval parameter.
var validIntervals = map[string]bool{
	"1s": true, "5s": true, "10s": true, "15s": true, "30s": true,
	"1m": true, "5m": true, "10m": true, "15m": true, "30m": true,
	"1h": true, "2h": true, "4h": true,
	"1d": true, "1w": true, "1M": true,
}

func validateInterval(interval string) error {
	if !validIntervals[interval] {
		return fmt.Errorf("invalid interval %q", interval)
	}
	return nil
}

// ValidInterval exposes the allowlist to the HTTP layer so a handler can
// reject a bad interval before it is interpolated into a cache key. The
// query layer rejects it either way, but only after the caller-controlled
// string has already been used to build a Redis key.
func ValidInterval(interval string) bool { return validIntervals[interval] }

type QuestDBRepo struct {
	ilpSender qdb.LineSender
	queryPool *pgxpool.Pool
}

// queryTimeout caps the wall-clock cost of any single QuestDB query so a
// runaway scan can't pin a connection from the pool indefinitely. The
// production-cluster equivalent would be a per-statement statement_timeout,
// but QuestDB's pgwire shim doesn't honour that yet — so we enforce in Go.
const queryTimeout = 15 * time.Second

// withQueryTimeout returns a derived context bounded by queryTimeout. The
// caller MUST defer the returned cancel func.
func withQueryTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, queryTimeout)
}

// windowAnchor returns the point in time that "last N months/days" windows
// are measured back from: the newest trading_date in the table, falling back
// to the wall clock when the table is empty.
//
// Anchoring to the data instead of now() keeps the app working when the feed
// is behind — a backfill CSV that ends months ago, or a stretch where the
// daily scrape didn't run. With now() as the anchor, every windowed query
// (symbol list, market overview, sector rotation) silently returns zero rows
// and the UI goes blank even though the data is sitting right there.
func (r *QuestDBRepo) windowAnchor(ctx context.Context) time.Time {
	latest, err := r.GetLastIngestionTime(ctx)
	if err != nil || latest.IsZero() {
		return time.Now().UTC()
	}
	return latest
}

// Ping verifies QuestDB pgwire connectivity. Used by the boot health check.
func (r *QuestDBRepo) Ping(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return r.queryPool.Ping(cctx)
}

func NewQuestDBRepo(ctx context.Context, ilpHost string, pgWireConnString string) (*QuestDBRepo, error) {
	sender, err := qdb.NewLineSender(ctx, qdb.WithTcp(), qdb.WithAddress(ilpHost))
	if err != nil {
		return nil, fmt.Errorf("failed to create ILP sender: %v", err)
	}

	// Pool tuning — pgxpool's defaults are MaxConns=4, which starves the
	// handler path under even modest concurrency (five parallel /v1/history
	// requests and the sixth blocks). The Postgres repo tunes its pool
	// explicitly; QuestDB was relying on defaults and shouldn't be.
	poolCfg, err := pgxpool.ParseConfig(pgWireConnString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse questdb pgwire conn string: %v", err)
	}
	poolCfg.MaxConns = 20
	poolCfg.MinConns = 2
	poolCfg.MaxConnLifetime = 15 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute
	poolCfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to questdb pgwire: %v", err)
	}

	// Create table if not exists
	query := `
	CREATE TABLE IF NOT EXISTS equities (
		timestamp TIMESTAMP,
		trading_date TIMESTAMP,
		symbol SYMBOL INDEX,
		year_high DOUBLE,
		year_low DOUBLE,
		prev_close_vwap DOUBLE,
		open_price DOUBLE,
		last_price DOUBLE,
		close_price_vwap DOUBLE,
		price_change DOUBLE,
		bid_price DOUBLE,
		offer_price DOUBLE,
		total_volume LONG,
		total_value DOUBLE
	) timestamp(trading_date) PARTITION BY YEAR WAL DEDUPLICATE UPSERT KEYS(trading_date, symbol);`

	_, err = pool.Exec(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to create equities table: %v", err)
	}

	// Attempt to gracefully enable deduplication on existing tables in case this is an upgrade from an older version.
	_, _ = pool.Exec(ctx, `ALTER TABLE equities DEDUPLICATE UPSERT KEYS(trading_date, symbol);`)

	return &QuestDBRepo{
		ilpSender: sender,
		queryPool: pool,
	}, nil
}

type Tick struct {
	Timestamp      time.Time
	TradingDate    time.Time
	Symbol         string
	YearHigh       float64
	YearLow        float64
	PrevCloseVWAP  float64
	OpenPrice      float64
	LastPrice      float64
	ClosePriceVWAP float64
	PriceChange    float64
	BidPrice       float64
	OfferPrice     float64
	TotalVolume    int64
	TotalValue     float64
}

func (r *QuestDBRepo) InsertTick(ctx context.Context, t Tick) error {
	// trading_date is the designated timestamp (set via .At), so do not also
	// write it as a regular TimestampColumn. Ingestion time is recorded
	// separately in the `timestamp` column for audit/freshness purposes.
	err := r.ilpSender.Table("equities").
		Symbol("symbol", t.Symbol).
		TimestampColumn("timestamp", t.Timestamp).
		Float64Column("year_high", t.YearHigh).
		Float64Column("year_low", t.YearLow).
		Float64Column("prev_close_vwap", t.PrevCloseVWAP).
		Float64Column("open_price", t.OpenPrice).
		Float64Column("last_price", t.LastPrice).
		Float64Column("close_price_vwap", t.ClosePriceVWAP).
		Float64Column("price_change", t.PriceChange).
		Float64Column("bid_price", t.BidPrice).
		Float64Column("offer_price", t.OfferPrice).
		Int64Column("total_volume", t.TotalVolume).
		Float64Column("total_value", t.TotalValue).
		At(ctx, t.TradingDate)
	return err
}

func (r *QuestDBRepo) Flush(ctx context.Context) error {
	return r.ilpSender.Flush(ctx)
}

// SymbolsWithCompleteTickOn returns the set of symbols that already
// have a fully-populated row for the given trading date — every column
// the CSV ingestor normally writes is non-zero. Used by the live
// scraper (collector.scrapeGSE) to skip symbols that were already
// filled in from an end-of-day upload: the scraper only provides
// last_price / price_change / total_volume, so re-writing those rows
// would leave year_high/year_low/prev_close_vwap/open_price/
// close_price_vwap/bid_price/offer_price/total_value at zero from the
// partial second insert.
//
// Returns a map for O(1) membership checks. Empty map + nil error on
// success with no matches.
func (r *QuestDBRepo) SymbolsWithCompleteTickOn(ctx context.Context, tradingDate time.Time) (map[string]bool, error) {
	query := `
		SELECT DISTINCT symbol FROM equities
		WHERE trading_date = $1
		  AND year_high > 0
		  AND year_low > 0
		  AND prev_close_vwap > 0
		  AND open_price > 0
		  AND close_price_vwap > 0
		  AND bid_price > 0
		  AND offer_price > 0
		  AND total_value > 0
	`
	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	rows, err := r.queryPool.Query(ctx, query, tradingDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]bool)
	for rows.Next() {
		var sym string
		if err := rows.Scan(&sym); err != nil {
			return nil, err
		}
		out[sym] = true
	}
	return out, rows.Err()
}

func (r *QuestDBRepo) Close(ctx context.Context) {
	r.ilpSender.Close(ctx)
	r.queryPool.Close()
}

type OHLC struct {
	TradingDate time.Time `json:"timestamp"`
	Open        float64   `json:"open"`
	High        float64   `json:"high"`
	Low         float64   `json:"low"`
	Close       float64   `json:"close"`
	Volume      int64     `json:"volume"`
}

func (r *QuestDBRepo) GetOHLC(ctx context.Context, symbol, interval string) ([]OHLC, error) {
	if err := validateInterval(interval); err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`
		SELECT
			trading_date,
			first(open_price) as open,
			max(close_price_vwap) as high,
			min(close_price_vwap) as low,
			last(close_price_vwap) as close,
			sum(total_volume) as volume
		FROM equities
		-- A zero close is "no price published", not a price of zero: days a
		-- symbol did not trade, and partial rows the live scraper writes
		-- (it supplies only last_price/price_change/total_volume, leaving
		-- close_price_vwap at zero). Included, they drag min() to zero and
		-- draw the series down to the axis. Excluding them gaps the bar
		-- instead, matching GetRecentCloses and GetDailyClosesAllSymbols.
		WHERE symbol = $1 AND close_price_vwap > 0
		SAMPLE BY %s ALIGN TO CALENDAR
		ORDER BY trading_date ASC;
	`, interval)

	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	rows, err := r.queryPool.Query(ctx, query, symbol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []OHLC
	for rows.Next() {
		var o OHLC
		err := rows.Scan(&o.TradingDate, &o.Open, &o.High, &o.Low, &o.Close, &o.Volume)
		if err != nil {
			return nil, err
		}
		result = append(result, o)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// GetRecentOHLC returns at most limit of the most recent aggregated bars in
// chronological order. Unlike GetOHLC, it bounds the query at the database so
// automation clients cannot turn a small response request into a full-history
// scan.
func (r *QuestDBRepo) GetRecentOHLC(ctx context.Context, symbol, interval string, limit int) ([]OHLC, error) {
	if err := validateInterval(interval); err != nil {
		return nil, err
	}
	if limit < 1 {
		return nil, fmt.Errorf("invalid limit %d", limit)
	}
	query := fmt.Sprintf(`
		SELECT trading_date,
		       first(open_price) as open,
		       max(close_price_vwap) as high,
		       min(close_price_vwap) as low,
		       last(close_price_vwap) as close,
		       sum(total_volume) as volume
		FROM equities
		WHERE symbol = $1 AND close_price_vwap > 0
		SAMPLE BY %s ALIGN TO CALENDAR
		ORDER BY trading_date DESC
		LIMIT $2;
	`, interval)
	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	rows, err := r.queryPool.Query(ctx, query, symbol, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]OHLC, 0, limit)
	for rows.Next() {
		var bar OHLC
		if err := rows.Scan(&bar.TradingDate, &bar.Open, &bar.High, &bar.Low, &bar.Close, &bar.Volume); err != nil {
			return nil, err
		}
		out = append(out, bar)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// GetOHLCBatch fetches OHLC series for several symbols in a single round
// trip. Replaces N sequential GetOHLC calls in HandleGetCompare. Symbols
// are filtered server-side via WHERE symbol IN ($1,$2,…) so QuestDB only
// walks the requested partitions; results are grouped by symbol in Go.
//
// We build the IN clause with positional scalar placeholders rather than
// a single array parameter (`ANY($1)`) because QuestDB's pgwire shim
// doesn't support array types — it reports the parameter as `unknown`
// and rejects `ANY(unknown)`. Positional scalars are the documented
// supported path and work identically to single-symbol GetOHLC.
func (r *QuestDBRepo) GetOHLCBatch(ctx context.Context, symbols []string, interval string, since time.Time) (map[string][]OHLC, error) {
	out := make(map[string][]OHLC, len(symbols))
	if len(symbols) == 0 {
		return out, nil
	}
	if err := validateInterval(interval); err != nil {
		return nil, err
	}

	placeholders := make([]string, len(symbols))
	args := make([]interface{}, len(symbols))
	for i, sym := range symbols {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = sym
	}
	// `since` is an optional lower bound on trading_date — when the
	// caller knows the series doesn't need to go back further (e.g.,
	// portfolio reconstruction's earliest purchase date), QuestDB
	// skips older partitions entirely instead of pulling decades of
	// bars only to throw them away. Zero time = no floor, matches the
	// original (pre-since) behaviour.
	dateFilter := ""
	if !since.IsZero() {
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(symbols)+1))
		args = append(args, since)
		dateFilter = fmt.Sprintf(" AND trading_date >= %s", placeholders[len(placeholders)-1])
	}
	query := fmt.Sprintf(`
		SELECT
			trading_date,
			symbol,
			first(open_price) as open,
			max(close_price_vwap) as high,
			min(close_price_vwap) as low,
			last(close_price_vwap) as close,
			sum(total_volume) as volume
		FROM equities
		-- See GetOHLC: a zero close means no published price, and would
		-- otherwise sink both min() and the rendered line.
		WHERE symbol IN (%s) AND close_price_vwap > 0%s
		SAMPLE BY %s ALIGN TO CALENDAR
		ORDER BY symbol, trading_date ASC;
	`, strings.Join(placeholders[:len(symbols)], ","), dateFilter, interval)

	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	rows, err := r.queryPool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var sym string
		var o OHLC
		if err := rows.Scan(&o.TradingDate, &sym, &o.Open, &o.High, &o.Low, &o.Close, &o.Volume); err != nil {
			return nil, err
		}
		out[sym] = append(out[sym], o)
	}
	return out, rows.Err()
}

func (r *QuestDBRepo) GetSymbols(ctx context.Context) ([]string, error) {
	cutoff := r.windowAnchor(ctx).AddDate(0, -3, 0)
	query := `SELECT DISTINCT symbol FROM equities WHERE trading_date > $1 ORDER BY symbol ASC;`
	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	rows, err := r.queryPool.Query(ctx, query, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var symbols []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		symbols = append(symbols, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return symbols, nil
}

type MarketSummaryItem struct {
	Symbol        string  `json:"symbol"`
	OpenPrice     float64 `json:"openPrice"`
	LastPrice     float64 `json:"lastPrice"`
	PriceChange   float64 `json:"priceChange"`
	PercentChange float64 `json:"percentChange"`
	Volume        int64   `json:"volume"`
	BidPrice      float64 `json:"bidPrice"`
	OfferPrice    float64 `json:"offerPrice"`
	Spread        float64 `json:"spread"`    // offerPrice - bidPrice
	SpreadPct     float64 `json:"spreadPct"` // spread / midPrice * 100
}

type MarketOverview struct {
	TopGainers  []MarketSummaryItem `json:"topGainers"`
	TopLosers   []MarketSummaryItem `json:"topLosers"`
	Active      []MarketSummaryItem `json:"active"`
	All         []MarketSummaryItem `json:"all"`
	LastUpdated *time.Time          `json:"lastUpdated,omitempty"`
	// MoversMinVolume is the volume floor applied to TopGainers/TopLosers
	// rankings — symbols traded below this threshold are excluded so a
	// tiny lot can't masquerade as a top mover. The UI surfaces it as a
	// "Min volume" footnote.
	MoversMinVolume int64 `json:"moversMinVolume,omitempty"`
}

// SymbolDailyClose is a single (date, symbol, close) row used by the sector
// rotation chart to build per-sector indexed series in Go without paying for
// per-sector subqueries.
type SymbolDailyClose struct {
	Date   time.Time
	Symbol string
	Close  float64
}

// GetDailyClosesAllSymbols returns daily closing prices for every symbol over
// the last `days` calendar days, ordered by date ascending. Used by the
// sector rotation chart aggregation.
func (r *QuestDBRepo) GetDailyClosesAllSymbols(ctx context.Context, days int) ([]SymbolDailyClose, error) {
	if days <= 0 {
		days = 30
	}
	query := `
		SELECT trading_date, symbol, close_price_vwap
		FROM equities
		WHERE trading_date > $1
		  AND close_price_vwap > 0
		ORDER BY trading_date ASC;
	`
	cutoff := r.windowAnchor(ctx).AddDate(0, 0, -days)
	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	rows, err := r.queryPool.Query(ctx, query, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SymbolDailyClose
	for rows.Next() {
		var p SymbolDailyClose
		if err := rows.Scan(&p.Date, &p.Symbol, &p.Close); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// GetRecentCloses returns the last `limit` daily VWAP closes for `symbol`
// in chronological order. Drives the alerts evaluator's RSI calculation —
// needs ~period+buffer bars (21 is a safe default for a 14-period RSI).
// Non-positive closes are filtered at the DB so the caller never has to
// divide by zero.
func (r *QuestDBRepo) GetRecentCloses(ctx context.Context, symbol string, limit int) ([]float64, error) {
	if limit <= 0 {
		limit = 30
	}
	// Pull the most recent N bars DESC, then reverse in Go — QuestDB's
	// SAMPLE BY optimiser prefers DESC + LIMIT over an unbounded ASC scan.
	query := `
		SELECT close_price_vwap
		FROM equities
		WHERE symbol = $1 AND close_price_vwap > 0
		ORDER BY trading_date DESC
		LIMIT $2;
	`
	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	rows, err := r.queryPool.Query(ctx, query, symbol, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]float64, 0, limit)
	for rows.Next() {
		var c float64
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Reverse to chronological order so WilderRSI's i-1 deltas compute
	// against the correct previous bar.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// GetLastIngestionTime returns the timestamp of the most recent trading_date in the equities table.
func (r *QuestDBRepo) GetLastIngestionTime(ctx context.Context) (time.Time, error) {
	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	var ts time.Time
	err := r.queryPool.QueryRow(ctx, `SELECT max(trading_date) FROM equities;`).Scan(&ts)
	return ts, err
}

func (r *QuestDBRepo) GetMarketSummary(ctx context.Context) ([]MarketSummaryItem, error) {
	query := `
		SELECT symbol, open_price, close_price_vwap, total_volume,
		       bid_price, offer_price
		FROM equities
		WHERE trading_date > $1
		LATEST ON trading_date PARTITION BY symbol;
	`
	cutoff := r.windowAnchor(ctx).AddDate(0, -3, 0)
	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	rows, err := r.queryPool.Query(ctx, query, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []MarketSummaryItem
	for rows.Next() {
		var item MarketSummaryItem
		err := rows.Scan(&item.Symbol, &item.OpenPrice, &item.LastPrice, &item.Volume,
			&item.BidPrice, &item.OfferPrice)
		if err != nil {
			return nil, err
		}

		item.PriceChange = item.LastPrice - item.OpenPrice
		if item.OpenPrice != 0 {
			item.PercentChange = (item.PriceChange / item.OpenPrice) * 100.0
		}
		// CSV-uploaded rows don't carry bid/offer (columns stay 0).
		// Fall back to a ±1% heuristic around the last price so the
		// depth panel always has something to show. Matches the same
		// heuristic the live scraper uses (collector/ticker.go:162-163).
		if item.BidPrice == 0 && item.LastPrice > 0 {
			item.BidPrice = math.Round(item.LastPrice*0.99*100) / 100
		}
		if item.OfferPrice == 0 && item.LastPrice > 0 {
			item.OfferPrice = math.Round(item.LastPrice*1.01*100) / 100
		}
		// Derive spread + spread% from bid/offer.
		item.Spread = math.Round((item.OfferPrice-item.BidPrice)*100) / 100
		mid := (item.BidPrice + item.OfferPrice) / 2
		if mid > 0 {
			item.SpreadPct = math.Round((item.OfferPrice-item.BidPrice)/mid*10000) / 100
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}
func (r *QuestDBRepo) GetTickerData(ctx context.Context, symbol string) ([]Tick, error) {
	query := `
		SELECT 
			timestamp, trading_date, symbol, year_high, year_low, prev_close_vwap, 
			open_price, last_price, close_price_vwap, price_change, 
			bid_price, offer_price, total_volume, total_value
		FROM equities
		WHERE symbol = $1
		ORDER BY trading_date DESC;
	`

	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	rows, err := r.queryPool.Query(ctx, query, symbol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Tick
	for rows.Next() {
		var t Tick
		err := rows.Scan(
			&t.Timestamp, &t.TradingDate, &t.Symbol, &t.YearHigh, &t.YearLow, &t.PrevCloseVWAP,
			&t.OpenPrice, &t.LastPrice, &t.ClosePriceVWAP, &t.PriceChange,
			&t.BidPrice, &t.OfferPrice, &t.TotalVolume, &t.TotalValue,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// RawQueryResult holds the column names and row data from a dynamic SQL query.
type RawQueryResult struct {
	Columns []string        `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
}

// RawQuery executes an arbitrary read-only SQL query against QuestDB and
// returns the results as generic column/row data. Used by the conversational
// query feature — the SQL is pre-validated before reaching this method.
func (r *QuestDBRepo) RawQuery(ctx context.Context, sql string) (*RawQueryResult, error) {
	rows, err := r.queryPool.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	descs := rows.FieldDescriptions()
	cols := make([]string, len(descs))
	for i, d := range descs {
		cols[i] = string(d.Name)
	}

	var data [][]interface{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		data = append(data, vals)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &RawQueryResult{Columns: cols, Rows: data}, nil
}
