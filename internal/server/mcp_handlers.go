package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"

	"github.com/teckdroids/ges-data-engine/internal/analysis"
	"github.com/teckdroids/ges-data-engine/internal/audit"
	"github.com/teckdroids/ges-data-engine/internal/auth"
	"github.com/teckdroids/ges-data-engine/internal/repository"
)

const (
	mcpProtocolVersion = "2025-03-26"
	maxMCPBodyBytes    = 64 << 10
	maxMCPHistoryBars  = 2_000
	maxMCPMovers       = 50
)

type mcpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type mcpResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *mcpError       `json:"error,omitempty"`
}

type mcpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mcpTool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	InputSchema any    `json:"inputSchema"`
}

type mcpToolCall struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type mcpContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type mcpToolResult struct {
	Content []mcpContent `json:"content"`
	IsError bool         `json:"isError,omitempty"`
}

var mcpTools = []mcpTool{
	{
		Name:        "get_latest_quote",
		Description: "Return the latest available Ghana Stock Exchange quote, including bid, offer, spread, and volume.",
		InputSchema: objectSchema(map[string]any{"symbol": stringSchema("GSE ticker symbol, e.g. MTNGH")}, []string{"symbol"}),
	},
	{
		Name:        "get_price_history",
		Description: "Return bounded OHLCV history for a Ghana Stock Exchange symbol. Interval defaults to 1d.",
		InputSchema: objectSchema(map[string]any{
			"symbol":   stringSchema("GSE ticker symbol"),
			"interval": stringEnumSchema("Aggregation interval", []string{"1d", "1w", "1M"}),
			"limit":    integerSchema("Maximum bars to return (1-2000)", 1, maxMCPHistoryBars),
		}, []string{"symbol"}),
	},
	{
		Name:        "get_market_movers",
		Description: "Return the latest top gainers, top losers, or most active GSE listings. A minimum-volume filter prevents thin trades from being labelled movers.",
		InputSchema: objectSchema(map[string]any{
			"direction": stringEnumSchema("gainers, losers, or active", []string{"gainers", "losers", "active"}),
			"limit":     integerSchema("Maximum rows to return (1-50)", 1, maxMCPMovers),
		}, nil),
	},
	{
		Name:        "get_market_briefing",
		Description: "Return the most recently saved daily market briefing and its per-symbol insights.",
		InputSchema: objectSchema(map[string]any{}, nil),
	},
	{
		Name:        "get_technical_indicators",
		Description: "Return deterministic RSI, SMA, and average-volume indicators for a GSE symbol. Requires Pro or Admin access.",
		InputSchema: objectSchema(map[string]any{"symbol": stringSchema("GSE ticker symbol")}, []string{"symbol"}),
	},
}

func objectSchema(properties map[string]any, required []string) map[string]any {
	schema := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func stringSchema(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}
func stringEnumSchema(description string, values []string) map[string]any {
	return map[string]any{"type": "string", "description": description, "enum": values}
}
func integerSchema(description string, min, max int) map[string]any {
	return map[string]any{"type": "integer", "description": description, "minimum": min, "maximum": max}
}

// HandleMCP implements the server-facing subset of the Model Context Protocol.
// It is intentionally a small, typed, read-only surface: tools call existing
// domain services and never accept arbitrary QuestDB SQL.
func (s *Server) HandleMCP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	user := auth.FromContext(r.Context())
	if user.IsGuest() {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxMCPBodyBytes)
	var req mcpRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		s.writeMCPError(w, nil, -32700, "Parse error")
		return
	}
	if err := ensureSingleJSONValue(dec); err != nil {
		s.writeMCPError(w, req.ID, -32600, "Invalid Request")
		return
	}
	if req.JSONRPC != "2.0" || strings.TrimSpace(req.Method) == "" {
		s.writeMCPError(w, req.ID, -32600, "Invalid Request")
		return
	}
	if !validMCPID(req.ID) {
		s.writeMCPError(w, nil, -32600, "Invalid Request")
		return
	}

	result, rpcErr := s.dispatchMCP(r, req, user)
	// JSON-RPC notifications intentionally do not receive a response.
	if len(req.ID) == 0 || string(req.ID) == "null" {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if rpcErr != nil {
		s.writeMCPError(w, req.ID, rpcErr.Code, rpcErr.Message)
		return
	}
	s.writeMCPResponse(w, mcpResponse{JSONRPC: "2.0", ID: req.ID, Result: result})
}

func ensureSingleJSONValue(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

// JSON-RPC request IDs may only be strings, numbers, or null. Notifications
// omit the ID entirely, which is also valid.
func validMCPID(raw json.RawMessage) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return true
	}
	var id any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&id); err != nil || ensureSingleJSONValue(dec) != nil {
		return false
	}
	switch id.(type) {
	case string, json.Number:
		return true
	default:
		return false
	}
}

func (s *Server) dispatchMCP(r *http.Request, req mcpRequest, user *auth.User) (any, *mcpError) {
	switch req.Method {
	case "initialize":
		var params map[string]json.RawMessage
		if !decodeMCPParams(req.Params, &params) || !decodeMCPParams(params["protocolVersion"], new(string)) {
			return nil, &mcpError{Code: -32602, Message: "Unsupported protocol version"}
		}
		var protocolVersion string
		_ = json.Unmarshal(params["protocolVersion"], &protocolVersion)
		if protocolVersion != mcpProtocolVersion {
			return nil, &mcpError{Code: -32602, Message: "Unsupported protocol version"}
		}
		return map[string]any{
			"protocolVersion": mcpProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]string{"name": "gse-terminal", "version": "1.0"},
		}, nil
	case "notifications/initialized":
		return map[string]any{}, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		return map[string]any{"tools": mcpTools}, nil
	case "tools/call":
		var call mcpToolCall
		if !decodeMCPParams(req.Params, &call) || strings.TrimSpace(call.Name) == "" {
			return nil, &mcpError{Code: -32602, Message: "Invalid tool parameters"}
		}
		result := s.callMCPTool(r, call, user)
		return result, nil
	default:
		return nil, &mcpError{Code: -32601, Message: "Method not found"}
	}
}

func decodeMCPParams(raw json.RawMessage, into any) bool {
	if len(raw) == 0 {
		return false
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	return dec.Decode(into) == nil && ensureSingleJSONValue(dec) == nil
}

func (s *Server) callMCPTool(r *http.Request, call mcpToolCall, user *auth.User) mcpToolResult {
	var data any
	var err error
	switch call.Name {
	case "get_latest_quote":
		var args struct {
			Symbol string `json:"symbol"`
		}
		if !decodeMCPParams(call.Arguments, &args) {
			err = errors.New("symbol is required")
		} else {
			data, err = s.mcpQuote(r, args.Symbol)
		}
	case "get_price_history":
		var args struct {
			Symbol   string `json:"symbol"`
			Interval string `json:"interval"`
			Limit    int    `json:"limit"`
		}
		if !decodeMCPParams(call.Arguments, &args) {
			err = errors.New("invalid history arguments")
		} else {
			data, err = s.mcpHistory(r, args.Symbol, args.Interval, args.Limit)
		}
	case "get_market_movers":
		var args struct {
			Direction string `json:"direction"`
			Limit     int    `json:"limit"`
		}
		if !decodeMCPParams(call.Arguments, &args) {
			err = errors.New("invalid mover arguments")
		} else {
			data, err = s.mcpMovers(r, args.Direction, args.Limit)
		}
	case "get_market_briefing":
		if len(call.Arguments) > 0 && string(call.Arguments) != "{}" {
			err = errors.New("this tool takes no arguments")
		} else {
			data, err = s.mcpBriefing(r)
		}
	case "get_technical_indicators":
		if !user.IsPro() && !user.IsAdmin() {
			err = errors.New("Pro or Admin access required")
		} else {
			var args struct {
				Symbol string `json:"symbol"`
			}
			if !decodeMCPParams(call.Arguments, &args) {
				err = errors.New("symbol is required")
			} else {
				data, err = s.mcpTechnicalIndicators(r, args.Symbol)
			}
		}
	default:
		err = errors.New("unknown tool")
	}

	metadata := map[string]interface{}{"tool": call.Name, "success": err == nil}
	s.auditLog.Log(r.Context(), audit.ActionMCPToolCall, "mcp_tool", call.Name, metadata)
	if err != nil {
		return mcpErrorResult(err)
	}
	return mcpSuccessResult(data)
}

func (s *Server) mcpQuote(r *http.Request, rawSymbol string) (*QuoteResponse, error) {
	symbol := strings.ToUpper(strings.TrimSpace(rawSymbol))
	if !validateSymbol(symbol) {
		return nil, errors.New("invalid symbol")
	}
	return s.quote(r.Context(), symbol)
}

func (s *Server) mcpHistory(r *http.Request, rawSymbol, interval string, limit int) (map[string]any, error) {
	symbol := strings.ToUpper(strings.TrimSpace(rawSymbol))
	if !validateSymbol(symbol) {
		return nil, errors.New("invalid symbol")
	}
	if interval == "" {
		interval = "1d"
	}
	if !repository.ValidInterval(interval) || (interval != "1d" && interval != "1w" && interval != "1M") {
		return nil, errors.New("invalid interval")
	}
	if limit == 0 {
		limit = 365
	}
	if limit < 1 || limit > maxMCPHistoryBars {
		return nil, fmt.Errorf("limit must be between 1 and %d", maxMCPHistoryBars)
	}
	if s.qdbRepo == nil {
		return nil, errors.New("market data is temporarily unavailable")
	}
	bars, err := s.qdbRepo.GetRecentOHLC(r.Context(), symbol, interval, limit)
	if err != nil {
		return nil, errors.New("market data is temporarily unavailable")
	}
	return map[string]any{"symbol": symbol, "interval": interval, "bars": bars, "count": len(bars)}, nil
}

func (s *Server) mcpMovers(r *http.Request, direction string, limit int) (map[string]any, error) {
	if direction == "" {
		direction = "gainers"
	}
	if limit == 0 {
		limit = 10
	}
	if limit < 1 || limit > maxMCPMovers {
		return nil, fmt.Errorf("limit must be between 1 and %d", maxMCPMovers)
	}
	items, err := s.cachedMarketSummaryItems(r.Context())
	if err != nil {
		return nil, errors.New("market data is temporarily unavailable")
	}
	gainers, losers := rankMovers(items, MoversMinVolume)
	var rows []repository.MarketSummaryItem
	switch direction {
	case "gainers":
		rows = gainers
	case "losers":
		rows = losers
	case "active":
		rows = append([]repository.MarketSummaryItem(nil), items...)
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].Volume > rows[j].Volume })
	default:
		return nil, errors.New("direction must be gainers, losers, or active")
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return map[string]any{"direction": direction, "minVolume": MoversMinVolume, "items": rows, "count": len(rows)}, nil
}

func (s *Server) mcpBriefing(r *http.Request) (any, error) {
	if s.pgRepo == nil {
		return nil, errors.New("briefing is temporarily unavailable")
	}
	briefing, err := s.pgRepo.GetLatestBriefing(r.Context())
	if err != nil {
		if isBriefingEmptyResult(err) {
			return emptyBriefingPayload(), nil
		}
		return nil, errors.New("briefing is temporarily unavailable")
	}
	return briefing, nil
}

func (s *Server) mcpTechnicalIndicators(r *http.Request, rawSymbol string) (map[string]any, error) {
	symbol := strings.ToUpper(strings.TrimSpace(rawSymbol))
	if !validateSymbol(symbol) {
		return nil, errors.New("invalid symbol")
	}
	if s.qdbRepo == nil {
		return nil, errors.New("market data is temporarily unavailable")
	}
	closes, err := s.qdbRepo.GetRecentCloses(r.Context(), symbol, 200)
	if err != nil {
		return nil, errors.New("market data is temporarily unavailable")
	}
	if len(closes) == 0 {
		return nil, errors.New("no price history available")
	}
	return map[string]any{
		"symbol":     symbol,
		"sampleSize": len(closes),
		"rsi14":      math.Round(analysis.WilderRSI(closes, 14)*100) / 100,
		"sma20":      math.Round(analysis.SMA(closes, 20)*100) / 100,
		"sma50":      math.Round(analysis.SMA(closes, 50)*100) / 100,
	}, nil
}

func mcpSuccessResult(data any) mcpToolResult {
	text, err := json.Marshal(data)
	if err != nil {
		return mcpErrorResult(errors.New("failed to encode tool result"))
	}
	return mcpToolResult{Content: []mcpContent{{Type: "text", Text: string(text)}}}
}

func mcpErrorResult(err error) mcpToolResult {
	return mcpToolResult{Content: []mcpContent{{Type: "text", Text: err.Error()}}, IsError: true}
}

func (s *Server) writeMCPError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	s.writeMCPResponse(w, mcpResponse{JSONRPC: "2.0", ID: id, Error: &mcpError{Code: code, Message: message}})
}

func (s *Server) writeMCPResponse(w http.ResponseWriter, response mcpResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("MCP-Protocol-Version", mcpProtocolVersion)
	_ = json.NewEncoder(w).Encode(response)
}
