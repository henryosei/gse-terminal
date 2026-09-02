package server

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/teckdroids/ges-data-engine/internal/analysis"
	"github.com/teckdroids/ges-data-engine/internal/audit"
	"github.com/teckdroids/ges-data-engine/internal/auth"
	"github.com/teckdroids/ges-data-engine/internal/config"
	"github.com/teckdroids/ges-data-engine/internal/ingestor"
	"github.com/teckdroids/ges-data-engine/internal/repository"
	"github.com/teckdroids/ges-data-engine/ui"
	"golang.org/x/sync/singleflight"
)

// Server is the HTTP layer's god-object: it owns every collaborator
// (repositories, services, hub, audit log) so individual handler files only
// need to receive *Server. The handlers themselves live in per-domain files
// (auth_handlers.go, market_handlers.go, sector_handlers.go, etc.) — this
// file is wiring + shared scaffolding only.
type Server struct {
	pgRepo      *repository.PostgresRepo
	qdbRepo     *repository.QuestDBRepo
	redisRepo   *repository.RedisRepo
	authSvc     *auth.AuthService
	ingestor    *ingestor.Ingestor
	tmpl        *template.Template
	httpClient  *http.Client
	Hub         *Hub
	cfg         *config.Config
	insightSvc  *analysis.InsightService
	briefingSvc *analysis.BriefingService
	// alertEvaluator is optional. When set (see SetAlertEvaluator) the
	// admin upload handler runs alert evaluation on the just-ingested
	// market snapshot after the briefing step. Kept as an opaque
	// `alertRunner` interface so the server package doesn't import
	// internal/alerts (it shouldn't need to).
	alertEvaluator alertRunner
	// digestRunner is optional. When set (see SetDigestRunner) the
	// /v1/admin/push/test-digest endpoint can fire a watchlist digest
	// against the live QuestDB snapshot, either to the calling admin
	// only (default) or to every subscribed user with a watchlist
	// (?all=true). Typed as a narrow interface so the server package
	// stays independent of push internals.
	digestRunner digestRunner
	// emailSender handles outgoing mail (verification links for the
	// manual-add-email flow, future transactional emails). Nil when SMTP
	// isn't configured — the request-verify handler tolerates that and
	// just logs the outcome. Typed via emailDispatcher so the server
	// package doesn't take a hard import on internal/email.
	emailSender    emailDispatcher
	oauthProviders map[string]auth.OAuthProvider // keyed by provider name
	wsUpgrader     websocket.Upgrader
	auditLog       *audit.Logger

	// bgCtx is the lifetime context for tracked background goroutines
	// (cache writes, post-upload briefing regeneration). Derived from
	// context.Background() — NOT from any request — so a cancelled request
	// doesn't kill the write, but Shutdown's bgCancel does. bgWG tracks
	// those goroutines so Shutdown can wait for them to drain before the
	// process exits.
	bgCtx    context.Context
	bgCancel context.CancelFunc
	bgWG     sync.WaitGroup

	// portfolioHistoryGroup collapses concurrent identical portfolio
	// history reconstructions into a single in-flight call. Without
	// this, a client spamming GET /v1/me/portfolio/history?window=all
	// (e.g. dashboard reload, mobile pull-to-refresh, broken retry
	// loop) re-runs the O(dates × holdings) reconstruction on every
	// request because the cache write happens after the response is
	// already on the wire — by the time hit #2 arrives, hit #1's
	// cache write may not have landed in Redis yet. Singleflight
	// keys on (user_id, window) so two different users (or the same
	// user on different windows) still parallelise; only true
	// duplicates wait on the leader's result.
	portfolioHistoryGroup singleflight.Group
}

// goBackground runs fn in a tracked goroutine bounded by the server's
// bgCtx. The context fires on Shutdown so callers can honour it. Replaces
// ad-hoc `go func() { ... context.WithoutCancel(r.Context()) ... }()`
// patterns that had no join-on-shutdown story.
func (s *Server) goBackground(fn func(ctx context.Context)) {
	s.bgWG.Add(1)
	go func() {
		defer s.bgWG.Done()
		fn(s.bgCtx)
	}()
}

// GoBackground exposes goBackground for subsystems wired from main.go
// (e.g. *alerts.Evaluator) so their fire-and-forget goroutines are
// awaited by Shutdown instead of being orphaned.
func (s *Server) GoBackground(fn func(ctx context.Context)) {
	s.goBackground(fn)
}

// Shutdown signals every tracked background goroutine to wind down and
// waits (bounded by the caller's ctx) for them to return. Call this
// AFTER http.Server.Shutdown — first stop accepting new work, then
// drain the detached writes.
func (s *Server) Shutdown(ctx context.Context) error {
	s.bgCancel()
	done := make(chan struct{})
	go func() {
		s.bgWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func NewServer(
	cfg *config.Config,
	pgRepo *repository.PostgresRepo,
	qdbRepo *repository.QuestDBRepo,
	redisRepo *repository.RedisRepo,
	authSvc *auth.AuthService,
	ingestor *ingestor.Ingestor,
	httpClient *http.Client,
) (*Server, error) {
	hub := NewHub()
	go hub.Run()

	bgCtx, bgCancel := context.WithCancel(context.Background())

	s := &Server{
		cfg:        cfg,
		pgRepo:     pgRepo,
		qdbRepo:    qdbRepo,
		redisRepo:  redisRepo,
		authSvc:    authSvc,
		ingestor:   ingestor,
		httpClient: httpClient,
		Hub:        hub,
		auditLog:   audit.New(pgRepo),
		bgCtx:      bgCtx,
		bgCancel:   bgCancel,
		wsUpgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     buildOriginChecker(cfg.AllowedOrigins),
		},
	}

	// Wire the API key resolver into the auth service so /v1/* endpoints can
	// be called with `Authorization: Bearer ges_live_…`.
	authSvc.SetAPIKeyResolver(pgRepo)
	// Hand the auth service a server-tracked goroutine launcher so the
	// TouchAPIKey refresh goroutine is awaited during Shutdown rather than
	// leaking as a fire-and-forget outside the WaitGroup.
	authSvc.SetBackgroundRunner(s.goBackground)

	// Compose the analysis pipeline. The news source delegates back into the
	// server (so it shares the configured http.Client and User-Agent), and the
	// LLM client is plain Gemini using the same client.
	s.oauthProviders = make(map[string]auth.OAuthProvider)
	if cfg.GoogleOAuthEnabled() {
		s.oauthProviders["google"] = auth.NewGoogleProvider(cfg.GoogleClientID, cfg.GoogleClientSecret, cfg.GoogleRedirectURL)
		slog.Info("OAuth provider registered", "provider", "google", "redirect", cfg.GoogleRedirectURL)
	} else {
		slog.Warn("Google OAuth not configured — GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, or GOOGLE_REDIRECT_URL is empty")
	}

	// Build the LLM provider chain from whichever API keys are
	// configured. Gemini first (primary, cheapest), Claude second
	// (strong quality + independent quota), OpenAI last (broadest
	// quota bucket). When a provider's retry budget is exhausted
	// with rate-limit errors, FallbackLLMClient tries the next. The
	// wrapper also singleflights identical prompts across all
	// providers so a cold-cache burst collapses to one upstream
	// call. Always wrapping (even single-provider deploys) keeps
	// the dedup benefit; an empty chain falls back to a stub that
	// surfaces a clear error instead of silently returning blank
	// insights.
	llmProviders := []analysis.LLMClient{}
	if cfg.GeminiAPIKey != "" {
		llmProviders = append(llmProviders, analysis.NewGeminiClient(cfg.GeminiAPIKey, httpClient))
	}
	if cfg.AnthropicAPIKey != "" {
		llmProviders = append(llmProviders, analysis.NewAnthropicClient(cfg.AnthropicAPIKey, "", httpClient))
	}
	if cfg.OpenAIAPIKey != "" {
		llmProviders = append(llmProviders, analysis.NewOpenAIClient(cfg.OpenAIAPIKey, "", httpClient))
	}
	// Any OpenAI-shaped endpoint (Ollama, vLLM, OpenRouter, Groq, …) goes
	// last: it's the deployment-specific escape hatch, so the hosted
	// providers keep their existing precedence when both are configured.
	compatName := cfg.OpenAICompatName
	if cfg.OpenAICompatEnabled() {
		compat, err := analysis.NewOpenAICompatibleClient(analysis.OpenAICompatibleConfig{
			Name:       compatName,
			BaseURL:    cfg.OpenAICompatBaseURL,
			APIKey:     cfg.OpenAICompatAPIKey,
			Model:      cfg.OpenAICompatModel,
			HTTPClient: httpClient,
		})
		if err != nil {
			// Misconfiguration, not an outage — don't take the server down
			// over an optional provider, but say so loudly.
			slog.Error("OpenAI-compatible provider not wired", "error", err)
		} else {
			llmProviders = append(llmProviders, compat)
			if compatName == "" {
				compatName = "openai-compatible"
			}
		}
	} else if cfg.OpenAICompatBaseURL != "" || cfg.OpenAICompatModel != "" {
		slog.Warn("OpenAI-compatible provider skipped — OPENAI_COMPAT_BASE_URL and OPENAI_COMPAT_MODEL must both be set",
			"baseURL", cfg.OpenAICompatBaseURL, "model", cfg.OpenAICompatModel)
	}
	if len(llmProviders) == 0 {
		slog.Warn("No LLM providers configured — GEMINI_API_KEY / ANTHROPIC_API_KEY / OPENAI_API_KEY / OPENAI_COMPAT_BASE_URL all empty. AI Oracle will return errors.")
	} else {
		providerNames := make([]string, 0, len(llmProviders))
		if cfg.GeminiAPIKey != "" {
			providerNames = append(providerNames, "gemini")
		}
		if cfg.AnthropicAPIKey != "" {
			providerNames = append(providerNames, "anthropic")
		}
		if cfg.OpenAIAPIKey != "" {
			providerNames = append(providerNames, "openai")
		}
		if compatName != "" {
			providerNames = append(providerNames, compatName)
		}
		slog.Info("LLM provider chain wired", "chain", providerNames)
	}
	llm := analysis.NewFallbackLLMClient(llmProviders...)

	s.insightSvc = analysis.NewInsightService(
		qdbRepo,
		&googleNewsSource{httpClient: httpClient},
		llm,
	)

	// Parse the embedded templates once at boot so a corrupted dist surfaces
	// immediately rather than on first request. Development mode normally
	// re-reads dist/ from disk per request, but it still needs these as a
	// fallback for environments with no source tree — a container running
	// APP_ENV=development off the same .env a workstation uses.
	if err := s.parseEmbeddedTemplates(); err != nil {
		if cfg.AppEnv != "development" {
			return nil, err
		}
		// On a workstation the on-disk dist/ is authoritative and this set
		// is only a backstop, so don't refuse to boot over it.
		slog.Warn("embedded templates unavailable; dev mode requires ui/dist on disk", "error", err)
	}

	return s, nil
}

// parseEmbeddedTemplates loads the //go:embed'ed dist/*.html into s.tmpl.
func (s *Server) parseEmbeddedTemplates() error {
	distFS, err := fs.Sub(ui.Files, "dist")
	if err != nil {
		return fmt.Errorf("dist subfolder: %w", err)
	}
	tmpl, err := template.New("").Funcs(templateFuncs()).ParseFS(distFS, "*.html")
	if err != nil {
		return fmt.Errorf("parse templates: %w", err)
	}
	s.tmpl = tmpl
	return nil
}

// AuditLog exposes the server's audit logger so background jobs (the
// collector daemon, scheduled scrapers) can write entries through the same
// path the HTTP handlers use.
func (s *Server) AuditLog() *audit.Logger { return s.auditLog }

// secureCookie returns true when session cookies should set Secure=true
// (HTTPS only). False in development so HTTP-only localhost works.
func (s *Server) secureCookie() bool { return s.cfg.IsProduction() }

// serveStaticFile streams a single file from the dist/ root. In dev we
// hit the local filesystem so a Vite rebuild surfaces immediately; in
// prod we stream from the embed.FS snapshotted at build time. Used for
// PWA assets (sw.js, manifest.webmanifest, offline.html) that need to
// live at the URL root rather than under /static/ — service worker
// scope and the install banner both demand it.
func (s *Server) serveStaticFile(w http.ResponseWriter, r *http.Request, name string) {
	if s.cfg.AppEnv == "development" {
		http.ServeFile(w, r, "ui/dist/"+name)
		return
	}
	distFS, err := fs.Sub(ui.Files, "dist")
	if err != nil {
		http.Error(w, "static asset error", http.StatusInternalServerError)
		return
	}
	f, err := distFS.Open(name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		http.Error(w, "stat error", http.StatusInternalServerError)
		return
	}
	rs, ok := f.(io.ReadSeeker)
	if !ok {
		// embed.FS files implement Seek; fall back to ReadFile + bytes
		// reader if a future swap-in doesn't.
		http.NotFound(w, r)
		return
	}
	http.ServeContent(w, r, name, stat.ModTime(), rs)
}

// InsightSvc exposes the insight service so the post-scrape hook can
// generate daily briefings using the same pipeline as /v1/ai-insight.
func (s *Server) InsightSvc() *analysis.InsightService { return s.insightSvc }

// OAuthProvider returns the named provider or nil if not configured.
func (s *Server) OAuthProvider(name string) auth.OAuthProvider {
	return s.oauthProviders[name]
}

// OAuthProviderNames returns the list of configured provider keys.
func (s *Server) OAuthProviderNames() []string {
	names := make([]string, 0, len(s.oauthProviders))
	for k := range s.oauthProviders {
		names = append(names, k)
	}
	return names
}

// SetBriefingSvc wires the briefing service into the server so upload
// handlers can regenerate the daily briefing after data ingestion.
func (s *Server) SetBriefingSvc(b *analysis.BriefingService) { s.briefingSvc = b }

// buildOriginChecker returns a CheckOrigin function that allows only the
// configured origins. An empty list permits all origins (development only).
func buildOriginChecker(allowed []string) func(*http.Request) bool {
	if len(allowed) == 0 {
		return func(r *http.Request) bool { return true }
	}
	set := make(map[string]struct{}, len(allowed))
	for _, o := range allowed {
		set[o] = struct{}{}
	}
	return func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		_, ok := set[origin]
		return ok
	}
}

// templateFuncs returns the FuncMap shared by all html/template instances.
// Add helpers here when a template needs simple computed values that aren't
// expressible in stock Go template syntax.
func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"neg": func(v float64) float64 { return -v },
	}
}

func (s *Server) renderTemplate(w http.ResponseWriter, name string, data interface{}) {
	if os.Getenv("APP_ENV") == "development" {
		// In dev, re-parse templates on every request from the built
		// dist/ directory on disk — not the source ui/*.html files.
		// The source HTML no longer stands alone: its CSS is compiled by
		// Vite (Tailwind + the extracted terminal.css) into dist/assets/
		// and the <link> + <script> tags are injected during the build.
		// Workflow: `npm run build` → page refresh (no Go restart needed).
		tmpl, err := template.New("").Funcs(templateFuncs()).ParseFiles(
			"ui/dist/terminal.html", "ui/dist/ui-pro.html", "ui/dist/login.html", "ui/dist/admin.html",
			"ui/dist/index.html", "ui/dist/developers.html", "ui/dist/swagger.html", "ui/dist/mcp.html",
			"ui/dist/settings.html",
		)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			// No dist/ on disk. That's the container: APP_ENV comes from
			// the same .env a workstation uses, but the image ships the
			// templates embedded and has no source tree to re-read. Serve
			// the embedded set rather than 500-ing on every page.
			slog.Debug("dev template reload skipped, using embedded templates", "error", err)
		case err != nil:
			// A real parse error — surface it, that's the point of dev mode.
			http.Error(w, fmt.Sprintf("Template error: %v", err), http.StatusInternalServerError)
			return
		default:
			if err := tmpl.ExecuteTemplate(w, name, data); err != nil {
				http.Error(w, fmt.Sprintf("Execution error: %v", err), http.StatusInternalServerError)
			}
			return
		}
	}

	if s.tmpl == nil {
		http.Error(w, "Template error: no templates available — run `npm run build` in ui/ to populate ui/dist", http.StatusInternalServerError)
		return
	}
	err := s.tmpl.ExecuteTemplate(w, name, data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Execution error: %v", err), http.StatusInternalServerError)
	}
}

// fetchTickerSnapshot returns the latest-traded market summary suitable for
// the marquee ticker on the landing and terminal pages. Errors are logged
// and an empty slice is returned so the template can fall back to its empty
// state.
func (s *Server) fetchTickerSnapshot(r *http.Request) []repository.MarketSummaryItem {
	items, err := s.cachedMarketSummaryItems(r.Context())
	if err != nil {
		LoggerFromCtx(r.Context()).Warn("ticker fetch failed", "error", err)
		return nil
	}
	out := make([]repository.MarketSummaryItem, 0, len(items))
	for _, item := range items {
		if item.Symbol == "" || item.LastPrice <= 0 {
			continue
		}
		out = append(out, item)
	}
	if len(out) > 20 {
		out = out[:20]
	}
	return out
}
