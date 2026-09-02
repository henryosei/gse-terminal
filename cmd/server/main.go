package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/httprate"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/teckdroids/ges-data-engine/internal/alerts"
	"github.com/teckdroids/ges-data-engine/internal/analysis"
	"github.com/teckdroids/ges-data-engine/internal/auth"
	"github.com/teckdroids/ges-data-engine/internal/collector"
	"github.com/teckdroids/ges-data-engine/internal/config"
	"github.com/teckdroids/ges-data-engine/internal/email"
	"github.com/teckdroids/ges-data-engine/internal/ingestor"
	"github.com/teckdroids/ges-data-engine/internal/logger"
	"github.com/teckdroids/ges-data-engine/internal/metrics"
	"github.com/teckdroids/ges-data-engine/internal/openapi"
	"github.com/teckdroids/ges-data-engine/internal/push"
	"github.com/teckdroids/ges-data-engine/internal/repository"
	"github.com/teckdroids/ges-data-engine/internal/server"
	"github.com/teckdroids/ges-data-engine/ui"
)

func main() {
	_ = godotenv.Load()
	logger.Setup()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("Configuration error", "error", err)
		os.Exit(1)
	}

	httpClient := &http.Client{Timeout: cfg.HTTPTimeout}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Init Repositories. Never log the connection string — it embeds the
	// password. Errors from pgxpool include only the host/db, never creds.
	pgRepo, err := repository.NewPostgresRepo(ctx, cfg.PostgresConnString())
	if err != nil {
		slog.Error("Failed to connect to Postgres",
			"host", cfg.PostgresHost, "db", cfg.PostgresDB, "error", err)
		os.Exit(1)
	}
	defer pgRepo.Close()

	// Generate default Admin user with random password if none exists
	hasUsers, err := pgRepo.HasUsers(ctx)
	if err == nil && !hasUsers {
		password := generateSecurePassword(16)
		err = pgRepo.CreateUser(ctx, "admin", password, "admin")
		if err == nil {
			fmt.Printf("====================================================\n")
			fmt.Printf("  Generated Admin User:     admin\n")
			fmt.Printf("  Generated Admin Password: %s\n", password)
			fmt.Printf("  ⚠ Save this password now — it will not be shown again.\n")
			fmt.Printf("====================================================\n")
		}
	}

	// 1.1 Init QuestDB
	qdbRepo, err := repository.NewQuestDBRepo(ctx, cfg.QuestDBILPURL, cfg.QuestDBURL)
	if err != nil {
		slog.Warn("Failed to connect to QuestDB", "error", err)
	} else {
		defer qdbRepo.Close(ctx)
	}

	redisRepo := repository.NewRedisRepo(cfg.RedisURL)
	defer redisRepo.Close()

	// Boot health check: every required dependency must answer before we
	// open the listener. Fail fast with a complete error message rather than
	// throwing 500s on the first request.
	{
		hctx, hcancel := context.WithTimeout(ctx, 10*time.Second)
		var hErrs []string
		if err := pgRepo.Ping(hctx); err != nil {
			hErrs = append(hErrs, fmt.Sprintf("postgres: %v", err))
		}
		if err := redisRepo.Ping(hctx); err != nil {
			hErrs = append(hErrs, fmt.Sprintf("redis: %v", err))
		}
		if qdbRepo != nil {
			if err := qdbRepo.Ping(hctx); err != nil {
				hErrs = append(hErrs, fmt.Sprintf("questdb: %v", err))
			}
		}
		hcancel()
		if len(hErrs) > 0 {
			slog.Error("Dependency health check failed", "errors", hErrs)
			os.Exit(1)
		}
		slog.Info("All dependencies healthy")
	}

	// Hydrate sector overrides from the DB into the in-memory layer so the
	// first request after boot reflects admin-edited mappings.
	if overrides, err := pgRepo.ListSectorOverrides(ctx); err == nil {
		repository.SetSectorOverrides(overrides)
	} else {
		slog.Warn("Failed to load sector overrides on boot", "error", err)
	}

	// 2. Init Services
	if cfg.IsProduction() && len(cfg.JWTSecret) < 32 {
		slog.Error("JWT_SECRET must be at least 32 characters in production")
		os.Exit(1)
	}
	authSvc := auth.NewAuthService(cfg.JWTSecret, cfg.IsProduction())
	// Wire JWT revocation through Redis so /logout invalidates the cookie
	// before its natural exp.
	authSvc.SetTokenRevoker(redisRepo)
	// Wire refresh-token persistence so Middleware can silently swap
	// expired 24h access tokens for fresh ones using the 90d refresh
	// cookie. Username + role are stashed alongside the user id in
	// Redis at login time so silent refresh is DB-free — no hot-path
	// Postgres lookup just because a token expired. Users visiting at
	// least once a season stay logged in indefinitely; truly idle
	// accounts expire at 90d.
	authSvc.SetRefreshTokenStore(redisRepo)
	csvIngestor := ingestor.NewIngestor(qdbRepo)

	// 3. Init HTTP Server Handlers
	srv, err := server.NewServer(cfg, pgRepo, qdbRepo, redisRepo, authSvc, csvIngestor, httpClient)
	if err != nil {
		slog.Error("Failed to construct server", "error", err)
		os.Exit(1)
	}

	// 4. Start Automated Tick Daemon (with WebSocket Hub reference)
	if qdbRepo != nil {
		briefingSvc := analysis.NewBriefingService(srv.InsightSvc(), srv.InsightSvc().LLM())
		srv.SetBriefingSvc(briefingSvc)

		// Watchlist-alerts evaluator. When cfg.EmailEnabled() is false the
		// email.Sender is a noop and alert rules still fire the in-app
		// push + audit trail — only the outgoing mail step is skipped.
		emailSender := email.NewSender(email.Config{
			Host:     cfg.SMTPHost,
			Port:     cfg.SMTPPort,
			Username: cfg.SMTPUser,
			Password: cfg.SMTPPassword,
			From:     cfg.SMTPFrom,
		}, slog.Default())
		alertEvaluator := alerts.NewEvaluator(
			server.NewAlertRuleStore(pgRepo),
			qdbRepo,
			emailSender,
			srv.Hub, // *server.Hub satisfies alerts.Pusher via PushToUser
			srv.AuditLog(),
			slog.Default().With("component", "alerts"),
			cfg.ResolvedAppBaseURL(),
		)
		// Hand the evaluator a server-tracked goroutine launcher so the
		// outgoing email sends are awaited during Shutdown rather than
		// orphaned mid-SMTP handshake when the process exits.
		alertEvaluator.SetBackgroundRunner(srv.GoBackground)

		// Wire Web Push if VAPID keys are configured.
		var pushSvc *push.Service
		if cfg.WebPushEnabled() {
			pushSvc = push.New(
				cfg.VAPIDPublicKey,
				cfg.VAPIDPrivateKey,
				"mailto:"+cfg.SMTPFrom,
				&push.RepoAdapter{Repo: pgRepo},
				slog.Default().With("component", "webpush"),
			)
			// Server-side per-(user, day) dedupe so post-upload and
			// post-scrape digest triggers can't double-fire on the
			// same trading day. RedisRepo satisfies push.DigestDedupe
			// via SETNX on a "digest:sent:<day>:<uid>" key.
			pushSvc.SetDigestDedupe(redisRepo)
			// Narrow the set of hosts we will POST notifications to. The
			// endpoint comes from the browser, so without this any
			// authenticated user could aim it at an internal service.
			if len(cfg.WebPushAllowedHosts) > 0 {
				pushSvc.SetEndpointValidator(push.NewEndpointValidator(cfg.WebPushAllowedHosts...))
			}
			slog.Info("Web Push endpoint allowlist", "hosts", pushSvc.Validator().AllowedHosts())
			alertEvaluator.SetWebPusher(pushSvc)
			// The same service powers the admin test endpoint at
			// /v1/admin/push/test-digest. Wired through the server's
			// narrow digestRunner interface so server doesn't pin
			// push internals.
			srv.SetDigestRunner(pushSvc)
			slog.Info("Web Push enabled")
		} else {
			slog.Warn("Web Push disabled — VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY not set")
		}
		// Expose the evaluator to the HTTP layer so admin CSV uploads
		// can trigger an immediate re-evaluation — not just the daily
		// scrape path.
		srv.SetAlertEvaluator(alertEvaluator)
		// Share the same emailSender with the HTTP layer so the manual
		// email-verification flow (/v1/me/email/request-verify) can send
		// links without each subsystem instantiating its own SMTP client.
		srv.SetEmailSender(emailSender)

		postScrape := &postScrapeAdapter{
			pgRepo:         pgRepo,
			qdbRepo:        qdbRepo,
			redisRepo:      redisRepo,
			briefingSvc:    briefingSvc,
			alertEvaluator: alertEvaluator,
			pushSvc:        pushSvc,
		}
		// GSE_PYTHON_BIN / GSE_DOWNLOAD_SCRIPT let a deployment point at a
		// virtualenv interpreter or an absolute script path; empty values
		// fall back to python3 + scripts/gse_download.py relative to the
		// working directory.
		collector.StartDaemon(ctx, qdbRepo, redisRepo, srv.AuditLog(), collector.ScraperConfig{
			PythonBin:  os.Getenv("GSE_PYTHON_BIN"),
			ScriptPath: os.Getenv("GSE_DOWNLOAD_SCRIPT"),
		}, srv.Hub.Broadcast, postScrape)
	}

	// 5. Setup Router
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(server.RequestIDHeader)
	// Behind a reverse proxy every request arrives with the proxy's address,
	// so all rate limiters collapse into one shared bucket — five failed
	// logins from anywhere would lock out the whole site. RealIP rewrites
	// RemoteAddr from the forwarded headers, which fixes that and makes the
	// access log show real clients.
	//
	// Gated on TRUST_PROXY because those headers are client-supplied: with
	// the app exposed directly, trusting them lets anyone forge an address
	// and sidestep every limit. Only enable when a proxy you control sets
	// them (the Caddy service in docker-compose.prod.yaml does).
	if cfg.TrustProxy {
		r.Use(middleware.RealIP)
		slog.Info("Trusting proxy headers for client IP (TRUST_PROXY=true)")
	}
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	// Baseline security headers. CSP is permissive enough for the existing CDN
	// stack (Tailwind, ApexCharts, htmx, html2pdf, fonts.googleapis) but blocks
	// arbitrary script injection.
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("X-Frame-Options", "DENY")
			w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
			// CSP: script-src no longer needs 'unsafe-inline' — every
			// onclick/onsubmit attribute has been migrated to delegated
			// data-action / data-submit-action handlers (see app.js), and
			// no inline <script> blocks remain in ui/*.html. 'unsafe-inline'
			// is kept on style-src because Tailwind-generated markup relies
			// on inline `style=` attributes (sentiment bars, animation
			// staggers, etc.) that can't be migrated without a larger
			// refactor.
			w.Header().Set("Content-Security-Policy",
				"default-src 'self'; "+
					"script-src 'self' https://cdn.jsdelivr.net https://unpkg.com https://cdnjs.cloudflare.com blob:; "+
					"style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "+
					"font-src 'self' https://fonts.gstatic.com https://fonts.scalar.com data:; "+
					"img-src 'self' data: blob: https:; "+
					"connect-src 'self' wss: https:; "+
					"frame-ancestors 'none';")
			next.ServeHTTP(w, req)
		})
	})
	// http.TimeoutHandler (used by middleware.Timeout) wraps the response
	// writer and, after the timeout, calls WriteHeader(503) on the
	// underlying connection — which is illegal once the WebSocket upgrade
	// has hijacked it. Skip the timeout (and the IP rate limit, since
	// long-lived sockets shouldn't be capped per minute) on /ws.
	skipForPath := func(path string, mw func(http.Handler) http.Handler) func(http.Handler) http.Handler {
		return func(next http.Handler) http.Handler {
			wrapped := mw(next)
			return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == path {
					next.ServeHTTP(w, req)
					return
				}
				wrapped.ServeHTTP(w, req)
			})
		}
	}
	// Static / PWA assets and the WS endpoint bypass the IP limiter. A
	// browser refetching the service worker, manifest, and a screen's
	// worth of static chunks on every page load can easily eat the
	// budget on its own — when that happens, sw.js gets a 429 and the
	// browser refuses to install the service worker until it fetches
	// cleanly. Authenticated POSTs and the LLM/destructive routes still
	// have their own (tighter) limits applied at the route level.
	skipRateLimit := func(req *http.Request) bool {
		p := req.URL.Path
		if p == "/ws" || p == "/sw.js" || p == "/manifest.webmanifest" ||
			p == "/offline.html" || p == "/favicon.png" ||
			p == "/icon-192.png" || p == "/icon-512.png" || p == "/icon-maskable.png" ||
			p == "/screenshot-wide.png" || p == "/screenshot-mobile.png" ||
			p == "/healthz" || p == "/readyz" || p == "/metrics" {
			return true
		}
		if strings.HasPrefix(p, "/static/") {
			return true
		}
		return false
	}
	skipForCondition := func(skip func(*http.Request) bool, mw func(http.Handler) http.Handler) func(http.Handler) http.Handler {
		return func(next http.Handler) http.Handler {
			wrapped := mw(next)
			return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if skip(req) {
					next.ServeHTTP(w, req)
					return
				}
				wrapped.ServeHTTP(w, req)
			})
		}
	}
	// Bumped from 100 to 300 req/min: a real session burns through the
	// previous cap quickly with HTMX polling, chart refreshes, and the
	// auth ticker. 300 still rejects an actual flood without nuisance
	// 429s on routine browsing.
	r.Use(skipForCondition(skipRateLimit, httprate.LimitByIP(300, 1*time.Minute)))
	r.Use(skipForPath("/ws", middleware.Timeout(30*time.Second)))
	// Prometheus instrumentation. Placed last in the middleware chain so
	// the deferred observation sees the final status/latency — including
	// anything the rate limiter or timeout handler writes. /metrics
	// itself is skipped inside the middleware to keep scrapes from
	// observing themselves.
	r.Use(metrics.InstrumentHTTP)

	// Per-user rate limiter used on LLM-backed endpoints. Keys on the
	// authenticated user id when present (set by authSvc.Middleware),
	// falling back to real-IP so an anonymous request path can't slip
	// past the cap by simply not authenticating. 10 req/min per user is
	// tight enough to cap daily LLM spend while still allowing a few
	// misclicks.
	keyByUserOrIP := func(req *http.Request) (string, error) {
		if uid, ok := req.Context().Value(auth.UserIDKey).(int); ok && uid > 0 {
			return "user:" + strconv.Itoa(uid), nil
		}
		return httprate.KeyByRealIP(req)
	}
	llmLimiter := httprate.Limit(
		10, 1*time.Minute,
		httprate.WithKeyFuncs(keyByUserOrIP),
	)
	// MCP tools are read-only but can still trigger bounded database work.
	// Keep their budget separate from browser and LLM traffic so an automated
	// client cannot consume either pool.
	mcpLimiter := httprate.Limit(
		30, 1*time.Minute,
		httprate.WithKeyFuncs(keyByUserOrIP),
	)

	// Endpoints that verify a password. These sit behind RequireAuth, so
	// they were only covered by the global 300/min budget — which made
	// POST /v1/me/merge-account a password-guessing oracle: it bcrypt-
	// checks an arbitrary username + password and returns distinguishable
	// statuses, at 300 attempts a minute, entirely outside the 5/min
	// limiter guarding /login.
	//
	// Two limiters rather than one. Keyed per-user alone, an attacker just
	// registers more accounts to get more buckets; keyed per-IP alone,
	// one office NAT shares a bucket. Both must pass, so neither dodge
	// works. The per-IP ceiling is the looser of the two so genuine
	// shared egress isn't punished for one user's typos.
	credentialLimiter := newCredentialLimiter(keyByUserOrIP)

	// Outgoing-email trigger. The destination address is supplied by the
	// caller, so without a cap this is an email cannon pointed at any
	// address of the attacker's choosing, sending branded mail from our
	// SMTP domain — a deliverability and blocklist problem long before
	// it's a victim's problem. Three an hour is well clear of a user
	// who mistypes their address a couple of times.
	emailSendLimiter := newEmailSendLimiter(keyByUserOrIP)

	// Liveness: process is up. Used by k8s livenessProbe.
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	// Prometheus scrape endpoint. Exposes the collectors registered in
	// internal/metrics (HTTP histogram, cache hit/miss, WS gauge) plus
	// the Go runtime collectors client_golang installs on init. The
	// InstrumentHTTP middleware (installed above) explicitly skips
	// /metrics so scrapes don't create a self-referential series.
	// /metrics is an information leak on a public host: Go runtime internals,
	// every route's latency profile, and enough request-count detail to
	// fingerprint usage. Development leaves it open for convenience; in
	// production it requires METRICS_TOKEN, and stays off entirely when
	// that isn't set so an operator can't expose it by forgetting to.
	switch {
	case !cfg.IsProduction():
		r.Handle("/metrics", promhttp.Handler())
	case cfg.MetricsToken != "":
		r.Handle("/metrics", requireBearerToken(cfg.MetricsToken, promhttp.Handler()))
		slog.Info("Metrics endpoint enabled behind METRICS_TOKEN")
	default:
		slog.Warn("Metrics endpoint disabled in production — set METRICS_TOKEN to enable /metrics")
	}

	// Readiness: dependencies reachable. Used by k8s readinessProbe so a pod
	// is removed from the Service when Postgres/Redis/QuestDB go away.
	r.Get("/readyz", func(w http.ResponseWriter, req *http.Request) {
		rctx, rcancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer rcancel()
		var errs []string
		if err := pgRepo.Ping(rctx); err != nil {
			errs = append(errs, fmt.Sprintf("postgres: %v", err))
		}
		if err := redisRepo.Ping(rctx); err != nil {
			errs = append(errs, fmt.Sprintf("redis: %v", err))
		}
		if qdbRepo != nil {
			if err := qdbRepo.Ping(rctx); err != nil {
				errs = append(errs, fmt.Sprintf("questdb: %v", err))
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if len(errs) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, `{"status":"unready","errors":%q}`, errs)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ready"}`))
	})

	// Public routes — auth POST endpoints get a stricter rate limit
	// to prevent credential bruteforce (5 attempts per IP per minute).
	r.Get("/login", srv.HandleLoginPage)
	r.Group(func(r chi.Router) {
		r.Use(httprate.LimitByIP(5, 1*time.Minute))
		r.Post("/login", srv.HandleLoginPost)
		r.Post("/signup", srv.HandleSignupPost)
		r.Post("/logout", srv.HandleLogout)
		r.Get("/auth/{provider}/login", srv.HandleOAuthLogin)
		// Callback needs auth middleware to read the session cookie for
		// the account-linking flow (link: prefixed state).
		r.With(authSvc.Middleware).Get("/auth/{provider}/callback", srv.HandleOAuthCallback)
	})

	// API - Public Data
	r.Get("/v1/history", srv.HandleGetHistory)
	r.Get("/v1/compare", srv.HandleGetCompare)
	r.Get("/v1/symbols", srv.HandleGetSymbols)
	r.Get("/v1/symbols/options", srv.HandleGetSymbolsHTML)
	r.Get("/v1/news", srv.HandleGetNews)
	r.Get("/v1/market-news", srv.HandleGetMarketNews)
	r.Get("/v1/market-summary", srv.HandleGetMarketSummary)
	r.Get("/v1/quote", srv.HandleGetQuote) // single-symbol bid/offer/spread snapshot
	r.Get("/v1/briefing", srv.HandleGetBriefing)
	// Slim sector overview — used by the dashboard heatmap tile.
	// Public counterpart to the Pro-gated /v1/market-sectors endpoint;
	// returns breadth + avg % only, never constituents.
	r.Get("/v1/market-sectors/overview", srv.HandleGetMarketSectorsOverview)

	// OpenAPI specification — public so that documentation tooling and
	// generators (Swagger UI, Postman import, openapi-codegen) can pull
	// the spec without a key. Served in both JSON and YAML.
	r.Get("/v1/openapi.json", openapi.HandleJSON)
	r.Get("/v1/openapi.yaml", openapi.HandleYAML)
	r.Options("/v1/openapi.json", openapi.HandleJSON)
	r.Options("/v1/openapi.yaml", openapi.HandleYAML)
	// Client error telemetry — browsers POST uncaught errors here via
	// sendBeacon. Public (errors can happen pre-auth) and inherits the
	// global per-IP limiter to cap runaway loops.
	r.Post("/v1/client-error", srv.HandleClientError)

	// Email verification link target — public because the click may land
	// in a different browser session than the one that requested it. The
	// token itself is the secret; the handler rejects expired/used tokens
	// and redirects into /terminal on success.
	r.Get("/auth/verify-email", srv.HandleVerifyEmail)

	// VAPID public key — public, no auth. The browser needs it before
	// any session exists to call PushManager.subscribe().
	r.Get("/v1/push/vapid-key", srv.HandleVAPIDPublicKey)

	// PWA assets — must live at the URL root so the service worker
	// scope covers the whole origin and the install banner picks up the
	// manifest. Public and unauthenticated; the browser may fetch them
	// before any session cookie has been set. Icons + screenshots are
	// referenced from the manifest with absolute paths, so they too
	// must answer at the root.
	r.Get("/sw.js", srv.HandleServiceWorker)
	r.Get("/manifest.webmanifest", srv.HandleManifest)
	r.Get("/offline.html", srv.HandleOfflinePage)
	r.Get("/icon-192.png", srv.HandlePWAIcon192)
	r.Get("/icon-512.png", srv.HandlePWAIcon512)
	r.Get("/icon-maskable.png", srv.HandlePWAIconMaskable)
	r.Get("/screenshot-wide.png", srv.HandlePWAScreenshotWide)
	r.Get("/screenshot-mobile.png", srv.HandlePWAScreenshotMobile)

	// MCP is opt-in and authenticated. Its handler returns an HTTP 401 rather
	// than redirecting machine clients when no valid session/API key is present.
	if cfg.MCPEnabled {
		r.Group(func(r chi.Router) {
			r.Use(authSvc.Middleware)
			r.With(mcpLimiter).Post("/mcp", srv.HandleMCP)
		})
		slog.Info("MCP endpoint enabled", "path", "/mcp")
	}

	// API - Session dependent (Guest friendly)
	r.Group(func(r chi.Router) {
		r.Use(authSvc.Middleware)
		r.Get("/", srv.HandleLandingPage)
		r.Get("/terminal", srv.HandleDashboard)
		r.Get("/terminal-pro", srv.HandleDashboardPro)
		// Public API reference — authed middleware is applied so the
		// masthead can render "Open Terminal" vs "Sign in" consistently
		// with the landing page. The content itself is public.
		r.Get("/developers", srv.HandleDevelopersPage)
		// Interactive OpenAPI console. Public — no auth required to
		// *view* the spec; calls executed from inside it still pass
		// through the normal per-endpoint auth rules.
		r.Get("/developers/swagger", srv.HandleSwaggerPage)
		r.Get("/v1/me", srv.HandleMe)
		r.Get("/ws", srv.HandleWS)
		r.Get("/auth/link/{provider}", srv.HandleLinkProvider)
	})

	// API - Strictly Authenticated (Incentive gating)
	r.Group(func(r chi.Router) {
		r.Use(authSvc.Middleware)
		r.Use(authSvc.RequireAuth)
		// User self-service console — account, alerts, API keys.
		// Every section inside is auth-gated on the API layer too, so
		// a deep-link to /settings#alerts from a logged-out tab bounces
		// through the same login flow the old nav modals did.
		r.Get("/settings", srv.HandleSettingsPage)

		r.Get("/v1/me/linked-providers", srv.HandleGetLinkedProviders)
		r.Post("/v1/me/unlink-provider", srv.HandleUnlinkProvider)
		r.With(credentialLimiter).Post("/v1/me/set-password", srv.HandleSetPassword)
		r.With(credentialLimiter).Post("/v1/me/merge-account", srv.HandleMergeAccount)
		r.Get("/v1/watchlist", srv.HandleGetWatchList)
		r.Post("/v1/watchlist", srv.HandleToggleWatchList)
		// API key management — every authenticated user manages their own keys
		r.Get("/v1/me/api-keys", srv.HandleListAPIKeys)
		r.Post("/v1/me/api-keys", srv.HandleCreateAPIKey)
		r.Delete("/v1/me/api-keys/{id}", srv.HandleRevokeAPIKey)

		// Portfolio holdings — available to every authenticated user (not
		// pro-gated; it's the "make it personal and sticky" feature).
		r.Get("/v1/me/portfolio", srv.HandleListPortfolio)
		r.Get("/v1/me/portfolio/history", srv.HandleGetPortfolioHistory)
		r.Post("/v1/me/portfolio", srv.HandleCreatePortfolioHolding)
		r.Patch("/v1/me/portfolio/{id}", srv.HandleUpdatePortfolioHolding)
		r.Delete("/v1/me/portfolio/{id}", srv.HandleDeletePortfolioHolding)

		// Web Push subscription management.
		r.Post("/v1/push/subscribe", srv.HandleSubscribePush)
		r.Post("/v1/push/unsubscribe", srv.HandleUnsubscribePush)

		// Manual email verification path. Available to every authenticated
		// user (not gated to Pro/Admin) — a basic-tier user who later
		// upgrades shouldn't have to re-verify. The write endpoint only
		// starts the flow; consumption happens at the public
		// /auth/verify-email GET above.
		r.With(emailSendLimiter).Post("/v1/me/email/request-verify", srv.HandleRequestEmailVerification)

		// Account management — self-service unlink-email + change-password.
		// Both gated by AuthMiddleware only (any authenticated tier).
		// The unlink-email handler refuses to run when OAuth is linked
		// (those emails are provider-managed); the change-password handler
		// refuses for OAuth-only users without a password (they should use
		// /v1/me/set-password instead).
		r.Post("/v1/me/email/unlink", srv.HandleUnlinkEmail)
		r.With(credentialLimiter).Post("/v1/me/password", srv.HandleChangePassword)

		// Pro-role request flow. A standard user submits a request via
		// POST /v1/me/pro-request; an admin decides it from the admin
		// portal (see the admin route group below). The GET endpoint
		// is what the account page polls to render the current status.
		r.Get("/v1/me/pro-request", srv.HandleGetProRequest)
		r.Post("/v1/me/pro-request", srv.HandleCreateProRequest)

		// Daily watchlist digest opt-in/out. Defaults to enabled for
		// every user (see migration 14). Toggling off silences only
		// the daily summary push; per-stock alert-rule pushes are
		// untouched.
		r.Get("/v1/me/digest-preference", srv.HandleGetDigestPreference)
		r.Post("/v1/me/digest-preference", srv.HandleSetDigestPreference)
	})

	// API - Premium Data (Pro/Admin restricted)
	r.Group(func(r chi.Router) {
		r.Use(authSvc.Middleware)
		r.Use(authSvc.RequireProOrAdmin)
		r.Get("/v1/stock/export/{symbol}/csv", srv.HandleExportStockData)
		r.Get("/v1/market-sectors", srv.HandleGetMarketSectors)
		r.Get("/v1/market-sectors/history", srv.HandleGetMarketSectorsHistory)
		r.With(llmLimiter).Post("/v1/query", srv.HandleNaturalQuery)
		// AI Market Oracle — burns an LLM call per stock visit, so the
		// access tier is the same as the conversational query: Pro/Admin.
		// Basic-tier users see the "upgrade" panel in the sidebar instead.
		r.With(llmLimiter).Get("/v1/ai-insight", srv.HandleGetAIInsight)
		// Deterministic technical backtest — pure indicator math, no LLM
		// call, but still O(N) compute per request. Rate-limited to
		// prevent a bot from hammering every symbol.
		r.With(httprate.LimitByIP(10, 1*time.Minute)).Get("/v1/backtest", srv.HandleBacktest)

		// Watchlist alert rules — gated to Pro/Admin so a basic-tier
		// account can't spin up the evaluator's per-rule workload (email
		// send, RSI history lookup, push fan-out). POST additionally
		// checks for a verified email inside the handler; a Pro user
		// without a linked/verified email still gets 403 there.
		r.Get("/v1/me/alerts", srv.HandleListAlertRules)
		r.Post("/v1/me/alerts", srv.HandleCreateAlertRule)
		r.Patch("/v1/me/alerts/{id}", srv.HandleUpdateAlertRule)
		r.Delete("/v1/me/alerts/{id}", srv.HandleDeleteAlertRule)
		// Drawer feed + read-state (same Pro/Admin gate).
		r.Get("/v1/me/alerts/events", srv.HandleListAlertEvents)
		r.Post("/v1/me/alerts/events/{id}/read", srv.HandleMarkAlertEventRead)
		r.Post("/v1/me/alerts/events/read-all", srv.HandleMarkAllAlertEventsRead)
	})

	// Admin routes
	r.Group(func(r chi.Router) {
		r.Use(authSvc.Middleware)
		r.Use(authSvc.AdminMiddleware)

		r.Get("/admin", srv.HandleAdminPage)
		r.Get("/admin/audit", srv.HandleAdminAuditPage)
		r.Get("/v1/admin/users", srv.HandleAdminUsersList)
		r.Get("/v1/admin/audit", srv.HandleAdminAuditLog)
		r.Get("/v1/admin/sectors", srv.HandleAdminListSectors)
		r.Get("/v1/admin/alerts", srv.HandleAdminAlerts)
		// WatchList alert administration — cross-user visibility into
		// alert rules and fire history, plus an admin-scope delete.
		r.Get("/v1/admin/alert-rules", srv.HandleAdminListAlertRules)
		r.Get("/v1/admin/alert-events", srv.HandleAdminListAlertEvents)
		r.Get("/v1/admin/alert-stats", srv.HandleAdminAlertStats)
		// Pro-role request administration. List = pending queue,
		// count = badge driver. Decide is in the destructive group
		// below (it mutates a user's role).
		r.Get("/v1/admin/pro-requests", srv.HandleAdminListProRequests)
		r.Get("/v1/admin/pro-requests/count", srv.HandleAdminProRequestCount)

		// Destructive admin operations: stricter rate limit (10 req/min per IP)
		r.Group(func(r chi.Router) {
			r.Use(httprate.LimitByIP(10, 1*time.Minute))
			r.Post("/upload", srv.HandleUpload) // multipart max 32MB default
			r.Post("/v1/admin/users/{id}/role", srv.HandleAdminUserUpdateRole)
			r.Post("/v1/admin/users/{id}/lock", srv.HandleAdminUserToggleLock)
			r.Post("/v1/admin/users/{id}/password", srv.HandleAdminUserResetPassword)
			r.Delete("/v1/admin/users/{id}", srv.HandleAdminUserDelete)
			r.Put("/v1/admin/sectors", srv.HandleAdminUpsertSector)
			r.Delete("/v1/admin/sectors/{symbol}", srv.HandleAdminDeleteSector)
			r.Post("/v1/admin/alerts/{id}/ack", srv.HandleAdminAckAlert)
			r.Delete("/v1/admin/alert-rules/{id}", srv.HandleAdminDeleteAlertRule)
			// Pro-role decision endpoint — mutates user.role on
			// approval, so it sits behind the same destructive
			// rate limit as the other role-mutating routes.
			r.Post("/v1/admin/pro-requests/{id}/decide", srv.HandleAdminDecideProRequest)
			// WatchList-digest test trigger. Default scope is
			// self-only; ?all=true fans out to every subscribed
			// user with a watchList (same code path as the daily
			// post-scrape dispatch). Lives in the destructive
			// group because broadcast mode delivers a real
			// notification to every subscriber.
			r.Post("/v1/admin/push/test-digest", srv.HandleAdminTestDigest)
		})
	})

	// Static Assets with conditional cache headers
	r.Group(func(r chi.Router) {
		r.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if cfg.IsProduction() {
					w.Header().Set("Cache-Control", "public, max-age=86400, immutable")
				} else {
					w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
				}
				next.ServeHTTP(w, r)
			})
		})
		// Everything is served out of the built dist/ tree so Vite-compiled
		// CSS + JS are what reach the browser — the raw ui/src tree is no
		// longer self-sufficient (CSS needs Tailwind's PostCSS pass).
		//
		// Development prefers the on-disk copy so `npm run build` shows up
		// on the next request without a Go restart. When it isn't there we
		// fall back to the compile-time snapshot: the container inherits
		// APP_ENV=development from the same .env a workstation uses, but
		// ships the assets embedded with no source tree to read.
		distFS, err := fs.Sub(ui.Files, "dist")
		if err != nil {
			slog.Error("Failed to sub-fs for static assets", "error", err)
			os.Exit(1)
		}
		var assets http.FileSystem = http.FS(distFS)
		if cfg.AppEnv == "development" {
			if _, statErr := os.Stat("ui/dist"); statErr == nil {
				assets = http.Dir("ui/dist")
			} else {
				slog.Warn("ui/dist not found on disk, serving embedded assets", "error", statErr)
			}
		}
		fileServer := http.FileServer(assets)

		r.Handle("/static/*", http.StripPrefix("/static/", fileServer))

		// The PWA files that have to live at the origin root are NOT
		// registered here. They used to be, and because chi silently
		// overwrites a duplicate method+pattern rather than panicking, the
		// registrations in this group won and the dedicated handlers above
		// (srv.HandleServiceWorker, HandleManifest, HandleOfflinePage,
		// HandlePWAIcon*) became unreachable code. The visible symptom was
		// /manifest.webmanifest going out as text/plain — HandleManifest
		// sets application/manifest+json, but it was never called — and
		// the offline shell inheriting this group's immutable day-long
		// cache, so a redeploy could not refresh it.
		//
		// /favicon.png stays because it has no dedicated handler; it is
		// the one root asset this group is genuinely responsible for.
		r.Get("/favicon.png", func(w http.ResponseWriter, r *http.Request) {
			fileServer.ServeHTTP(w, r)
		})
	})

	// Graceful shutdown. ReadHeaderTimeout is the Slowloris guard —
	// without it an attacker can send headers one byte per second and
	// ReadTimeout never fires because the body hasn't started. 8 KiB
	// MaxHeaderBytes is generous for cookies + user-agent but caps the
	// header-bomb blast radius.
	httpSrv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    8 << 10, // 8 KiB
	}

	go func() {
		slog.Info("Server starting", "port", cfg.Port)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("Shutting down gracefully...")
	// The k8s terminationGracePeriodSeconds is 30s and the preStop hook
	// holds the pod for ~5s to let the LB stop routing. That leaves ~25s
	// of real drain time before SIGKILL; use 20s so both httpSrv.Shutdown
	// and srv.Shutdown (detached background goroutines) share the budget
	// with a small safety margin.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer shutdownCancel()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("Server forced to shutdown", "error", err)
		os.Exit(1)
	}
	// Drain detached background writes (cache fills, briefing regen) that
	// were spawned via goBackground. http.Server.Shutdown only joins
	// in-flight handlers; these survived behind WithoutCancel-style
	// detachment until we wired them through srv.Shutdown.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("Background goroutines did not drain in time", "error", err)
	}
	cancel() // cancel the main context to stop the daemon
	slog.Info("Server stopped.")
}

// postScrapeAdapter bridges the collector's PostScrapeHook interface to the
// analysis + repository packages. It runs anomaly detection, generates the
// daily briefing, then evaluates watchList alert rules against the fresh
// market snapshot.
type postScrapeAdapter struct {
	pgRepo         *repository.PostgresRepo
	qdbRepo        *repository.QuestDBRepo
	redisRepo      *repository.RedisRepo
	briefingSvc    *analysis.BriefingService
	alertEvaluator *alerts.Evaluator
	pushSvc        *push.Service
}

// DispatchWatchListDigest converts the collector's snapshot into the
// push package's neutral SymbolSnap shape and asks the push service to
// fan out a per-user notification. The collector already derives
// PercentChange against the previous session's close, so there's nothing
// to recompute here.
func (a *postScrapeAdapter) DispatchWatchListDigest(ctx context.Context, snapshot []collector.LiveQuote) {
	if a.pushSvc == nil || len(snapshot) == 0 {
		return
	}
	snapMap := make(map[string]push.SymbolSnap, len(snapshot))
	for _, s := range snapshot {
		if s.Symbol == "" {
			continue
		}
		snapMap[s.Symbol] = push.SymbolSnap{
			Symbol:        s.Symbol,
			LastPrice:     s.LastPrice,
			PercentChange: s.PercentChange,
		}
	}
	a.pushSvc.SendWatchListDigest(ctx, snapMap)
}

func (a *postScrapeAdapter) RunPostScrape(ctx context.Context, qdb *repository.QuestDBRepo, symbols []string) {
	logger := slog.With("component", "post-scrape")

	// 1. Anomaly detection
	var allAlerts []repository.DataAlert
	for _, sym := range symbols {
		history, err := qdb.GetOHLC(ctx, sym, "1d")
		if err != nil || len(history) < 5 {
			continue
		}
		latest := history[len(history)-1]
		prev := history[:len(history)-1]
		anomalies := analysis.DetectAnomalies(sym, latest, prev)
		for _, al := range anomalies {
			allAlerts = append(allAlerts, repository.DataAlert{
				Symbol:      al.Symbol,
				TradingDate: al.TradingDate.Format("2006-01-02"),
				AlertType:   string(al.Type),
				Severity:    string(al.Severity),
				Message:     al.Message,
				Metadata:    al.Metadata,
			})
		}
	}
	if len(allAlerts) > 0 {
		if err := a.pgRepo.SaveAlerts(ctx, allAlerts); err != nil {
			logger.Error("Failed to save anomaly alerts", "error", err, "count", len(allAlerts))
		} else {
			logger.Info("Data quality alerts saved", "count", len(allAlerts))
		}
	}

	// 2. Daily briefing (top-10 most active symbols).
	//    Skip if today's briefing already exists (avoids redundant LLM calls
	//    on startup seed-scrape when a briefing was already generated).
	if a.briefingSvc == nil || len(symbols) == 0 {
		return
	}
	tradingDate := time.Now().UTC().Format("2006-01-02")
	if existing, err := a.pgRepo.GetLatestBriefing(ctx); err == nil && existing != nil && existing.TradingDate == tradingDate {
		logger.Info("Daily briefing already exists, skipping", "date", tradingDate)
		return
	}

	top := symbols
	if len(top) > 10 {
		top = top[:10]
	}
	insights, err := a.briefingSvc.GenerateDailyBriefing(ctx, top)
	if err != nil {
		logger.Warn("Daily briefing generation failed", "error", err)
		return
	}

	// Persist per-symbol insights
	for _, ins := range insights {
		insJSON, _ := json.Marshal(ins)
		if err := a.pgRepo.SaveBriefing(ctx, tradingDate, ins.Symbol, insJSON); err != nil {
			logger.Warn("Failed to save briefing", "symbol", ins.Symbol, "error", err)
		}
	}

	// Generate and persist market summary
	summary, err := a.briefingSvc.GenerateMarketSummary(ctx, insights)
	if err != nil {
		logger.Warn("Market summary generation failed", "error", err)
		return
	}

	// Compute simple gainers/losers for the summary record
	var avgSentiment float64
	for _, ins := range insights {
		avgSentiment += ins.Sentiment
	}
	if len(insights) > 0 {
		avgSentiment /= float64(len(insights))
	}

	if err := a.pgRepo.SaveMarketSummary(ctx, tradingDate, summary, nil, nil, avgSentiment); err != nil {
		logger.Warn("Failed to save market summary", "error", err)
	} else {
		logger.Info("Daily briefing saved", "date", tradingDate, "symbols", len(insights))
	}

	// Bust the briefing cache AFTER the save so the next /v1/briefing
	// fetch rebuilds from the freshly-saved row. The scraper already
	// wipes gse:data:* before this hook runs, but any request arriving
	// between that wipe and the SaveMarketSummary above would have
	// re-cached yesterday's briefing (fetched from Postgres before the
	// new one landed) for the full 7-day dataCacheTTL — which is why
	// the "Today's Briefing" panel could stay frozen on an old date
	// for days at a time even as post-scrape ran daily. Hitting the
	// explicit keys here closes that race. Soft-fail — a cache blip
	// shouldn't block alert evaluation downstream.
	if a.redisRepo != nil {
		if err := a.redisRepo.Delete(ctx, "gse:data:briefing", "gse:data:market-summary"); err != nil {
			logger.Warn("post-save briefing cache bust failed", "error", err)
		}
	}

	// 3. WatchList alert evaluation. Runs AFTER briefing so the audit log
	//    ordering is deterministic (briefing → alerts) and so a slow LLM
	//    call never blocks user-facing email/WS notifications. Pulls the
	//    latest market snapshot straight from QuestDB — it's already the
	//    source of truth for the Symbol/LastPrice/PercentChange triple
	//    the evaluator needs.
	if a.alertEvaluator == nil || a.qdbRepo == nil {
		return
	}
	snapshot, err := a.qdbRepo.GetMarketSummary(ctx)
	if err != nil {
		logger.Warn("alerts: snapshot load failed", "error", err)
		return
	}
	alertSnapshot := make([]alerts.Snapshot, 0, len(snapshot))
	for _, item := range snapshot {
		alertSnapshot = append(alertSnapshot, alerts.Snapshot{
			Symbol:        item.Symbol,
			LastPrice:     item.LastPrice,
			PercentChange: item.PercentChange,
		})
	}
	fired, err := a.alertEvaluator.Run(ctx, alertSnapshot)
	if err != nil {
		logger.Warn("alerts: evaluator error", "error", err, "fired", fired)
	} else if fired > 0 {
		logger.Info("alerts fired", "count", fired)
	}
}

// generateSecurePassword generates a cryptographically random hex password
func generateSecurePassword(length int) string {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		// Crypto failure is unrecoverable: a degraded entropy source must
		// never silently produce a predictable bootstrap password.
		slog.Error("Crypto entropy failure generating bootstrap password", "error", err)
		os.Exit(1)
	}
	return hex.EncodeToString(b)[:length]
}

// Rate-limit budgets for the authenticated credential + outgoing-email
// endpoints. Named constants so the test can assert against the same
// numbers the router installs.
const (
	credentialPerUserPerMin = 5
	credentialPerIPPerMin   = 20
	emailSendPerUserPerHour = 3
	emailSendPerIPPerHour   = 10
)

// newCredentialLimiter caps endpoints that verify a password.
//
// These sit behind RequireAuth, so they were only covered by the global
// 300/min budget — which made POST /v1/me/merge-account a password-guessing
// oracle: it bcrypt-checks an arbitrary username + password and returns
// distinguishable statuses, at 300 attempts a minute, entirely outside the
// 5/min limiter guarding /login.
//
// Two limiters rather than one. Keyed per-user alone, an attacker just
// registers more accounts to get more buckets; keyed per-IP alone, one
// office NAT shares a bucket between unrelated people. Both must pass, so
// neither dodge works. The per-IP ceiling is the looser of the two so
// genuine shared egress isn't punished for one user's typos.
func newCredentialLimiter(keyFn httprate.KeyFunc) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		perUser := httprate.Limit(credentialPerUserPerMin, 1*time.Minute, httprate.WithKeyFuncs(keyFn))
		return httprate.LimitByIP(credentialPerIPPerMin, 1*time.Minute)(perUser(next))
	}
}

// newEmailSendLimiter caps the endpoint that triggers outgoing mail.
//
// The destination address is supplied by the caller, so without a cap this
// is an email cannon pointed at any address of the attacker's choosing,
// sending branded mail from our SMTP domain — a deliverability and
// blocklist problem long before it is the recipient's problem. Three an
// hour is well clear of a user who mistypes their address a couple of
// times.
func newEmailSendLimiter(keyFn httprate.KeyFunc) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		perUser := httprate.Limit(emailSendPerUserPerHour, 1*time.Hour, httprate.WithKeyFuncs(keyFn))
		return httprate.LimitByIP(emailSendPerIPPerHour, 1*time.Hour)(perUser(next))
	}
}

// requireBearerToken gates a handler behind `Authorization: Bearer <token>`.
// Comparison is constant-time so a wrong token can't be recovered by timing
// the response. Used for /metrics, which is unauthenticated otherwise.
func requireBearerToken(token string, next http.Handler) http.Handler {
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, want) != 1 {
			// 404 rather than 401: an unauthenticated prober learns
			// nothing about whether the endpoint exists.
			http.NotFound(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
