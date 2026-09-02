package server

import (
	"net/http"

	"github.com/teckdroids/ges-data-engine/internal/auth"
)

// HandleLandingPage serves the public marketing page at /. The nav adapts
// to whether the visitor already has a session, and the marquee ticker is
// hydrated server-side from the most recent market summary so the page is
// fully meaningful even before any client-side JavaScript runs.
func (s *Server) HandleLandingPage(w http.ResponseWriter, r *http.Request) {
	user := auth.FromContext(r.Context())
	s.renderTemplate(w, "index.html", map[string]interface{}{
		"IsAuthenticated": !user.IsGuest(),
		"Username":        user.Username,
		"Ticker":          s.fetchTickerSnapshot(r),
		// Two passes so the marquee CSS animation loops seamlessly.
		"TickerPasses": []bool{false, true},
	})
}

// HandleDashboard serves the terminal dashboard at /terminal. Works for both
// guests (limited features) and authenticated users. The marquee ticker is
// hydrated server-side from the same snapshot used by the landing page.
func (s *Server) HandleDashboard(w http.ResponseWriter, r *http.Request) {
	user := auth.FromContext(r.Context())
	username := user.Username
	if username == "" {
		username = "Guest User"
	}
	s.renderTemplate(w, "terminal.html", map[string]interface{}{
		"Username":     username,
		"UserRole":     user.Role,
		"Ticker":       s.fetchTickerSnapshot(r),
		"TickerPasses": []bool{false, true},
	})
}

// HandleDashboardPro serves the redesigned terminal preview at /terminal-pro.
// Same data contract as HandleDashboard so every JS module that the page
// loads (auth-ticker, watchlist, portfolio, alerts, charts, etc.) hydrates
// identically. Lives alongside /terminal during the redesign rollout.
func (s *Server) HandleDashboardPro(w http.ResponseWriter, r *http.Request) {
	user := auth.FromContext(r.Context())
	username := user.Username
	if username == "" {
		username = "Guest User"
	}
	s.renderTemplate(w, "ui-pro.html", map[string]interface{}{
		"Username":     username,
		"UserRole":     user.Role,
		"Ticker":       s.fetchTickerSnapshot(r),
		"TickerPasses": []bool{false, true},
	})
}

func (s *Server) HandleAdminPage(w http.ResponseWriter, r *http.Request) {
	username, _ := r.Context().Value(auth.UsernameKey).(string)
	role, _ := r.Context().Value(auth.RoleKey).(string)

	s.renderTemplate(w, "admin.html", map[string]interface{}{
		"Username": username,
		"UserRole": role,
	})
}

func (s *Server) HandleLoginPage(w http.ResponseWriter, r *http.Request) {
	s.renderTemplate(w, "login.html", nil)
}

// HandleDevelopersPage serves the public API reference at /developers.
// The page is static — no server-side rendering beyond the masthead's
// auth-awareness so the nav can show "Open Terminal" or "Sign in"
// consistently with the landing page. Mock response bodies for the
// interactive "Try" buttons live entirely in ui/src/developers.js so
// this handler has no per-endpoint coupling.
func (s *Server) HandleDevelopersPage(w http.ResponseWriter, r *http.Request) {
	user := auth.FromContext(r.Context())
	s.renderTemplate(w, "developers.html", map[string]interface{}{
		"IsAuthenticated": !user.IsGuest(),
		"Username":        user.Username,
	})
}

// HandleSwaggerPage serves the interactive OpenAPI console at
// /developers/swagger. The page boots Swagger UI from the CDN and
// points it at /v1/openapi.json — everything the user sees is driven
// by the embedded spec, so there is no server-side coupling to the
// list of endpoints beyond serving the HTML shell.
func (s *Server) HandleSwaggerPage(w http.ResponseWriter, r *http.Request) {
	s.renderTemplate(w, "swagger.html", nil)
}

// HandleMCPDocsPage serves the public connection guide for the authenticated,
// remote MCP endpoint. It is separate from OpenAPI because MCP uses JSON-RPC
// rather than the terminal's REST contract.
func (s *Server) HandleMCPDocsPage(w http.ResponseWriter, r *http.Request) {
	user := auth.FromContext(r.Context())
	s.renderTemplate(w, "mcp.html", map[string]interface{}{
		"IsAuthenticated": !user.IsGuest(),
	})
}

// HandleServiceWorker serves the PWA service worker from the root path.
// SWs scope to where they're served from, so this MUST live at /sw.js,
// not /static/sw.js — otherwise the SW could only intercept /static/*
// requests instead of the whole app. The Service-Worker-Allowed header
// lets the SW expand its scope explicitly even if the file moves later.
func (s *Server) HandleServiceWorker(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Header().Set("Service-Worker-Allowed", "/")
	// Don't long-cache the SW — browsers already byte-compare on every
	// load to detect changes; an aggressive cache here would delay
	// version bumps without saving meaningful bandwidth.
	w.Header().Set("Cache-Control", "no-cache")
	s.serveStaticFile(w, r, "sw.js")
}

// HandleManifest serves the web app manifest from /manifest.webmanifest.
// Located at the root (not /static/) so the install banner triggers and
// `start_url: /terminal` resolves cleanly.
func (s *Server) HandleManifest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/manifest+json; charset=utf-8")
	s.serveStaticFile(w, r, "manifest.webmanifest")
}

// HandleOfflinePage serves the static offline shell the SW falls back to
// when a navigation request fails. Inlined CSS/HTML; no JS dependency
// so it works even when the runtime cache is empty.
func (s *Server) HandleOfflinePage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// Revalidate rather than cache: the shell is what users see when the
	// network is down, so serving a stale copy from a previous deploy is
	// the one case where it matters most that it is current.
	w.Header().Set("Cache-Control", "no-cache")
	s.serveStaticFile(w, r, "offline.html")
}

// PWA install assets — icons + screenshots referenced from the manifest.
// One handler per file so the route table stays explicit; serveStaticFile
// already streams from disk in dev and from embed.FS in prod.
//
// These sit outside the /static/* group, so they do not inherit its
// cache-control middleware and have to set their own. Icons and
// screenshots are content-addressed by the manifest and change only on a
// redeploy, so a day is right — but not `immutable`, which the static
// group applies and which would stop a redeploy from ever refreshing them.
func (s *Server) servePWAAsset(w http.ResponseWriter, r *http.Request, name string) {
	if s.cfg.IsProduction() {
		w.Header().Set("Cache-Control", "public, max-age=86400")
	} else {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	}
	s.serveStaticFile(w, r, name)
}

func (s *Server) HandlePWAIcon192(w http.ResponseWriter, r *http.Request) {
	s.servePWAAsset(w, r, "icon-192.png")
}
func (s *Server) HandlePWAIcon512(w http.ResponseWriter, r *http.Request) {
	s.servePWAAsset(w, r, "icon-512.png")
}
func (s *Server) HandlePWAIconMaskable(w http.ResponseWriter, r *http.Request) {
	s.servePWAAsset(w, r, "icon-maskable.png")
}
func (s *Server) HandlePWAScreenshotWide(w http.ResponseWriter, r *http.Request) {
	s.servePWAAsset(w, r, "screenshot-wide.png")
}
func (s *Server) HandlePWAScreenshotMobile(w http.ResponseWriter, r *http.Request) {
	s.servePWAAsset(w, r, "screenshot-mobile.png")
}

// HandleSettingsPage serves the user self-service console at /settings.
// Consolidates account (identity/email/password), watchlist alert rules,
// and API-key management into one page. Auth-gated at the router level
// (RequireAuth) — the modals this replaced were only visible to signed-in
// users anyway, so a public /settings doesn't make sense.
func (s *Server) HandleSettingsPage(w http.ResponseWriter, r *http.Request) {
	user := auth.FromContext(r.Context())
	s.renderTemplate(w, "settings.html", map[string]interface{}{
		"Username": user.Username,
		"UserRole": user.Role,
	})
}
