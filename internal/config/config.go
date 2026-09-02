// Package config centralises environment-driven configuration. Values are
// loaded once at startup via Load(), validated, and passed by reference into
// services that need them. Avoid calling os.Getenv elsewhere in the codebase.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// Runtime
	AppEnv   string // "development" or "production"
	Port     string
	LogLevel string

	// Postgres
	PostgresHost     string
	PostgresPort     string
	PostgresUser     string
	PostgresPassword string
	PostgresDB       string

	// QuestDB
	QuestDBURL    string // pgwire DSN
	QuestDBILPURL string // ILP TCP host:port

	// Redis
	RedisURL string

	// Auth
	JWTSecret string

	// External APIs
	//
	// GeminiAPIKey is the primary LLM provider. AnthropicAPIKey and
	// OpenAIAPIKey are optional fallbacks — when configured they're
	// chained behind Gemini in that order via FallbackLLMClient so a
	// 429 on one provider's bucket doesn't take the AI Oracle down.
	// Leaving either empty skips that provider in the chain.
	GeminiAPIKey    string
	AnthropicAPIKey string
	OpenAIAPIKey    string
	HTTPTimeout     time.Duration

	// OpenAI-compatible endpoint (optional, appended last in the chain).
	// Any server speaking POST /chat/completions works: Ollama, vLLM,
	// LM Studio, OpenRouter, Groq, LiteLLM. BaseURL and Model are both
	// required to enable it; the key is optional so an unauthenticated
	// local runtime needs no credential.
	OpenAICompatBaseURL string
	OpenAICompatAPIKey  string
	OpenAICompatModel   string
	// OpenAICompatName labels the provider in logs (default
	// "openai-compatible").
	OpenAICompatName string

	// Google OAuth (optional — leave empty to disable)
	GoogleClientID     string
	GoogleClientSecret string
	GoogleRedirectURL  string

	// Outgoing email (optional — leave SMTPHost empty to disable. When
	// disabled, alert rules still evaluate and fire to the in-app drawer
	// + audit log; only the outgoing email step is skipped.)
	SMTPHost     string
	SMTPPort     int
	SMTPUser     string
	SMTPPassword string
	SMTPFrom     string // e.g. "GSE Terminal <alerts@yourdomain.com>"

	// AppBaseURL is the public origin used when building links inside
	// outgoing emails (e.g. deep-links back to the stock the alert fired
	// on). Falls back to the first ALLOWED_ORIGINS entry if unset.
	AppBaseURL string

	// Web Push (optional — leave VAPID keys empty to disable. When
	// disabled, alert rules still fire to the in-app drawer, audit log,
	// and email; only the push-notification step is skipped.)
	VAPIDPublicKey  string
	VAPIDPrivateKey string

	// WebPushAllowedHosts overrides the allowlist of push-service hosts
	// we are willing to send notifications to. The endpoint in a push
	// subscription is a URL the server later POSTs to, so accepting an
	// arbitrary one is a server-side request forgery hole — see
	// internal/push/endpoint.go. Empty uses the built-in list covering
	// Chrome, Firefox, Safari, and legacy Edge. Set it only to admit a
	// browser whose push service isn't in that list; an entry beginning
	// with "." matches that domain and its subdomains.
	WebPushAllowedHosts []string

	// TrustProxy makes the server read the client address from the
	// X-Forwarded-For / X-Real-IP headers a reverse proxy sets. Required
	// for correct per-client rate limiting behind Caddy/nginx, and unsafe
	// when the app is exposed directly — those headers are client-supplied,
	// so trusting them without a proxy in front lets anyone forge an
	// address and evade every limit.
	TrustProxy bool

	// MetricsToken guards /metrics. In production the endpoint is disabled
	// unless this is set, and then requires `Authorization: Bearer <token>`.
	// Development leaves it open.
	MetricsToken string

	// MCPEnabled exposes the authenticated, read-only Model Context Protocol
	// endpoint. It is disabled by default so deployments must opt in.
	MCPEnabled bool

	// WebSocket
	AllowedOrigins []string // comma-separated, used for CheckOrigin
}

// PostgresConnString builds the pgx DSN from the discrete fields.
func (c *Config) PostgresConnString() string {
	host := c.PostgresHost
	if host == "" {
		host = "localhost"
	}
	port := c.PostgresPort
	if port == "" {
		port = "5432"
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		c.PostgresUser, c.PostgresPassword, host, port, c.PostgresDB)
}

// IsProduction returns true when running in production mode.
func (c *Config) IsProduction() bool {
	return c.AppEnv == "production"
}

// OpenAICompatEnabled returns true when an OpenAI-compatible endpoint is
// fully configured. Both the URL and the model are needed — unlike the
// hosted providers there's no default model to fall back on.
func (c *Config) OpenAICompatEnabled() bool {
	return c.OpenAICompatBaseURL != "" && c.OpenAICompatModel != ""
}

// GoogleOAuthEnabled returns true when Google OAuth credentials are configured.
func (c *Config) GoogleOAuthEnabled() bool {
	return c.GoogleClientID != "" && c.GoogleClientSecret != "" && c.GoogleRedirectURL != ""
}

// EmailEnabled returns true when SMTP credentials are fully configured.
// The alerts pipeline consults this before attempting an outgoing send;
// when false, the rest of the evaluator (in-app push + audit log) still
// runs so the feature degrades gracefully in environments that haven't
// wired SMTP yet.
func (c *Config) EmailEnabled() bool {
	return c.SMTPHost != "" && c.SMTPPort > 0 && c.SMTPFrom != ""
}

// WebPushEnabled returns true when VAPID keys are configured. The alerts
// pipeline consults this before attempting a push send; when false, the
// rest of the evaluator (email, in-app push, audit) still runs.
func (c *Config) WebPushEnabled() bool {
	return c.VAPIDPublicKey != "" && c.VAPIDPrivateKey != ""
}

// ResolvedAppBaseURL returns the explicitly-configured AppBaseURL or, if
// unset, the first entry from AllowedOrigins. Used to build deep-links
// inside outgoing alert emails.
func (c *Config) ResolvedAppBaseURL() string {
	if c.AppBaseURL != "" {
		return c.AppBaseURL
	}
	if len(c.AllowedOrigins) > 0 {
		return c.AllowedOrigins[0]
	}
	return ""
}

// Load reads environment variables, applies defaults, and returns a validated
// Config. It returns an error listing every missing required variable so the
// operator can fix them all in one cycle.
func Load() (*Config, error) {
	cfg := &Config{
		AppEnv:              getEnv("APP_ENV", "development"),
		Port:                getEnv("PORT", "8080"),
		LogLevel:            getEnv("LOG_LEVEL", "info"),
		PostgresHost:        getEnv("POSTGRES_HOST", "localhost"),
		PostgresPort:        getEnv("POSTGRES_PORT", "5432"),
		PostgresUser:        os.Getenv("POSTGRES_USER"),
		PostgresPassword:    os.Getenv("POSTGRES_PASSWORD"),
		PostgresDB:          os.Getenv("POSTGRES_DB"),
		QuestDBURL:          os.Getenv("QUESTDB_URL"),
		QuestDBILPURL:       os.Getenv("QUESTDB_ILP_TCP_URL"),
		RedisURL:            getEnv("REDIS_URL", "localhost:6379"),
		JWTSecret:           os.Getenv("JWT_SECRET"),
		GeminiAPIKey:        os.Getenv("GEMINI_API_KEY"),
		AnthropicAPIKey:     os.Getenv("ANTHROPIC_API_KEY"),
		OpenAIAPIKey:        os.Getenv("OPENAI_API_KEY"),
		OpenAICompatBaseURL: os.Getenv("OPENAI_COMPAT_BASE_URL"),
		OpenAICompatAPIKey:  os.Getenv("OPENAI_COMPAT_API_KEY"),
		OpenAICompatModel:   os.Getenv("OPENAI_COMPAT_MODEL"),
		OpenAICompatName:    os.Getenv("OPENAI_COMPAT_NAME"),
		HTTPTimeout:         time.Duration(getEnvInt("HTTP_TIMEOUT_SECONDS", 10)) * time.Second,
		GoogleClientID:      os.Getenv("GOOGLE_CLIENT_ID"),
		GoogleClientSecret:  os.Getenv("GOOGLE_CLIENT_SECRET"),
		GoogleRedirectURL:   os.Getenv("GOOGLE_REDIRECT_URL"),
		SMTPHost:            os.Getenv("SMTP_HOST"),
		SMTPPort:            getEnvInt("SMTP_PORT", 587),
		SMTPUser:            os.Getenv("SMTP_USER"),
		SMTPPassword:        os.Getenv("SMTP_PASSWORD"),
		SMTPFrom:            os.Getenv("SMTP_FROM"),
		AppBaseURL:          os.Getenv("APP_BASE_URL"),
		VAPIDPublicKey:      os.Getenv("VAPID_PUBLIC_KEY"),
		VAPIDPrivateKey:     os.Getenv("VAPID_PRIVATE_KEY"),
		WebPushAllowedHosts: parseList(getEnv("WEBPUSH_ALLOWED_HOSTS", "")),
		TrustProxy:          getEnvBool("TRUST_PROXY", false),
		MetricsToken:        os.Getenv("METRICS_TOKEN"),
		MCPEnabled:          getEnvBool("MCP_ENABLED", false),
		AllowedOrigins:      parseList(getEnv("ALLOWED_ORIGINS", "")),
	}

	// Validate required vars and accumulate errors so a misconfigured deploy
	// fails fast with a complete list rather than one var at a time.
	required := map[string]string{
		"POSTGRES_USER":       cfg.PostgresUser,
		"POSTGRES_PASSWORD":   cfg.PostgresPassword,
		"POSTGRES_DB":         cfg.PostgresDB,
		"JWT_SECRET":          cfg.JWTSecret,
		"QUESTDB_URL":         cfg.QuestDBURL,
		"QUESTDB_ILP_TCP_URL": cfg.QuestDBILPURL,
		"ALLOWED_ORIGINS":     getEnv("ALLOWED_ORIGINS", ""),
	}
	var missing []string
	for k, v := range required {
		if v == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	if _, err := strconv.Atoi(cfg.Port); err != nil {
		return nil, fmt.Errorf("invalid PORT value %q: %w", cfg.Port, err)
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func parseList(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// getEnvBool reads a boolean env var. Accepts the spellings people
// actually write in a .env file; anything unparseable falls back to def
// rather than failing the boot on a stray "yes".
func getEnvBool(key string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}
