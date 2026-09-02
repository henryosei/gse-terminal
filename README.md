# GSE Terminal

A market data terminal for the **Ghana Stock Exchange**: end-of-day prices
for every listed counter, charts and technical indicators, watchlists with
alerting, and LLM-backed market commentary — served from a single Go binary
with an embedded UI.

## What it does

- **Daily ingest.** Pulls the exchange's own "Daily Shares & ETFs" table
  from gse.com.gh at 16:30 UTC and writes it to QuestDB. The same CSV
  format can be uploaded by hand from the admin panel; both paths share one
  parser, so a backfill and a scheduled run behave identically.
- **Market data.** OHLC history, symbol comparison, sector rotation, top
  movers, bid/offer spreads, and a CSV export — 61 `/v1/*` endpoints.
- **Watchlists and alerts.** Per-user rules evaluated after every ingest,
  delivered in-app, by email, and by Web Push.
- **AI oracle.** Per-symbol commentary and a daily market briefing, plus a
  natural-language query endpoint that generates QuestDB SQL, validates it
  as read-only, and retries once using the database's own error when a
  statement is rejected.
- **Portfolio.** Holdings, reconstructed value history, and backtesting.

## Architecture

| Piece | Role |
|---|---|
| Go 1.25 (`cmd/server`) | HTTP API, WebSocket hub, scheduler, embedded UI |
| QuestDB | Price time series (`equities`, one row per symbol per session) |
| Postgres | Users, watchlists, alert rules, briefings, audit log |
| Redis | Derived-data cache, sessions, rate-limit and digest state |
| Caddy | TLS termination in production |

`internal/` is organised by domain — `collector` (scraping), `ingestor`
(CSV parsing), `analysis` (indicators, LLM clients, NL→SQL), `alerts`,
`auth`, `push`, `repository` (all three datastores), `server` (handlers).

Two frontends live in the repo: `ui/` is the shipped terminal (vanilla JS +
htmx + Tailwind, compiled by Vite and embedded into the binary via
`go:embed`), and `web/` is a React/Radix rewrite in progress.

The LLM layer takes any provider: Gemini, Anthropic, OpenAI, or **any
OpenAI-compatible endpoint** (Ollama, vLLM, OpenRouter, Groq, LiteLLM) via
`OPENAI_COMPAT_BASE_URL`. Providers are tried in order, so one being rate
limited doesn't take the feature down.

### MCP integration

Set `MCP_ENABLED=true` to expose an authenticated, read-only MCP endpoint at
`POST /mcp`. Clients authenticate with an existing session cookie or a GSE
Terminal API key. The initial tools provide bounded quote, OHLC history,
market movers, briefing, and Pro/Admin technical-indicator access. Tool calls
use typed schemas, strict limits, existing authorization, and audit logging;
they never accept arbitrary QuestDB SQL.

## Running locally

Requires Go 1.25, Node 20, and Docker.

```bash
cp .env.example .env      # fill in JWT_SECRET at minimum
docker compose up -d      # QuestDB, Postgres, Redis
cd ui && npm ci && npm run build && cd ..
go run ./cmd/server
```

The server listens on `:8080`. First boot creates an `admin` user and
prints its password **once** — save it.

There is no seed data. Either wait for the 16:30 UTC scrape, upload a CSV
from the admin panel, or fetch a date range directly:

```bash
python3 scripts/gse_download.py 2026-01-01 2026-08-21 -o gse.csv
```

That script drives a headless Chrome through the `chrome-agent` CLI, which
must be on `PATH`. The production image bundles both.

## Deployment

`deploy/README.md` is the runbook for a single VM: Caddy with automatic
TLS, the app unpublished behind it, database backups, and systemd units.

```bash
make vm-up      # docker compose with the production overlay
make backup     # pg_dump + consistent QuestDB snapshot
make help       # everything else
```

Kubernetes manifests for GKE are in `k8s/`.

## Configuration

Every variable is documented in `.env.example`. Seven are required
(`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `JWT_SECRET`,
`QUESTDB_URL`, `QUESTDB_ILP_TCP_URL`, `ALLOWED_ORIGINS`); the server exits
at boot listing everything that's missing.

`APP_ENV` is the switch that matters. In `development` the templates and
static assets are re-read from `ui/dist` on every request and session
cookies aren't marked `Secure`, so `http://localhost` works. In
`production` assets come from the embedded snapshot, cookies require HTTPS,
and `JWT_SECRET` must be at least 32 characters.

## Testing

```bash
make test    # go test ./... with the race detector
make lint    # go vet
```
