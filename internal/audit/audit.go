// Package audit provides a thin convenience wrapper around the audit_log
// table so handlers can record administrative actions in one line. The
// underlying inserts go through PostgresRepo.InsertAuditEntry; this package
// only adds JSON metadata serialisation, structured logging, and a
// fire-and-log error handler so callers don't have to think about the audit
// path failing.
package audit

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/teckdroids/ges-data-engine/internal/auth"
	"github.com/teckdroids/ges-data-engine/internal/repository"
)

// Logger writes audit entries to the database. Construct one in main.go and
// share it across handlers.
type Logger struct {
	pg *repository.PostgresRepo
}

func New(pg *repository.PostgresRepo) *Logger {
	return &Logger{pg: pg}
}

// Action constants are kept here so the spelling is centralised. Add new
// values as new admin operations are introduced.
const (
	ActionUserCreate    = "user.create"
	ActionUserDelete    = "user.delete"
	ActionUserStatus    = "user.status"
	ActionUserRoleSet   = "user.role"
	ActionUserPwdReset  = "user.password_reset"
	ActionDataUpload    = "data.upload"
	ActionDataScrape    = "data.scrape"
	ActionDataScrapeErr = "data.scrape.failure"
	ActionAPIKeyCreate  = "api_key.create"
	ActionAPIKeyRevoke  = "api_key.revoke"
	ActionLoginSuccess  = "auth.login.success"
	ActionLoginFailure  = "auth.login.failure"
	ActionAlertCreate   = "alert.create"
	ActionAlertDelete   = "alert.delete"
	ActionAlertFire     = "alert.fire"
	ActionAlertToggle   = "alert.toggle"
	ActionProRequestNew = "pro_request.create"
	ActionProRequestOK  = "pro_request.approve"
	ActionProRequestNo  = "pro_request.deny"
	// ActionMCPToolCall records a sanitized, read-only MCP tool invocation.
	ActionMCPToolCall = "mcp.tool.call"
)

// Log records one audit entry. metadata may be nil.
func (l *Logger) Log(ctx context.Context, action, targetType, targetID string, metadata map[string]interface{}) {
	if l == nil || l.pg == nil {
		return
	}
	user := auth.FromContext(ctx)
	var actorID *int
	if !user.IsGuest() {
		id := user.ID
		actorID = &id
	}
	// Always stamp the chi request_id (when present) into the audit metadata
	// so a row in the audit_log table can be cross-referenced against
	// structured logs by request_id in one query.
	if reqID := middleware.GetReqID(ctx); reqID != "" {
		if metadata == nil {
			metadata = map[string]interface{}{}
		}
		if _, exists := metadata["request_id"]; !exists {
			metadata["request_id"] = reqID
		}
	}
	var metaJSON string
	if metadata != nil {
		if b, err := json.Marshal(metadata); err == nil {
			metaJSON = string(b)
		}
	}
	if err := l.pg.InsertAuditEntry(ctx, actorID, user.Username, action, targetType, targetID, metaJSON); err != nil {
		slog.Warn("audit log write failed", "action", action, "target", targetID, "error", err)
	}
}
