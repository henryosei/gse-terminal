package config

import (
	"testing"
)

// TRUST_PROXY decides whether client-supplied forwarding headers are
// believed, so an unrecognised value must not silently read as true.
func TestGetEnvBool(t *testing.T) {
	cases := []struct {
		val  string
		def  bool
		want bool
	}{
		{"true", false, true}, {"TRUE", false, true}, {"1", false, true},
		{"yes", false, true}, {"on", false, true},
		{"false", true, false}, {"0", true, false}, {"no", true, false},
		{" true ", false, true},
		{"", false, false}, {"", true, true},
		{"maybe", false, false}, {"maybe", true, true},
	}
	for _, tc := range cases {
		t.Setenv("TEST_BOOL", tc.val)
		if got := getEnvBool("TEST_BOOL", tc.def); got != tc.want {
			t.Errorf("getEnvBool(%q, def=%v) = %v, want %v", tc.val, tc.def, got, tc.want)
		}
	}
}

func TestLoadDeploymentFlags(t *testing.T) {
	for k, v := range map[string]string{
		"POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p", "POSTGRES_DB": "d",
		"JWT_SECRET": "s", "QUESTDB_URL": "q", "QUESTDB_ILP_TCP_URL": "i",
		"ALLOWED_ORIGINS": "https://example.com",
		"TRUST_PROXY":     "true", "METRICS_TOKEN": "tok", "MCP_ENABLED": "true",
	} {
		t.Setenv(k, v)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.TrustProxy {
		t.Error("TRUST_PROXY=true not applied")
	}
	if cfg.MetricsToken != "tok" {
		t.Errorf("MetricsToken = %q", cfg.MetricsToken)
	}
	if !cfg.MCPEnabled {
		t.Error("MCP_ENABLED=true not applied")
	}
}

func TestLoadMCPDisabledByDefault(t *testing.T) {
	for k, v := range map[string]string{
		"POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p", "POSTGRES_DB": "d",
		"JWT_SECRET": "s", "QUESTDB_URL": "q", "QUESTDB_ILP_TCP_URL": "i",
		"ALLOWED_ORIGINS": "https://example.com", "MCP_ENABLED": "",
	} {
		t.Setenv(k, v)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MCPEnabled {
		t.Error("MCP should be disabled when MCP_ENABLED is unset")
	}
}

// The DSN must honour POSTGRES_PORT — compose publishes 5433 on the host.
func TestPostgresConnStringPort(t *testing.T) {
	c := &Config{PostgresUser: "u", PostgresPassword: "p", PostgresDB: "d", PostgresPort: "5433"}
	if got, want := c.PostgresConnString(), "postgres://u:p@localhost:5433/d"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
	c.PostgresPort = ""
	if got, want := c.PostgresConnString(), "postgres://u:p@localhost:5432/d"; got != want {
		t.Errorf("default port: got %q want %q", got, want)
	}
}
