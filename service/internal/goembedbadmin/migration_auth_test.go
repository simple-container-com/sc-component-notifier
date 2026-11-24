// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package goembedbadmin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The migration endpoint is the service's widest write path: it reads
// every key out of S3 and upserts it into the live store, and it is
// deliberately NOT behind the service JWT. Its whole access control is
// the three checks at the top of Migrate, so those get a test of their
// own rather than relying on the migration body's coverage.
//
// `AdminToken` here is always PLAINTEXT. It arrives at the service as a
// SecretsManager ARN (bound from the component's `migrationToken:` via
// the plugin's ComponentSecretEnvMap) and `main` resolves it before
// constructing this struct — see secret_resolver.go. An ARN reaching
// this field would make every operator call 401 with nothing in the log
// to say why, which is exactly the failure the plugin-side
// TestSecretEnvVarsAreResolvedByTheService guards.

func postMigrate(t *testing.T, c *MigrationConfig, header string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/admin/migrate-s3-to-goembedb", nil)
	if header != "" {
		req.Header.Set("X-Admin-Token", header)
	}
	rec := httptest.NewRecorder()
	c.Migrate(rec, req)
	return rec
}

// An operator who never opted in must have no migration surface at all.
// 404 and not 401: a 401 would advertise that the route exists.
func TestMigrate_NoTokenConfigured_Is404(t *testing.T) {
	rec := postMigrate(t, &MigrationConfig{}, "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("want 404 when AdminToken is empty, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "ADMIN_MIGRATION_TOKEN") {
		t.Errorf("the 404 body should name the env var an operator has to set, got: %s", rec.Body.String())
	}
}

func TestMigrate_WrongOrMissingToken_Is401(t *testing.T) {
	cfg := &MigrationConfig{AdminToken: "the-real-token"}

	for name, sent := range map[string]string{
		"absent":        "",
		"wrong":         "not-the-token",
		"prefix":        "the-real-toke",
		"with-suffix":   "the-real-tokenX",
		"case-mismatch": "THE-REAL-TOKEN",
	} {
		t.Run(name, func(t *testing.T) {
			rec := postMigrate(t, cfg, sent)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("want 401 for a %s token, got %d: %s", name, rec.Code, rec.Body.String())
			}
		})
	}
}

// The matching token must get PAST auth. Asserting the next failure (a
// 500 naming the unconfigured source) rather than a 200 keeps the test
// free of both stores while still proving the compare accepted — a test
// that only checked "not 401" would pass against a handler that 404s.
func TestMigrate_MatchingToken_PassesAuth(t *testing.T) {
	cfg := &MigrationConfig{AdminToken: "the-real-token"}

	rec := postMigrate(t, cfg, "the-real-token")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("a matching token should reach the source check (500), got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "source") {
		t.Errorf("want the 500 to name the missing source, got: %s", rec.Body.String())
	}
}
