// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// Tests for the operator login flow and the op-gating it makes possible.
//
// Both halves were missing from the first deploy, and the second half is
// the one that mattered: GET /api/v1/tenants accepted any validly-signed
// token, and every publisher stack's plugin holds one.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	. "github.com/onsi/gomega"

	"github.com/simple-container-com/sc-component-notifier/service/internal/tenant"
)

const testPassword = "correct-horse-battery-staple"

func newSession(password string) *SessionConfig {
	return &SessionConfig{
		SigningKey:       []byte(testKey),
		OperatorPassword: password,
		CookieSecure:     true,
	}
}

func postLogin(t *testing.T, s *SessionConfig, password string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]string{"password": password})
	Expect(err).ToNot(HaveOccurred())
	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	s.Login(rr, req)
	return rr
}

// sessionCookie digs the Set-Cookie value out of a login response, so the
// tests assert on what a browser would actually send back.
func sessionCookie(t *testing.T, rr *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()
	for _, c := range rr.Result().Cookies() {
		if c.Name == SessionCookieName {
			return c
		}
	}
	return nil
}

func TestLogin_RightPasswordSetsAnHTTPOnlySessionCookie(t *testing.T) {
	RegisterTestingT(t)

	s := newSession(testPassword)
	rr := postLogin(t, s, testPassword)

	Expect(rr.Code).To(Equal(http.StatusNoContent))

	c := sessionCookie(t, rr)
	Expect(c).ToNot(BeNil(), "login must set the session cookie")
	Expect(c.Value).ToNot(BeEmpty())

	// HttpOnly is the whole point of moving off localStorage: a token the
	// page's own JavaScript can read is a token any injected script can
	// exfiltrate, and this one is signed with the key that also seals
	// every tenant's channel credentials.
	Expect(c.HttpOnly).To(BeTrue(), "session cookie must be HttpOnly")
	Expect(c.Secure).To(BeTrue(), "session cookie must be Secure outside local debug")
	Expect(c.SameSite).To(Equal(http.SameSiteLaxMode))

	// And the token it carries must actually be an operator token.
	claims := &Claims{}
	tok, err := jwt.ParseWithClaims(c.Value, claims, func(*jwt.Token) (any, error) {
		return []byte(testKey), nil
	}, jwt.WithValidMethods([]string{"HS256"}))
	Expect(err).ToNot(HaveOccurred())
	Expect(tok.Valid).To(BeTrue())
	Expect(claims.Op).To(Equal(OpOperator))
	Expect(claims.Subject).To(Equal("operator"))
	Expect(claims.ExpiresAt).ToNot(BeNil(), "an operator session must expire")
}

func TestLogin_WrongPasswordIs401AndSetsNoCookie(t *testing.T) {
	RegisterTestingT(t)

	s := newSession(testPassword)
	rr := postLogin(t, s, "hunter2")

	Expect(rr.Code).To(Equal(http.StatusUnauthorized))
	Expect(sessionCookie(t, rr)).To(BeNil(), "a failed login must not set a cookie")
}

// The distinction this asserts is the reason GET /api/v1/session exists.
// `operatorPassword` is optional in the operator config, so a deployment
// can legitimately have none — and without a different status an operator
// typing the right password into such a deployment sees "wrong password"
// and goes looking for a typo that isn't there.
func TestLogin_NoPasswordConfiguredIs403NotJust401(t *testing.T) {
	RegisterTestingT(t)

	s := newSession("")
	rr := postLogin(t, s, testPassword)

	Expect(rr.Code).To(Equal(http.StatusForbidden))
	Expect(rr.Body.String()).To(ContainSubstring("NOTIFIER_OPERATOR_PASSWORD"),
		"the 403 must name the env var, or the operator cannot act on it")
	Expect(sessionCookie(t, rr)).To(BeNil())
}

func TestSession_ReportsPasswordSetAndAuthenticated(t *testing.T) {
	RegisterTestingT(t)

	s := newSession(testPassword)

	// map[string]any, not map[string]bool: the authenticated response
	// also carries `version` as a string, and a bool-typed target makes
	// every assertion here fail on the unmarshal rather than on the
	// field under test.
	probe := func(cookie *http.Cookie) map[string]any {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/session", nil)
		if cookie != nil {
			req.AddCookie(cookie)
		}
		rr := httptest.NewRecorder()
		s.Session(rr, req)
		Expect(rr.Code).To(Equal(http.StatusOK), "the probe is unauthenticated and always answers 200")
		var got map[string]any
		Expect(json.Unmarshal(rr.Body.Bytes(), &got)).To(Succeed())
		return got
	}

	anon := probe(nil)
	Expect(anon["password_set"]).To(BeTrue())
	Expect(anon["authenticated"]).To(BeFalse())

	authed := probe(sessionCookie(t, postLogin(t, s, testPassword)))
	Expect(authed["authenticated"]).To(BeTrue())

	unset := newSession("")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/session", nil)
	rr := httptest.NewRecorder()
	unset.Session(rr, req)
	var got map[string]any
	Expect(json.Unmarshal(rr.Body.Bytes(), &got)).To(Succeed())
	Expect(got["password_set"]).To(BeFalse(), "an unset password must be visible to the SPA")
}

// The build stamp the SPA header renders. Unconditional, the same value
// /healthz hands out — the smoke reads it there, because that probe
// needs no credentials at all.
func TestSession_ReportsTheBuildVersion(t *testing.T) {
	RegisterTestingT(t)

	s := newSession(testPassword)
	s.Version = "v2026.11.24-1d7101d"

	probe := func(cookie *http.Cookie) map[string]any {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/session", nil)
		if cookie != nil {
			req.AddCookie(cookie)
		}
		rr := httptest.NewRecorder()
		s.Session(rr, req)
		var got map[string]any
		Expect(json.Unmarshal(rr.Body.Bytes(), &got)).To(Succeed())
		return got
	}

	authed := probe(sessionCookie(t, postLogin(t, s, testPassword)))
	Expect(authed["version"]).To(Equal("v2026.11.24-1d7101d"),
		"an operator must be able to read which build is serving")

	anon := probe(nil)
	Expect(anon["version"]).To(Equal("v2026.11.24-1d7101d"),
		"same value either way — CalVer already contains the short SHA, so there is "+
			"nothing here that /healthz does not also publish")
}

// Healthz is what a post-deploy smoke actually calls: no cookie, no
// bearer, no AWS credentials. Two properties:
//
//   - it carries the build, or a pin bump is unverifiable from outside
//     again (which is the defect this endpoint shape exists to fix);
//   - it still says "ok", because the Lambda/ECS health check and
//     lambda_runtime_test.go both match on that substring and a
//     silently-renamed status field would read as a passing probe.
func TestHealthz_CarriesTheBuildAndStillSaysOK(t *testing.T) {
	RegisterTestingT(t)

	rr := httptest.NewRecorder()
	Healthz("v2026.11.24-1d7101d")(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	Expect(rr.Code).To(Equal(http.StatusOK))
	Expect(rr.Body.String()).To(ContainSubstring("ok"))

	var got map[string]string
	Expect(json.Unmarshal(rr.Body.Bytes(), &got)).To(Succeed())
	Expect(got["status"]).To(Equal("ok"))
	Expect(got["version"]).To(Equal("v2026.11.24-1d7101d"))

	// Deliberately absent: observatory's second field. The full 40-char
	// commit is redundant with the short SHA inside the CalVer and is
	// the only part that discloses something CalVer does not.
	Expect(got).ToNot(HaveKey("commit"))
}

// An `event` token is a valid operator-session-shaped credential in every
// respect except `op`. Answering `authenticated: true` for one would tell
// a publisher's deploy-time credential that it is a logged-in human.
func TestSession_AnEventTokenIsNotAnOperatorSession(t *testing.T) {
	RegisterTestingT(t)

	s := newSession(testPassword)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/session", nil)
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: mintOpJWT(t, OpEvent, "my-backup")})
	rr := httptest.NewRecorder()
	s.Session(rr, req)

	// The response is no longer all-bool (`version` is a string on the
	// authenticated branch), so decode into `any` — otherwise a leak of
	// `version` onto this path would fail with "cannot unmarshal string
	// into bool" rather than naming the field.
	var got map[string]any
	Expect(json.Unmarshal(rr.Body.Bytes(), &got)).To(Succeed())
	Expect(got["authenticated"]).To(BeFalse())
}

func TestLogout_ClearsTheCookieAndIsIdempotent(t *testing.T) {
	RegisterTestingT(t)

	s := newSession(testPassword)
	for i := range 2 {
		rr := httptest.NewRecorder()
		s.Logout(rr, httptest.NewRequest(http.MethodPost, "/api/v1/logout", nil))
		Expect(rr.Code).To(Equal(http.StatusNoContent), "logout %d must succeed", i+1)
		c := sessionCookie(t, rr)
		Expect(c).ToNot(BeNil())
		Expect(c.MaxAge).To(BeNumerically("<", 0), "logout must expire the cookie")
		Expect(c.Value).To(BeEmpty())
	}
}

// THE regression test. GET /api/v1/tenants shipped with no op check, so
// any validly-signed token listed every tenant of the notifier — and
// every publisher stack's plugin holds an `event` token for exactly this
// service. Event tokens are scoped to one `tenant` so a publisher cannot
// reach past itself; an ungated read route handed that back.
func TestTenantsList_OnlyAnOperatorTokenMayEnumerateTenants(t *testing.T) {
	RegisterTestingT(t)

	store := tenant.NewInMemoryStore()
	Expect(store.Upsert(context.Background(), tenant.Record{
		StackName: "my-backup", Version: "v1",
		CreatedAt: time.Now().UTC(), LastSeenAt: time.Now().UTC(),
	})).To(Succeed())

	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/tenants", JWTMiddleware([]byte(testKey))(TenantsList(store)))

	get := func(authz string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants", nil)
		if authz != "" {
			req.Header.Set("Authorization", "Bearer "+authz)
		}
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		return rr
	}

	// A publisher's own token: valid signature, wrong purpose. 403, and
	// the body must not carry any tenant name.
	ev := get(mintOpJWT(t, OpEvent, "my-backup"))
	Expect(ev.Code).To(Equal(http.StatusForbidden))
	Expect(ev.Body.String()).To(ContainSubstring("operator"))
	Expect(ev.Body.String()).ToNot(ContainSubstring("my-backup"),
		"a rejected read must not leak the rows it refused to serve")

	// A consumer's registration token: same story.
	Expect(get(mintOpJWT(t, OpRegister, "my-backup")).Code).To(Equal(http.StatusForbidden))

	// The scheduler's token drives the drain and nothing else.
	Expect(get(mintOpJWT(t, OpScheduler, "")).Code).To(Equal(http.StatusForbidden))

	// No credential at all is still 401, not 403 — the distinction tells
	// the SPA whether to show a login form or an access error.
	Expect(get("").Code).To(Equal(http.StatusUnauthorized))

	// And the operator session the login flow issues does work, through
	// the cookie, which is the transport the SPA uses.
	s := newSession(testPassword)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants", nil)
	req.AddCookie(sessionCookie(t, postLogin(t, s, testPassword)))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	Expect(rr.Code).To(Equal(http.StatusOK))
	Expect(rr.Body.String()).To(ContainSubstring("my-backup"))
}

// The cookie transport is new; before it the header was the only one, and
// a browser cannot set a header on a page load. Asserted separately from
// the authorization tests so a regression here is not mistaken for an
// op-gating failure.
func TestJWTMiddleware_AcceptsTheSessionCookieAndPrefersTheHeader(t *testing.T) {
	RegisterTestingT(t)

	var seen string
	h := JWTMiddleware([]byte(testKey))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = ClaimsFromContext(r.Context()).Op
		w.WriteHeader(http.StatusOK)
	}))

	// Cookie only.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: mintOpJWT(t, OpOperator, "")})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	Expect(rr.Code).To(Equal(http.StatusOK))
	Expect(seen).To(Equal(OpOperator))

	// Both: the header wins, so a curl can override a browser session.
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: mintOpJWT(t, OpOperator, "")})
	req.Header.Set("Authorization", "Bearer "+mintOpJWT(t, OpScheduler, ""))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	Expect(rr.Code).To(Equal(http.StatusOK))
	Expect(seen).To(Equal(OpScheduler), "the Bearer header must take precedence over the cookie")

	// An empty cookie is no credential, not a malformed one.
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: ""})
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	Expect(rr.Code).To(Equal(http.StatusUnauthorized))
	Expect(strings.ToLower(rr.Body.String())).To(ContainSubstring("missing"))
}
