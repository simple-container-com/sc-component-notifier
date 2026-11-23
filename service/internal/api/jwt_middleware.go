// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// claimsKey is the context key the auth middleware uses to propagate the
// JWT claims to downstream handlers. Private to this package so no caller
// can fabricate claims.
type claimsKey struct{}

// Claims is the shape the plugin's mintJWT (in plugin/internal/compute.go)
// produces, and the operator login flow too. Verified against the HS256
// key injected as SERVICE_JWT_SECRET.
type Claims struct {
	// Op is the token's purpose, and it is checked PER ROUTE rather than
	// only for validity. See the Op* constants.
	Op string `json:"op"`

	// Tenant scopes an event token to exactly one publishing stack.
	Tenant string `json:"tenant"`

	jwt.RegisteredClaims
}

// Token purposes. Constants rather than literals because they are
// compared across two modules — the service checks them, the plugin
// mints them — and a typo on either side surfaces as a bare 401 with
// nothing naming the mismatch.
const (
	OpRegister  = "register"
	OpEvent     = "event"
	OpScheduler = "scheduler"
	OpOperator  = "operator"
)

// SessionCookieName is the operator session cookie.
//
// Namespaced to this component: the notifier is a reusable SC Component,
// so a deployment sharing an origin with another sc-component-* service
// must not collide with its cookie.
const SessionCookieName = "sc_notifier_session"

// tokenFromRequest prefers the Bearer header, so a curl against a
// browser-authed deployment can still override the cookie.
//
// Cookie support is not a convenience: it is what lets the SPA
// authenticate at all. Before it existed the only transport was the
// header, so the embedded dashboard had no way to hold a credential
// except `localStorage` — which is why the first deploy's SPA asked the
// operator to paste in a hand-minted JWT.
func tokenFromRequest(r *http.Request) string {
	if authz := r.Header.Get("Authorization"); strings.HasPrefix(authz, "Bearer ") {
		return strings.TrimPrefix(authz, "Bearer ")
	}
	if c, err := r.Cookie(SessionCookieName); err == nil && c.Value != "" {
		return c.Value
	}
	return ""
}

// JWTMiddleware returns a middleware that verifies the HS256 JWT from
// either the `Authorization: Bearer` header or the session cookie.
//
// Rejections (401):
//   - No credential in either transport
//   - Unsigned or wrong-alg token
//   - Signature doesn't verify
//   - Expired (exp < now) or not-yet-valid (nbf > now)
//
// On success, Claims are placed in the request context under claimsKey.
// Validity is NOT authorization: the route still has to check `Op`.
func JWTMiddleware(signingKey []byte) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw := tokenFromRequest(r)
			if raw == "" {
				http.Error(w, "missing or malformed credentials", http.StatusUnauthorized)
				return
			}

			claims := &Claims{}
			tok, err := jwt.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
				if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, jwt.ErrSignatureInvalid
				}
				return signingKey, nil
			}, jwt.WithValidMethods([]string{"HS256"}))
			if err != nil || !tok.Valid {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}

			r = r.WithContext(context.WithValue(r.Context(), claimsKey{}, claims))
			next.ServeHTTP(w, r)
		})
	}
}

// ClaimsFromContext pulls the verified claims out of the request context.
// Returns nil if the middleware didn't run (shouldn't happen on gated
// routes, but guard defensively).
func ClaimsFromContext(ctx context.Context) *Claims {
	if v, ok := ctx.Value(claimsKey{}).(*Claims); ok {
		return v
	}
	return nil
}

// requireOperator gates the dashboard's read routes on a human session.
//
// Validity alone is not enough, and this is not theoretical: EVERY
// publisher stack's plugin holds a valid `event` token for this service,
// and every consumer's holds a `register` one. A read route that
// accepted any valid token would let one tenant enumerate every other
// tenant of the same notifier. Event tokens are scoped to a single
// `tenant` precisely so a publisher cannot reach beyond itself; letting
// the same token read the whole fleet hands back what that scoping took
// away.
//
// Live-caught 2026-11-23: GET /api/v1/tenants shipped with no op check
// and a comment saying "a signed token is enough for the notifier".
func requireOperator(w http.ResponseWriter, r *http.Request) bool {
	claims := ClaimsFromContext(r.Context())
	if claims == nil || claims.Op != OpOperator {
		writeErr(w, http.StatusForbidden, "token op must be 'operator'")
		return false
	}
	return true
}

// writeErr renders an error as the same JSON envelope every other
// handler uses, so the SPA can read one shape.
func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
