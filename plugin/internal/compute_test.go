// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package notifier

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	. "github.com/onsi/gomega"
)

// Three cases per design doc §P3 push-model operational contract:
//   1. Happy path — first attempt 200.
//   2. 5xx then 200 — retries succeed.
//   3. 500 all three attempts — error returned, names the component.
//
// Uses a local httptest.Server for the target; the retry timing is
// shortened by overriding the backoff schedule for tests (so the suite
// doesn't take 7+ seconds on the sad path).

func TestPushRegister_HappyPath(t *testing.T) {
	RegisterTestingT(t)

	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		Expect(r.Header.Get("Authorization")).To(HavePrefix("Bearer "))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	proc := NewProcessor(ProcessorConfig{HTTPClient: srv.Client()})
	err := proc.pushRegister(t.Context(), srv.URL, "fake-jwt", TenantRecord{StackName: "test"})
	Expect(err).ToNot(HaveOccurred())
	Expect(atomic.LoadInt32(&hits)).To(Equal(int32(1)))
}

func TestPushRegister_RetrySucceeds(t *testing.T) {
	RegisterTestingT(t)

	// Shrink backoff for tests — overwrite via package-level var.
	saved := backoffSeconds
	backoffSeconds = []int{0, 0, 0}
	defer func() { backoffSeconds = saved }()

	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n < 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	proc := NewProcessor(ProcessorConfig{HTTPClient: srv.Client()})
	err := proc.pushRegister(t.Context(), srv.URL, "fake-jwt", TenantRecord{StackName: "test"})
	Expect(err).ToNot(HaveOccurred())
	Expect(atomic.LoadInt32(&hits)).To(Equal(int32(2)))
}

func TestPushRegister_AllFail(t *testing.T) {
	RegisterTestingT(t)

	saved := backoffSeconds
	backoffSeconds = []int{0, 0, 0}
	defer func() { backoffSeconds = saved }()

	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	proc := NewProcessor(ProcessorConfig{HTTPClient: srv.Client()})
	err := proc.pushRegister(t.Context(), srv.URL, "fake-jwt", TenantRecord{StackName: "test"})
	Expect(err).To(HaveOccurred())
	Expect(err.Error()).To(ContainSubstring("all 3 attempts failed"))
	Expect(atomic.LoadInt32(&hits)).To(Equal(int32(3)))
}

func TestPushRegister_NonRetryable4xx(t *testing.T) {
	RegisterTestingT(t)

	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	proc := NewProcessor(ProcessorConfig{HTTPClient: srv.Client()})
	err := proc.pushRegister(t.Context(), srv.URL, "fake-jwt", TenantRecord{StackName: "test"})
	Expect(err).To(HaveOccurred())
	Expect(err.Error()).To(ContainSubstring("non-retryable http 403"))
	Expect(atomic.LoadInt32(&hits)).To(Equal(int32(1)), "403 must NOT retry (only 4xx-except-429 are non-retryable)")
}

const testSigningKey = "a-fake-signing-key-that-is-at-least-32-bytes-long"

// claimsOf parses a token WITHOUT validating time claims, so an
// expiry-related assertion below reads the value rather than being
// pre-empted by the parser rejecting it.
func claimsOf(t *testing.T, tok string) jwt.MapClaims {
	t.Helper()
	claims := jwt.MapClaims{}
	_, err := jwt.NewParser(jwt.WithValidMethods([]string{"HS256"}), jwt.WithoutClaimsValidation()).
		ParseWithClaims(tok, claims, func(*jwt.Token) (any, error) { return []byte(testSigningKey), nil })
	Expect(err).ToNot(HaveOccurred())
	return claims
}

func TestMintRegisterJWT(t *testing.T) {
	RegisterTestingT(t)

	tok, err := mintRegisterJWT(testSigningKey, "my-publisher-stack", registerJWTTTL)
	Expect(err).ToNot(HaveOccurred())
	// HS256 JWTs are 3 base64url segments separated by dots.
	Expect(strings.Count(tok, ".")).To(Equal(2))

	claims := claimsOf(t, tok)
	Expect(claims["op"]).To(Equal("register"), "the registration token must not be usable to ingest events")
	Expect(claims["tenant"]).To(Equal("my-publisher-stack"))
	Expect(claims).To(HaveKey("exp"), "the registration token is spent within a second and must expire")
}

// The publisher's event token must carry NO expiry. A finite one is the
// date the publisher silently stops being able to alert anyone — see
// mintEventJWT's doc comment for the full reasoning. Asserted here
// because the fault is invisible for as long as the TTL lasts, and a
// well-meaning "add an exp like every other token" edit is the exact
// shape of change that would reintroduce it.
func TestMintEventJWTNeverExpires(t *testing.T) {
	RegisterTestingT(t)

	tok, err := mintEventJWT(testSigningKey, "my-publisher-stack")
	Expect(err).ToNot(HaveOccurred())

	claims := claimsOf(t, tok)
	Expect(claims["op"]).To(Equal("event"), "the publisher's token must not be able to register or drive the drain")
	Expect(claims["tenant"]).To(Equal("my-publisher-stack"),
		"the ingest handler takes the event source from this claim, so it must be the publisher's own stack")
	Expect(claims).ToNot(HaveKey("exp"),
		"an expiry here becomes the date this publisher's alerting goes quiet with nothing to re-mint it")
}
