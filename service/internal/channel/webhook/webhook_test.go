// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/simple-container-com/sc-component-notifier/service/internal/channel"
)

var fixedNow = time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

func testMessage() channel.Message {
	return channel.Message{
		ID:       "backup.failed:forge-conductor:run-77",
		Title:    "Backup failed",
		Body:     "mongodump exited 1",
		Severity: channel.SeverityError,
		Source:   "my-backup",
		Kind:     "backup.failed",
		Subject:  "forge-conductor",
		URL:      "https://backup.simple-forge.com/runs/77",
		Labels:   map[string]string{"region": "eu-west-1"},
		At:       fixedNow,
	}
}

type capture struct {
	headers http.Header
	body    []byte
	count   int
}

func newServer(t *testing.T, status int, respBody string) (string, *capture) {
	t.Helper()
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		cap.headers = r.Header.Clone()
		cap.body = b
		cap.count++
		w.WriteHeader(status)
		_, _ = w.Write([]byte(respBody))
	}))
	t.Cleanup(srv.Close)
	return srv.URL, cap
}

// TestSignatureVerifiesAgainstReceiverSide is the contract: a receiver
// recomputing the HMAC from the headers and the raw body must get the
// same value. If the separator or the field order drifts, every receiver
// in the world rejects every message — and the notifier looks fine from
// the inside.
func TestSignatureVerifiesAgainstReceiverSide(t *testing.T) {
	url, cap := newServer(t, 200, "")
	ch := New(Config{
		Name: "ims", URL: url, Secret: "shhh",
		Now: func() time.Time { return fixedNow },
	})

	if err := ch.Send(context.Background(), testMessage()); err != nil {
		t.Fatalf("Send: %v", err)
	}

	ts := cap.headers.Get(TimestampHeader)
	sig := cap.headers.Get(SignatureHeader)
	if ts == "" || sig == "" {
		t.Fatalf("missing headers: ts=%q sig=%q", ts, sig)
	}
	if want := Sign("shhh", ts, cap.body); sig != want {
		t.Errorf("signature mismatch\n got %s\nwant %s", sig, want)
	}
	// The wrong secret must NOT verify — otherwise the test above would
	// pass for any implementation at all.
	if Sign("wrong", ts, cap.body) == sig {
		t.Error("the signature verifies under a different secret")
	}
	// Nor a tampered body.
	if Sign("shhh", ts, append(cap.body, ' ')) == sig {
		t.Error("the signature verifies over a modified body")
	}
}

// TestSignatureCoversTimestamp: signing the body alone would let an
// intercepted request be replayed forever. The timestamp must be inside
// the MAC, not merely alongside it.
func TestSignatureCoversTimestamp(t *testing.T) {
	body := []byte(`{"a":1}`)
	a := Sign("k", "1700000000", body)
	b := Sign("k", "1700000001", body)
	if a == b {
		t.Error("the signature is identical for two timestamps — a replayed body stays valid forever")
	}
}

// TestTimestampIsFresh: a receiver rejects a stale timestamp, so the
// header must carry send time rather than, say, the event time (which
// for a retried delivery can be hours old).
func TestTimestampIsFresh(t *testing.T) {
	url, cap := newServer(t, 200, "")
	// Event time well in the past; send time is now.
	sendTime := time.Date(2026, 11, 23, 18, 30, 0, 0, time.UTC)
	ch := New(Config{
		Name: "ims", URL: url, Secret: "shhh",
		Now: func() time.Time { return sendTime },
	})

	m := testMessage()
	m.At = fixedNow // 6.5h earlier

	if err := ch.Send(context.Background(), m); err != nil {
		t.Fatalf("Send: %v", err)
	}
	got, err := strconv.ParseInt(cap.headers.Get(TimestampHeader), 10, 64)
	if err != nil {
		t.Fatalf("timestamp header not an integer: %v", err)
	}
	if got != sendTime.Unix() {
		t.Errorf("timestamp = %d (%s), want the SEND time %d — a receiver's freshness window would reject this",
			got, time.Unix(got, 0).UTC(), sendTime.Unix())
	}
}

// TestNoSecretSendsNoSignature: a signature computed over an empty key
// would look authenticated while proving nothing. Absent is honest.
func TestNoSecretSendsNoSignature(t *testing.T) {
	url, cap := newServer(t, 200, "")
	ch := New(Config{Name: "local", URL: url}) // no secret

	if err := ch.Send(context.Background(), testMessage()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := cap.headers.Get(SignatureHeader); got != "" {
		t.Errorf("%s = %q with no secret configured; an empty-key MAC looks authenticated but proves nothing",
			SignatureHeader, got)
	}
	if got := cap.headers.Get(TimestampHeader); got != "" {
		t.Errorf("%s = %q with no signature to anchor", TimestampHeader, got)
	}
}

// TestPayloadContract pins the wire format. Receivers — including the
// future sc-component-ims — are written against these names, so a
// rename is a breaking change and should fail here first.
func TestPayloadContract(t *testing.T) {
	url, cap := newServer(t, 200, "")
	ch := New(Config{Name: "ims", URL: url, Now: func() time.Time { return fixedNow }})

	if err := ch.Send(context.Background(), testMessage()); err != nil {
		t.Fatalf("Send: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(cap.body, &got); err != nil {
		t.Fatalf("body is not JSON: %v\n%s", err, cap.body)
	}
	for k, want := range map[string]any{
		"version":  float64(PayloadVersion),
		"id":       "backup.failed:forge-conductor:run-77",
		"source":   "my-backup",
		"kind":     "backup.failed",
		"severity": "error",
		"subject":  "forge-conductor",
		"title":    "Backup failed",
		"body":     "mongodump exited 1",
	} {
		if got[k] != want {
			t.Errorf("payload[%q] = %#v, want %#v", k, got[k], want)
		}
	}
	if _, ok := got["at"]; !ok {
		t.Error("payload has no `at` — a receiver cannot order events")
	}
	if cap.headers.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q", cap.headers.Get("Content-Type"))
	}
}

// TestEventIDHeaderAllowsDedupeWithoutParsing: a receiver should be able
// to drop a replay before unmarshalling anything.
func TestEventIDHeaderAllowsDedupeWithoutParsing(t *testing.T) {
	url, cap := newServer(t, 200, "")
	ch := New(Config{Name: "ims", URL: url})

	if err := ch.Send(context.Background(), testMessage()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := cap.headers.Get(EventIDHeader); got != "backup.failed:forge-conductor:run-77" {
		t.Errorf("%s = %q", EventIDHeader, got)
	}
}

// TestSendUsesBrowserUserAgent — same Cloudflare 1010 scar as Telegram.
// A webhook pointed at anything behind Cloudflare (which is most things)
// gets 403'd on the default Go UA.
func TestSendUsesBrowserUserAgent(t *testing.T) {
	url, cap := newServer(t, 200, "")
	ch := New(Config{Name: "ims", URL: url})

	if err := ch.Send(context.Background(), testMessage()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	ua := cap.headers.Get("User-Agent")
	if ua != channel.UserAgent {
		t.Errorf("User-Agent = %q, want %q", ua, channel.UserAgent)
	}
	if strings.Contains(ua, "Go-http-client") {
		t.Error("the default Go User-Agent is 403'd by Cloudflare with error 1010")
	}
}

// TestPermanentVsRetryableStatus: a 404 webhook URL must not be retried
// five times; a 429 or 5xx must be.
func TestPermanentVsRetryableStatus(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		permanent bool
	}{
		{"bad request", 400, true},
		{"unauthorized", 401, true},
		{"not found", 404, true},
		{"gone", 410, true},
		{"rate limited", 429, false},
		{"server error", 500, false},
		{"bad gateway", 502, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url, _ := newServer(t, tc.status, "nope")
			ch := New(Config{Name: "ims", URL: url})
			err := ch.Send(context.Background(), testMessage())
			if err == nil {
				t.Fatalf("status %d returned nil", tc.status)
			}
			if got := channel.IsPermanent(err); got != tc.permanent {
				t.Errorf("status %d: IsPermanent=%v, want %v", tc.status, got, tc.permanent)
			}
			if !strings.Contains(err.Error(), "ims") {
				t.Errorf("error does not name the channel: %v", err)
			}
		})
	}
}

// TestMissingURLIsPermanent: never "degrade gracefully" on a mint/send
// path — that is how a notifier ends up quietly not notifying.
func TestMissingURLIsPermanent(t *testing.T) {
	ch := New(Config{Name: "broken"})
	err := ch.Send(context.Background(), testMessage())
	if err == nil {
		t.Fatal("a channel with no URL reported success")
	}
	if !channel.IsPermanent(err) {
		t.Errorf("a missing URL is a config error, not a blip: %v", err)
	}
	if ch.Health(context.Background()) == nil {
		t.Error("Health() passed with no URL — the SPA would show it as fine")
	}
}

// TestHealthDoesNotProbeTheEndpoint: a probe POST would deliver a bogus
// notification to whatever is on the other end, and there is no safe GET
// for a write-only URL. Health must report config validity and claim
// nothing more.
func TestHealthDoesNotProbeTheEndpoint(t *testing.T) {
	url, cap := newServer(t, 500, "should never be called")
	ch := New(Config{Name: "ims", URL: url, Secret: "s"})

	if err := ch.Health(context.Background()); err != nil {
		t.Fatalf("Health on a configured channel: %v", err)
	}
	if cap.count != 0 {
		t.Errorf("Health made %d request(s) to the webhook — it would deliver a bogus notification", cap.count)
	}
}

// TestSendTransportErrorIsRetryable keeps the cold-start case (the one a
// 3s timeout once lost real deliveries on) out of the give-up path.
func TestSendTransportErrorIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing listening

	ch := New(Config{Name: "ims", URL: url})
	err := ch.Send(context.Background(), testMessage())
	if err == nil {
		t.Fatal("expected a transport error")
	}
	if channel.IsPermanent(err) {
		t.Errorf("a transport failure must stay retryable: %v", err)
	}
}

// TestSendTimeoutIsGenerous: the 3s → 15s fix. A Lambda→Lambda POST into
// a cold receiver routinely takes longer than a few seconds, and the
// short timeout lost real deliveries.
func TestSendTimeoutIsGenerous(t *testing.T) {
	if channel.SendTimeout < 10*time.Second {
		t.Errorf("SendTimeout = %s; a cold-start receiver needs ~15s (a 3s timeout lost real deliveries)",
			channel.SendTimeout)
	}
}
