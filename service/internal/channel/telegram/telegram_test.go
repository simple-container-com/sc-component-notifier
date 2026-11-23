// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package telegram

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/simple-container-com/sc-component-notifier/service/internal/channel"
)

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
		Labels:   map[string]string{"region": "eu-west-1", "attempt": "3"},
		At:       time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC),
	}
}

// newServer stands in for api.telegram.org and records what arrived.
func newServer(t *testing.T, status int, body string) (*httptest.Server, *[]*http.Request, *[]string) {
	t.Helper()
	var reqs []*http.Request
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		reqs = append(reqs, r)
		bodies = append(bodies, string(b))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	prev := apiBase
	apiBase = srv.URL
	t.Cleanup(func() { apiBase = prev; srv.Close() })
	return srv, &reqs, &bodies
}

// TestSendUsesBrowserUserAgent is the Cloudflare scar. Cloudflare 403s
// the default Go UA with error 1010, and a notifier whose sends are
// blocked at the edge is silently broken in exactly the case it exists
// to cover.
func TestSendUsesBrowserUserAgent(t *testing.T) {
	_, reqs, _ := newServer(t, 200, `{"ok":true}`)
	ch := New(Config{Name: "ops", Token: "tok", ChatID: "-100"})

	if err := ch.Send(context.Background(), testMessage()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(*reqs) != 1 {
		t.Fatalf("got %d requests, want 1", len(*reqs))
	}
	ua := (*reqs)[0].Header.Get("User-Agent")
	if ua != channel.UserAgent {
		t.Errorf("User-Agent = %q, want the browser token %q", ua, channel.UserAgent)
	}
	if strings.Contains(ua, "Go-http-client") || ua == "" {
		t.Error("the default Go User-Agent is 403'd by Cloudflare with error 1010")
	}
}

// TestSendPostsExpectedPayload: parse_mode must match what Render emits,
// or every message arrives with visible markup.
func TestSendPostsExpectedPayload(t *testing.T) {
	_, reqs, bodies := newServer(t, 200, `{"ok":true}`)
	ch := New(Config{Name: "ops", Token: "s3cret", ChatID: "-1001234"})

	if err := ch.Send(context.Background(), testMessage()); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if got := (*reqs)[0].URL.Path; got != "/bots3cret/sendMessage" {
		t.Errorf("path = %q, want /bot<token>/sendMessage", got)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte((*bodies)[0]), &payload); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	if payload["chat_id"] != "-1001234" {
		t.Errorf("chat_id = %v", payload["chat_id"])
	}
	if payload["parse_mode"] != "HTML" {
		t.Errorf("parse_mode = %v, want HTML — Render emits HTML tags", payload["parse_mode"])
	}
	if payload["disable_web_page_preview"] != true {
		t.Error("link previews should be off; an alert should not unfurl a dashboard screenshot")
	}
	if text, _ := payload["text"].(string); !strings.Contains(text, "Backup failed") {
		t.Errorf("text does not contain the title: %q", text)
	}
}

// TestPermanentVsRetryableStatus: a 400 (wrong chat id) must not be
// retried five times, and a 429/5xx must be.
func TestPermanentVsRetryableStatus(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		permanent bool
	}{
		{"bad request", 400, `{"ok":false,"description":"Bad Request: chat not found"}`, true},
		{"forbidden", 403, `{"ok":false,"description":"Forbidden: bot was blocked by the user"}`, true},
		{"rate limited", 429, `{"ok":false,"description":"Too Many Requests: retry after 30"}`, false},
		{"bad gateway", 502, `bad gateway`, false},
		{"gateway timeout", 504, ``, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			newServer(t, tc.status, tc.body)
			ch := New(Config{Name: "ops", Token: "tok", ChatID: "-100"})
			err := ch.Send(context.Background(), testMessage())
			if err == nil {
				t.Fatalf("status %d returned a nil error", tc.status)
			}
			if got := channel.IsPermanent(err); got != tc.permanent {
				t.Errorf("status %d: IsPermanent=%v, want %v (err: %v)", tc.status, got, tc.permanent, err)
			}
		})
	}
}

// TestSendSurfacesTelegramDescription: "400 Bad Request" alone sends the
// operator to the wrong place — it is nearly always a wrong chat id or a
// bot that was never added to the chat, and only `description` says so.
func TestSendSurfacesTelegramDescription(t *testing.T) {
	newServer(t, 400, `{"ok":false,"error_code":400,"description":"Bad Request: chat not found"}`)
	ch := New(Config{Name: "ops", Token: "tok", ChatID: "-100"})

	err := ch.Send(context.Background(), testMessage())
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "chat not found") {
		t.Errorf("error does not surface Telegram's description: %v", err)
	}
	if !strings.Contains(err.Error(), "ops") {
		t.Errorf("error does not name the channel, so the operator cannot tell which one broke: %v", err)
	}
	// The description must be EXTRACTED, not reached by dumping the raw
	// body. Without this, describeError could do nothing at all and the
	// assertion above would still pass off the fallback — the raw JSON
	// contains the same substring. (Caught by mutation C10.)
	for _, leak := range []string{`"ok"`, "error_code", "{"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("error contains raw JSON (%q), so the description was not parsed out: %v", leak, err)
		}
	}
}

// TestDescribeErrorHandlesWholeBody guards the 4KB-cap regression:
// forge-notifier read a capped prefix of the response and then parsed
// the truncated JSON, producing a false ok=false on responses that had
// actually succeeded.
func TestDescribeErrorHandlesWholeBody(t *testing.T) {
	// A body larger than any sane read cap, with the useful field last.
	padding := strings.Repeat("x", 8*1024)
	body := `{"ok":false,"junk":"` + padding + `","description":"Bad Request: chat not found"}`
	newServer(t, 400, body)

	ch := New(Config{Name: "ops", Token: "tok", ChatID: "-100"})
	err := ch.Send(context.Background(), testMessage())
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "chat not found") {
		t.Errorf("a >4KB response body lost the description — this is the false ok=false bug: %v", err)
	}
}

// TestMissingCredentialsArePermanent: an unconfigured channel must fail
// loudly and immediately, never "degrade gracefully".
func TestMissingCredentialsArePermanent(t *testing.T) {
	// apiBase is pointed at a dead address, for two reasons. The test
	// must make no real network call — a unit test that reaches
	// api.telegram.org is slow, flaky offline, and sends traffic nobody
	// asked for. And the dead address is what makes the assertion
	// meaningful: if the missing-credential check were ever removed, the
	// send would attempt a transport call and fail RETRYABLY, so the
	// IsPermanent assertion below actually fails. Against the real API
	// it would 404 — which is permanent — and the test would pass for
	// entirely the wrong reason. (Caught by mutation C9.)
	deadBase(t)

	for _, tc := range []struct{ name, token, chat string }{
		{"no token", "", "-100"},
		{"no chat", "tok", ""},
		{"neither", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ch := New(Config{Name: "ops", Token: tc.token, ChatID: tc.chat})
			err := ch.Send(context.Background(), testMessage())
			if err == nil {
				t.Fatal("an unconfigured channel reported success")
			}
			if !channel.IsPermanent(err) {
				t.Errorf("missing config should be permanent, not retried 5x: %v", err)
			}
			// The error must say WHICH thing is missing; "unauthorized"
			// would send the operator rotating a token that is fine.
			if !strings.Contains(err.Error(), "token") && !strings.Contains(err.Error(), "chat id") {
				t.Errorf("error names neither the missing token nor the missing chat id: %v", err)
			}
			if herr := ch.Health(context.Background()); herr == nil {
				t.Error("Health() passed on an unconfigured channel — the SPA would show it as fine")
			}
		})
	}
}

// deadBase points apiBase at a closed port for the duration of a test,
// so any attempted request fails as a transport error rather than
// escaping to the real Bot API.
func deadBase(t *testing.T) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()
	prev := apiBase
	apiBase = url
	t.Cleanup(func() { apiBase = prev })
}

// TestHealthUsesGetMeNotSendMessage: a health check that messages the
// chat would spam the operator every time the dashboard refreshed.
func TestHealthUsesGetMeNotSendMessage(t *testing.T) {
	_, reqs, _ := newServer(t, 200, `{"ok":true,"result":{"username":"forge_bot"}}`)
	ch := New(Config{Name: "ops", Token: "tok", ChatID: "-100"})

	if err := ch.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}
	path := (*reqs)[0].URL.Path
	if !strings.HasSuffix(path, "/getMe") {
		t.Errorf("Health called %q; it must use getMe so it never messages the chat", path)
	}
	if strings.Contains(path, "sendMessage") {
		t.Error("Health sent a message to the chat")
	}
}

// TestRenderEscapesHTML: parse_mode is HTML, so an error body containing
// a '<' would otherwise be a 400 from the API — i.e. the alert with the
// most interesting content is the one that fails to send.
func TestRenderEscapesHTML(t *testing.T) {
	m := testMessage()
	m.Body = `cannot parse <config> & "quoted" > value`
	out := Render(m)

	if strings.Contains(out, "<config>") {
		t.Error("raw <config> left unescaped — Telegram rejects it as an unknown tag")
	}
	for _, want := range []string{"&lt;config&gt;", "&amp;"} {
		if !strings.Contains(out, want) {
			t.Errorf("escaped form %q missing from:\n%s", want, out)
		}
	}
	// The deliberate markup must survive.
	if !strings.Contains(out, "<b>") {
		t.Error("the title's own <b> markup was escaped away")
	}
}

// TestRenderLabelsAreSorted: an unstable map order would make two
// deliveries of one event look like two different messages, which
// defeats eyeballing a channel for duplicates.
func TestRenderLabelsAreSorted(t *testing.T) {
	m := testMessage()
	m.Labels = map[string]string{"zulu": "1", "alpha": "2", "mike": "3"}

	first := Render(m)
	for i := 0; i < 20; i++ {
		if Render(m) != first {
			t.Fatal("Render is not deterministic across map iterations")
		}
	}
	ai, mi, zi := strings.Index(first, "alpha"), strings.Index(first, "mike"), strings.Index(first, "zulu")
	if !(ai < mi && mi < zi) {
		t.Errorf("labels not in sorted order (alpha=%d mike=%d zulu=%d)", ai, mi, zi)
	}
}

// TestRenderTruncatesOnRuneBoundary is the one that matters most here: a
// half-written UTF-8 sequence is a 400 from the API, so a too-long alert
// becomes NO alert. Cyrillic is not hypothetical — this fleet's own
// operator-facing copy is bilingual.
func TestRenderTruncatesOnRuneBoundary(t *testing.T) {
	m := testMessage()
	// Multi-byte runes, so a naive byte cut lands mid-sequence.
	m.Body = strings.Repeat("резервное копирование ", 500)

	out := Render(m)

	if len(out) > maxMessageBytes {
		t.Fatalf("rendered %d bytes, over Telegram's %d cap", len(out), maxMessageBytes)
	}
	if !utf8.ValidString(out) {
		t.Fatal("truncation produced invalid UTF-8 — Telegram answers 400 and the alert is lost")
	}
	if !strings.Contains(out, "(truncated)") {
		t.Error("truncation is not marked, so a silently-cut alert reads as complete")
	}
	// The head of the message — severity, title, subject — must survive;
	// that is the part the operator reads first.
	if !strings.Contains(out, "Backup failed") {
		t.Error("the title was truncated away")
	}
}

// TestRenderShortMessageIsNotTruncated: the guard must not fire on
// ordinary traffic.
func TestRenderShortMessageIsNotTruncated(t *testing.T) {
	out := Render(testMessage())
	if strings.Contains(out, "(truncated)") {
		t.Errorf("a short message was marked truncated:\n%s", out)
	}
	if !utf8.ValidString(out) {
		t.Error("invalid UTF-8")
	}
}

// TestRenderUnknownSeverityStillRenders: the delivery gate decides what
// gets sent; a renderer that dropped an unknown severity would hide a
// publisher bug instead of showing it.
func TestRenderUnknownSeverityStillRenders(t *testing.T) {
	m := testMessage()
	m.Severity = "catastrophic"
	out := Render(m)
	if !strings.Contains(out, "Backup failed") {
		t.Error("an unknown severity dropped the message body at render time")
	}
}

// TestSendTransportErrorIsRetryable: a timeout or reset is exactly the
// cold-start case a 3s timeout once lost real deliveries on, and it must
// stay retryable.
func TestSendTransportErrorIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	prev := apiBase
	apiBase = srv.URL
	srv.Close() // nothing is listening now
	t.Cleanup(func() { apiBase = prev })

	ch := New(Config{Name: "ops", Token: "tok", ChatID: "-100"})
	err := ch.Send(context.Background(), testMessage())
	if err == nil {
		t.Fatal("expected a transport error")
	}
	if channel.IsPermanent(err) {
		t.Errorf("a transport failure must stay retryable, not give up on attempt 1: %v", err)
	}
}
