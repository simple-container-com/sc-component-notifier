// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// Package webhook implements channel.Channel as a signed JSON POST.
//
// This is the generic escape hatch, and it is the reason v1 ships only
// two channels: Slack, Discord, Mattermost, PagerDuty's events API and
// the eventual sc-component-ims are all "POST some JSON to a URL". Each
// one gets a nicer rendering eventually; none of them is blocked on it.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/simple-container-com/sc-component-notifier/service/internal/channel"
)

// Header names on the outbound request.
const (
	// SignatureHeader carries the hex HMAC-SHA256 of the request body.
	SignatureHeader = "X-SC-Signature"

	// TimestampHeader carries the Unix seconds the signature covers, so
	// a receiver can reject a replayed body. The signature is computed
	// over "<timestamp>.<body>" — signing the body alone would let an
	// intercepted request be replayed forever.
	TimestampHeader = "X-SC-Timestamp"

	// EventIDHeader lets a receiver dedupe without parsing the body.
	EventIDHeader = "X-SC-Event-Id"
)

// Channel is one webhook destination.
type Channel struct {
	name   string
	url    string
	secret string
	client *http.Client
	now    func() time.Time
}

// Config carries one channel's wiring.
type Config struct {
	Name string
	URL  string
	// Secret signs the body. Optional: a receiver on a private network
	// or behind its own auth may not need it. When empty NO signature
	// header is sent — rather than sending one computed over an empty
	// key, which would look authenticated while proving nothing.
	Secret string
	Client *http.Client     // nil → channel.NewHTTPClient()
	Now    func() time.Time // nil → time.Now; injectable for tests
}

// New builds a webhook channel.
func New(cfg Config) *Channel {
	c := cfg.Client
	if c == nil {
		c = channel.NewHTTPClient()
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Channel{name: cfg.Name, url: cfg.URL, secret: cfg.Secret, client: c, now: now}
}

func (c *Channel) Name() string { return c.name }
func (c *Channel) Type() string { return "webhook" }

func (c *Channel) Capabilities() channel.Capabilities {
	return channel.Capabilities{
		Markdown:     false, // the receiver renders; we send structured JSON
		MaxBodyBytes: 0,     // no known limit — the receiver's problem
		InboundAck:   false,
	}
}

// Payload is the on-the-wire contract. Stable and versioned: receivers
// are written against it, including the future incident-management
// component, so fields are ADDED and never repurposed.
type Payload struct {
	Version  int               `json:"version"`
	ID       string            `json:"id"`
	Source   string            `json:"source"`
	Kind     string            `json:"kind"`
	Severity string            `json:"severity"`
	Subject  string            `json:"subject,omitempty"`
	Title    string            `json:"title"`
	Body     string            `json:"body,omitempty"`
	URL      string            `json:"url,omitempty"`
	At       time.Time         `json:"at"`
	Labels   map[string]string `json:"labels,omitempty"`
}

// PayloadVersion is the contract version in every emitted body.
const PayloadVersion = 1

// Send POSTs the signed payload.
func (c *Channel) Send(ctx context.Context, m channel.Message) error {
	if c.url == "" {
		return channel.Permanentf("webhook channel %q has no url", c.name)
	}

	body, err := json.Marshal(Payload{
		Version:  PayloadVersion,
		ID:       m.ID,
		Source:   m.Source,
		Kind:     m.Kind,
		Severity: m.Severity,
		Subject:  m.Subject,
		Title:    m.Title,
		Body:     m.Body,
		URL:      m.URL,
		At:       m.At,
		Labels:   m.Labels,
	})
	if err != nil {
		return channel.Permanent(fmt.Errorf("marshal webhook payload: %w", err))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		// A URL the stdlib refuses is a config error, not a blip.
		return channel.Permanent(fmt.Errorf("webhook channel %q: build request: %w", c.name, err))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", channel.UserAgent)
	if m.ID != "" {
		req.Header.Set(EventIDHeader, m.ID)
	}
	if c.secret != "" {
		ts := strconv.FormatInt(c.now().UTC().Unix(), 10)
		req.Header.Set(TimestampHeader, ts)
		req.Header.Set(SignatureHeader, Sign(c.secret, ts, body))
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook channel %q POST %s: %w", c.name, c.url, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	sendErr := fmt.Errorf("webhook channel %q: http %d: %s", c.name, resp.StatusCode, truncate(string(respBody), 256))
	if !channel.RetryableFromStatus(resp.StatusCode) {
		return channel.Permanent(sendErr)
	}
	return sendErr
}

// Health reports config validity only.
//
// It deliberately does NOT probe the endpoint: a webhook URL is
// write-only by nature — there is no safe GET, and a probe POST would
// deliver a bogus notification to whatever is on the other end. Claiming
// more than this would be the "check that proves nothing" failure mode,
// so the SPA shows exactly what was verified: that the channel is
// configured.
func (c *Channel) Health(_ context.Context) error {
	if c.url == "" {
		return channel.Permanentf("webhook channel %q has no url", c.name)
	}
	return nil
}

// Sign computes the header value: hex(HMAC-SHA256(secret, ts + "." + body)).
//
// Exported so a receiver implementation (and its tests) can verify
// against the same function rather than reimplementing the concatenation
// and getting the separator wrong.
func Sign(secret, ts string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "…"
}
