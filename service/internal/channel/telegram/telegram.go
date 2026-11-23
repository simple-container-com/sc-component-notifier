// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// Package telegram implements channel.Channel over the Telegram Bot API.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/simple-container-com/sc-component-notifier/service/internal/channel"
)

// apiBase is the Bot API root. A variable so tests can point it at an
// httptest server.
var apiBase = "https://api.telegram.org"

// maxMessageBytes is Telegram's documented sendMessage cap (4096 UTF-8
// characters). Exceeding it is a 400, so the body is truncated with a
// visible marker rather than letting the send fail — losing the tail of
// a stack trace beats losing the whole alert.
const maxMessageBytes = 4096

// Channel is one Telegram destination: a bot token plus a chat.
type Channel struct {
	name   string
	token  string
	chatID string
	client *http.Client
}

// Config carries one channel's wiring.
type Config struct {
	Name   string
	Token  string
	ChatID string
	Client *http.Client // nil → channel.NewHTTPClient()
}

// New builds a Telegram channel.
func New(cfg Config) *Channel {
	c := cfg.Client
	if c == nil {
		c = channel.NewHTTPClient()
	}
	return &Channel{name: cfg.Name, token: cfg.Token, chatID: cfg.ChatID, client: c}
}

func (c *Channel) Name() string { return c.name }
func (c *Channel) Type() string { return "telegram" }

func (c *Channel) Capabilities() channel.Capabilities {
	return channel.Capabilities{
		Markdown:     true,
		MaxBodyBytes: maxMessageBytes,
		// Inbound acknowledgement would need callback handling, and
		// `setWebhook` is PER-BOT: pointing it at the notifier would
		// take over the whole bot, breaking whatever else uses it. So
		// inbound requires a dedicated bot, which v1 deliberately does
		// not require the operator to create.
		InboundAck: false,
	}
}

// Send posts one sendMessage call.
func (c *Channel) Send(ctx context.Context, m channel.Message) error {
	if c.token == "" {
		return channel.Permanentf("telegram channel %q has no bot token", c.name)
	}
	if c.chatID == "" {
		return channel.Permanentf("telegram channel %q has no chat id", c.name)
	}

	payload := map[string]any{
		"chat_id":                  c.chatID,
		"text":                     Render(m),
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return channel.Permanent(fmt.Errorf("marshal telegram payload: %w", err))
	}

	status, respBody, err := c.post(ctx, "sendMessage", body)
	if err != nil {
		// Transport failure — timeout, DNS, connection reset. Retryable.
		return fmt.Errorf("telegram sendMessage to channel %q: %w", c.name, err)
	}
	if status >= 200 && status < 300 {
		return nil
	}
	// Telegram puts a human-readable reason in `description`; surface it,
	// because "400 Bad Request" alone sends the operator to the wrong
	// place (it is almost always a wrong chat_id or a bot not in the chat).
	reason := describeError(respBody)
	err = fmt.Errorf("telegram channel %q: http %d: %s", c.name, status, reason)
	if !channel.RetryableFromStatus(status) {
		return channel.Permanent(err)
	}
	return err
}

// Health calls getMe, which validates the token without sending anything
// to the chat. It deliberately does NOT validate the chat id: there is no
// read-only way to do that, and probing by sending a message would mean
// the health check spams the operator.
func (c *Channel) Health(ctx context.Context) error {
	if c.token == "" {
		return channel.Permanentf("telegram channel %q has no bot token", c.name)
	}
	if c.chatID == "" {
		return channel.Permanentf("telegram channel %q has no chat id", c.name)
	}
	status, respBody, err := c.post(ctx, "getMe", []byte("{}"))
	if err != nil {
		return fmt.Errorf("telegram getMe for channel %q: %w", c.name, err)
	}
	if status >= 200 && status < 300 {
		return nil
	}
	return fmt.Errorf("telegram channel %q: getMe http %d: %s", c.name, status, describeError(respBody))
}

func (c *Channel) post(ctx context.Context, method string, body []byte) (int, []byte, error) {
	url := fmt.Sprintf("%s/bot%s/%s", strings.TrimRight(apiBase, "/"), c.token, method)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", channel.UserAgent)

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	// Read the WHOLE body. forge-notifier capped a response read at 4KB
	// and then parsed the truncated JSON, which yielded a false
	// `ok=false` on responses that had actually succeeded. These bodies
	// are small; a cap buys nothing and costs correctness.
	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, respBody, nil
}

// describeError pulls Telegram's `description` out of an error body,
// falling back to the raw bytes.
func describeError(body []byte) string {
	var env struct {
		OK          bool   `json:"ok"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal(body, &env); err == nil && env.Description != "" {
		return env.Description
	}
	s := string(body)
	if len(s) > 256 {
		return s[:256] + "…"
	}
	if s == "" {
		return "(empty response body)"
	}
	return s
}

// Render formats a Message as Telegram HTML.
//
// Exported so a test can assert the rendering without a transport, and
// so the SPA's preview can show exactly what will be sent.
func Render(m channel.Message) string {
	var b strings.Builder

	b.WriteString(severityPrefix(m.Severity))
	b.WriteString(" <b>")
	b.WriteString(escape(m.Title))
	b.WriteString("</b>\n")

	if m.Source != "" || m.Kind != "" {
		b.WriteString("<i>")
		b.WriteString(escape(strings.TrimSpace(m.Source + " · " + m.Kind)))
		b.WriteString("</i>\n")
	}
	if m.Subject != "" {
		b.WriteString("subject: <code>")
		b.WriteString(escape(m.Subject))
		b.WriteString("</code>\n")
	}
	if m.Body != "" {
		b.WriteString("\n")
		b.WriteString(escape(m.Body))
		b.WriteString("\n")
	}
	if len(m.Labels) > 0 {
		b.WriteString("\n")
		// Sorted so the same event always renders identically — an
		// unstable map order would make two deliveries of one event
		// look like different messages.
		keys := make([]string, 0, len(m.Labels))
		for k := range m.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			b.WriteString("<code>")
			b.WriteString(escape(k + "=" + m.Labels[k]))
			b.WriteString("</code>\n")
		}
	}
	if m.URL != "" {
		b.WriteString("\n")
		b.WriteString(escape(m.URL))
	}

	out := b.String()
	if len(out) > maxMessageBytes {
		const marker = "\n…(truncated)"
		// Cut on a rune boundary — a half-written UTF-8 sequence is a
		// 400 from the API, which would turn a too-long alert into no
		// alert at all.
		cut := maxMessageBytes - len(marker)
		for cut > 0 && !utf8Start(out[cut]) {
			cut--
		}
		out = out[:cut] + marker
	}
	return out
}

// utf8Start reports whether b can begin a UTF-8 sequence (i.e. is not a
// continuation byte).
func utf8Start(b byte) bool { return b&0xC0 != 0x80 }

func severityPrefix(s string) string {
	switch s {
	case channel.SeverityCritical:
		return "🔴"
	case channel.SeverityError:
		return "🟠"
	case channel.SeverityWarning:
		return "🟡"
	case channel.SeverityInfo:
		return "🔵"
	default:
		// An unknown severity is still shown rather than dropped at
		// render time — the gate that decides whether it is delivered
		// lives in routing, and a renderer that silently swallowed it
		// would hide a publisher's bug.
		return "⚪"
	}
}

// escape handles the three characters Telegram's HTML parse mode treats
// as markup. Anything else is passed through: these bodies contain error
// text and stack traces, and over-escaping makes them unreadable.
func escape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	return strings.ReplaceAll(s, ">", "&gt;")
}
