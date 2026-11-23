// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// Package channel is the notifier's generic delivery seam.
//
// Every per-destination behaviour lives behind the Channel interface, so
// adding Slack, email or an incident manager is a new file in a
// subpackage rather than a new branch in the send path. This mirrors
// sc-component-observatory's `telemetry.Provider` deliberately: that
// component's per-cloud behaviour sits behind one interface for the same
// reason, and the two components should be readable by the same person
// without relearning the shape.
//
// The negative example is in the fleet already. forge-notifier's
// `poller.go` branches `deliverTelegram` / `deliverEmail` as a hard-coded
// switch; every channel added there touches the poller, and the poller is
// the file where duplicate-delivery bugs live. This interface is what
// replaces that switch.
package channel

import (
	"context"
	"time"
)

// Severity values, duplicated from the api module ON PURPOSE.
//
// The service module does NOT import `.../api` — that module depends on
// the whole SC core (Pulumi SDK and all), and the deployed Lambda has no
// business carrying it. The cost is this duplication; the guard against
// drift is that these strings are also the on-the-wire event contract, so
// a change on either side breaks an HTTP test, not just a constant.
const (
	SeverityInfo     = "info"
	SeverityWarning  = "warning"
	SeverityError    = "error"
	SeverityCritical = "critical"
)

// severityOrder maps severity to ordinal. An unknown string is absent
// from the map and therefore ranks 0, which fails every minimum gate.
// That direction is deliberate: a typo'd severity must not pass the
// lowest gate and storm a channel.
var severityOrder = map[string]int{
	SeverityInfo:     1,
	SeverityWarning:  2,
	SeverityError:    3,
	SeverityCritical: 4,
}

// SeverityRank returns the ordinal for s, or 0 when s is not a known
// severity.
func SeverityRank(s string) int { return severityOrder[s] }

// KnownSeverity reports whether s is one of the four. Callers that want
// to REJECT an unknown severity (the ingest handler) use this; callers
// that want to fail it closed (the routing gate) use SeverityRank.
func KnownSeverity(s string) bool {
	_, ok := severityOrder[s]
	return ok
}

// Message is what a channel delivers. Channel-shaped but not
// channel-named — the same discipline as `telemetry.Workload`: a
// Telegram `sendMessage` and a Slack incoming webhook are the same
// question asked twice, and nothing above this struct should be able to
// tell which one is on the other end.
type Message struct {
	// ID is the originating event's idempotency key. Channels that can
	// pass a dedupe hint to the receiver (the webhook's X-SC-Event-Id)
	// use it; channels that cannot ignore it.
	ID string

	// Title is the one-line summary. Rendered bold where the channel
	// supports it.
	Title string

	// Body is the detail. May be multi-line. Channels that cap length
	// truncate with an explicit marker rather than silently.
	Body string

	// Severity is one of the Severity* constants.
	Severity string

	// Source is the publishing stack, e.g. "my-backup".
	Source string

	// Kind is the event kind, e.g. "backup.failed".
	Kind string

	// Subject is what the event is ABOUT within the source — a tenant
	// stack name, a service name. Part of the cooldown key.
	Subject string

	// URL optionally deep-links to the publisher's own UI.
	URL string

	// Labels carries publisher-supplied structured context. Rendered as
	// key=value lines; never parsed by the notifier.
	Labels map[string]string

	// At is when the underlying event happened (not when delivery was
	// attempted).
	At time.Time
}

// Capabilities describes what a channel can render, so a caller can
// degrade deliberately instead of hoping.
//
// Health() and Capabilities() both exist for the reason observatory's
// provider interface has them: an unconfigured or broken channel must
// render as ABSENT WITH A REASON, never as silently fine. A notifier
// that cannot notify is worse than no notifier, and the only way the SPA
// can say so is if the channel can be asked.
type Capabilities struct {
	// Markdown is true when the channel renders basic markup.
	Markdown bool

	// MaxBodyBytes is the channel's hard limit, 0 for "no known limit".
	MaxBodyBytes int

	// InboundAck is true when the channel can carry an acknowledgement
	// back (Telegram callback buttons). False for every v1 channel —
	// `setWebhook` is per-bot, so inbound needs a dedicated bot, which
	// v1 deliberately does not require.
	InboundAck bool
}

// Channel delivers messages to one configured destination.
type Channel interface {
	// Name is the operator-assigned label, e.g. "ops-telegram". Unique
	// within a notifier; it is how the delivery log identifies rows.
	Name() string

	// Type is the implementation kind: "telegram", "webhook", ...
	Type() string

	// Capabilities describes rendering limits.
	Capabilities() Capabilities

	// Send delivers m, or returns an error.
	//
	// The error MUST distinguish permanent from transient — wrap with
	// Permanent() for a 4xx-equivalent so the delivery queue stops
	// retrying instead of burning all five attempts on a bad chat ID.
	Send(ctx context.Context, m Message) error

	// Health reports whether the channel is usable right now, without
	// sending a user-visible message. Called by the SPA and by the
	// component's own self-registration with observatory.
	Health(ctx context.Context) error
}
