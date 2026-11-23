// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// Package events holds the notifier's on-the-wire and at-rest models:
// the event a publisher posts, the channel it may be routed to, and the
// delivery row that tracks one (event, channel) attempt chain.
package events

import (
	"time"
)

// Event is what a publisher POSTs to /api/v1/events.
type Event struct {
	// ID is the publisher-supplied IDEMPOTENCY KEY, and it is the whole
	// defence against duplicate alerts.
	//
	// A publisher derives it deterministically from the thing that
	// happened — e.g. "backup.failed:<tenant>:<run-id>" — so that a
	// re-fired scheduler tick, a Lambda retry, or an operator replaying
	// a webhook produces the SAME id and is deduped rather than
	// re-delivered. Publishers that generate a random id per POST get
	// no protection, which is why the field is required rather than
	// defaulted.
	ID string `json:"id"`

	// Source is the publishing stack, filled in SERVER-SIDE from the
	// JWT's tenant claim and ignored if present in the body. A
	// publisher cannot attribute an event to someone else.
	Source string `json:"source"`

	// Kind is the event type, e.g. "backup.failed". Dot-namespaced by
	// convention so a channel can filter on a prefix later.
	Kind string `json:"kind"`

	// Severity is one of info/warning/error/critical. An unknown value
	// is REJECTED at ingest (400) rather than accepted and silently
	// dropped by the routing gate — the publisher's bug should surface
	// at the publisher.
	Severity string `json:"severity"`

	// Subject is what the event is about within the source: a tenant
	// stack, a service name, a bucket. Part of the cooldown key, so a
	// single flapping subject cannot silence its siblings.
	Subject string `json:"subject,omitempty"`

	// Title is the one-line summary an operator reads first.
	Title string `json:"title"`

	// Body is the detail: an error string, a run summary.
	Body string `json:"body,omitempty"`

	// URL optionally deep-links into the publisher's own UI.
	URL string `json:"url,omitempty"`

	// At is when the thing happened. Defaulted to receipt time when
	// absent, but publishers should set it — delivery can lag the event
	// by a tick, and an operator reading "15:04" wants the failure's
	// time, not the notifier's.
	At time.Time `json:"at,omitempty"`

	// Labels is publisher-supplied structured context, rendered as
	// key=value and never parsed by the notifier.
	Labels map[string]string `json:"labels,omitempty"`

	// ReceivedAt is set server-side.
	ReceivedAt time.Time `json:"receivedAt,omitempty"`
}

// Event kinds the fleet's publishers emit in v1. Not an allowlist — the
// ingest accepts any non-empty kind, because a component should not need
// a notifier release to emit a new kind. Listed as the documented set.
const (
	KindBackupFailed    = "backup.failed"
	KindBackupCancelled = "backup.cancelled"
	KindBackupSucceeded = "backup.succeeded" // info — off by default
	KindRestoreFailed   = "restore.failed"
	KindHealthChanged   = "service.health.changed"
)

// DedupeWindow is how long an event id is remembered. 24 hours covers a
// daily backup schedule re-firing, which is the case this exists for.
const DedupeWindow = 24 * time.Hour

// Delivery tracks one (event, channel) pair through its attempt chain.
//
// The shape is not invented: it is what fixed a live incident. On
// 2026-09-19 forge-notifier had no retry cap and sent 20+ duplicate
// Telegram messages for a single notification. The fields below are the
// minimum that makes "how many times have we tried, and are we done" a
// question the storage layer can answer rather than an inference.
type Delivery struct {
	// ID is "<eventID>|<channelName>" — deterministic, so routing the
	// same event twice produces the same row rather than a second one.
	ID string `json:"id"`

	EventID string `json:"eventId"`
	Channel string `json:"channel"`

	// Event is denormalised onto the row so the drain needs one read,
	// not two, and so the SPA can show what was sent even after the
	// event's dedupe record has expired.
	Event Event `json:"event"`

	// Attempts counts send attempts made so far.
	Attempts int `json:"attempts"`

	// LastTriedAt is when the most recent attempt ran.
	LastTriedAt *time.Time `json:"lastTriedAt,omitempty"`

	// LastError is the most recent failure, for the SPA.
	LastError string `json:"lastError,omitempty"`

	// DeliveredAt is set on success. Non-nil means done, succeeded.
	DeliveredAt *time.Time `json:"deliveredAt,omitempty"`

	// GiveUpAt is set when attempts are exhausted or a permanent error
	// is seen. Non-nil means done, failed.
	//
	// The row is KEPT after this, not deleted: "we gave up on this
	// alert" is exactly the thing an operator needs to be able to see,
	// and a queue that tidies its own failures away is how a notifier
	// ends up silently not notifying.
	GiveUpAt *time.Time `json:"giveUpAt,omitempty"`

	// Permanent records that the give-up was a config error (bad chat
	// id, revoked token) rather than exhausted retries, so the SPA can
	// point at the right fix.
	Permanent bool `json:"permanent,omitempty"`

	CreatedAt time.Time `json:"createdAt"`
}

// MaxDeliveryAttempts caps the attempt chain.
//
// Five, matching forge-notifier's post-incident value. The cap is the
// point: before it existed one notification produced 20+ Telegram
// messages, because every tick re-read a row that had no record of
// having been tried.
const MaxDeliveryAttempts = 5

// Done reports whether the row has reached a terminal state.
func (d *Delivery) Done() bool { return d.DeliveredAt != nil || d.GiveUpAt != nil }

// DefaultCooldown is the per-(source, subject, kind) quiet period.
//
// 15 minutes. A service that flaps up/down every probe cycle would
// otherwise produce a message every cycle, and an operator who mutes a
// storming channel is strictly worse off than one who got a single
// alert. Suppression is RECORDED (the delivery log shows it) so a muted
// alert is never an invisible one.
const DefaultCooldown = 15 * time.Minute

// ChannelRecord is a channel as persisted. Declarative channels are
// re-seeded from config on every boot; SPA-created ones live only here.
type ChannelRecord struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	MinSeverity string   `json:"minSeverity,omitempty"`
	Sources     []string `json:"sources,omitempty"`
	Kinds       []string `json:"kinds,omitempty"`

	// ChatID / URL are the non-secret destination parts.
	ChatID string `json:"chatId,omitempty"`
	URL    string `json:"url,omitempty"`

	// Credential is the sealed token (AES-256-GCM under a per-tenant
	// DEK). NEVER rendered to the SPA — reads return HasCredential
	// instead. Empty for a managed channel, whose credential comes from
	// the SC secrets registry at boot and is never persisted at all.
	Credential []byte `json:"credential,omitempty"`

	// Managed marks a channel seeded from server.yaml. Managed channels
	// are authoritative, read-only in the SPA, and re-seeded every
	// boot. One channel has exactly one origin, so "which wins" never
	// becomes a question.
	Managed bool `json:"managed"`

	// Enabled lets an operator mute a channel without deleting it.
	Enabled bool `json:"enabled"`

	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// HasCredential reports whether a secret is held, without revealing it.
func (c *ChannelRecord) HasCredential() bool { return len(c.Credential) > 0 }
