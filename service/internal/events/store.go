// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package events

import (
	"context"
	"time"
)

// Store is the notifier's persistence seam.
//
// Deliberately NOT a document-database interface: observatory has no
// Mongo either, and the notifier's volume (a handful of events a day, a
// few channels) does not justify a paid cluster. The implementation is
// JSON objects in whatever `storage: <resource-name>` points at, which
// keeps the operator's only storage knob a reference to an SC resource.
type Store interface {
	// SeenEvent reports whether id has been accepted within the dedupe
	// window, and records it if not. The check and the record are one
	// call so an ingest handler cannot accidentally do one without the
	// other.
	//
	// Returns true when the event is a DUPLICATE and must be dropped.
	SeenEvent(ctx context.Context, id string, now time.Time) (bool, error)

	// PutDelivery writes (or overwrites) a delivery row.
	PutDelivery(ctx context.Context, d *Delivery) error

	// GetDelivery reads one row by id.
	GetDelivery(ctx context.Context, id string) (*Delivery, error)

	// ListDue returns rows that still need an attempt — i.e. those
	// where DeliveredAt and GiveUpAt are both nil — oldest first, up to
	// limit.
	//
	// The filter is the store's job rather than the caller's because
	// getting it wrong is what produced the 20-duplicate incident: a
	// drain that re-reads terminal rows will re-send them.
	ListDue(ctx context.Context, now time.Time, limit int) ([]*Delivery, error)

	// ListDeliveries returns recent rows for the SPA, newest first,
	// including terminal ones.
	ListDeliveries(ctx context.Context, limit int) ([]*Delivery, error)

	// PutChannel writes a channel record.
	PutChannel(ctx context.Context, c *ChannelRecord) error

	// GetChannel reads one channel by name.
	GetChannel(ctx context.Context, name string) (*ChannelRecord, error)

	// ListChannels returns all channel records, managed and
	// SPA-created, sorted by name.
	ListChannels(ctx context.Context) ([]*ChannelRecord, error)

	// DeleteChannel removes an SPA-created channel. Deleting a managed
	// one is refused at the handler, not here.
	DeleteChannel(ctx context.Context, name string) error

	// CooldownActive reports whether key is within its quiet period,
	// and starts a new one if not.
	//
	// Returns true when the caller must SUPPRESS. Combined like
	// SeenEvent so the check cannot be made without the record.
	CooldownActive(ctx context.Context, key string, window time.Duration, now time.Time) (bool, error)

	// AcquireTickLease takes the single-flight lease for the delivery
	// drain, returning false when another drain holds it.
	//
	// Overlapping ticks are THE duplicate-delivery vector in this
	// design: the drain runs inline on a scheduled HTTP tick (a Lambda
	// cannot hold a ticker — the runtime freezes background goroutines
	// the instant the handler returns), so two ticks that overlap would
	// both read the same due rows. On S3 this is a conditional put;
	// elsewhere it is best-effort, which is sound because those
	// backends only run single-process.
	AcquireTickLease(ctx context.Context, ttl time.Duration, now time.Time) (bool, error)

	// ReleaseTickLease drops the lease early, so a fast drain does not
	// block the next tick for the whole TTL.
	ReleaseTickLease(ctx context.Context) error
}

// CooldownKey builds the suppression key.
//
// Per (source, subject, kind) — NOT per channel and NOT per source
// alone. Per source alone would let one flapping service silence every
// other alert from the same component; per channel would let the same
// flap through once per channel.
func CooldownKey(source, subject, kind string) string {
	return source + "|" + subject + "|" + kind
}

// DeliveryID builds the deterministic row id.
func DeliveryID(eventID, channelName string) string {
	return eventID + "|" + channelName
}
