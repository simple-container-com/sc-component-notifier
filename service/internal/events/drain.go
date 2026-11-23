// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package events

import (
	"context"
	"fmt"
	"time"

	"github.com/simple-container-com/sc-component-notifier/service/internal/channel"
)

// LeaseTTL bounds how long one drain may hold the single-flight lease.
//
// Longer than a drain should ever take (bounded by BatchLimit ×
// channel.SendTimeout) but short enough that a drain killed mid-flight
// unblocks the queue within a couple of ticks rather than wedging it.
const LeaseTTL = 2 * time.Minute

// BatchLimit caps rows per drain.
//
// The drain runs INLINE in an HTTP handler, so the whole batch must fit
// inside the Lambda's timeout with room to spare: 25 rows × a 15s
// worst-case send is well past any sane handler budget, but real sends
// take well under a second and the cap exists for the pathological case.
// Leftover rows are picked up by the next tick — the queue is durable,
// so a partial drain is a delay, not a loss.
const BatchLimit = 25

// Logger is the minimal logging seam, so this package pulls in no
// logging dependency and tests can assert on what was logged.
type Logger interface {
	Infof(format string, args ...any)
	Errorf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Infof(string, ...any)  {}
func (nopLogger) Errorf(string, ...any) {}

// Drainer sends due deliveries. One per process.
type Drainer struct {
	store    Store
	channels func(ctx context.Context) (map[string]channel.Channel, error)
	log      Logger
	now      func() time.Time
}

// DrainerConfig wires a Drainer.
type DrainerConfig struct {
	Store Store

	// Channels resolves the live channel set at drain time rather than
	// at construction. A channel's credential can change under the SPA
	// between ticks, and a Drainer holding a stale closure would keep
	// using a revoked token.
	Channels func(ctx context.Context) (map[string]channel.Channel, error)

	Log Logger
	Now func() time.Time
}

func NewDrainer(cfg DrainerConfig) *Drainer {
	log := cfg.Log
	if log == nil {
		log = nopLogger{}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Drainer{store: cfg.Store, channels: cfg.Channels, log: log, now: now}
}

// DrainResult summarises one tick, for the HTTP response and the log.
type DrainResult struct {
	// Skipped is true when another drain held the lease. NOT an error:
	// on a one-minute schedule this is the normal outcome whenever a
	// previous drain is still running.
	Skipped   bool `json:"skipped"`
	Examined  int  `json:"examined"`
	Delivered int  `json:"delivered"`
	Failed    int  `json:"failed"`
	GaveUp    int  `json:"gaveUp"`
}

// Drain attempts every due delivery once, under the single-flight lease.
//
// Why inline and not a background goroutine: a Lambda FREEZES background
// goroutines the instant the handler returns. A `go func()` here would
// appear to work locally and then silently deliver nothing in
// production — the handler would return 200 having sent zero messages.
// Every periodic job in this fleet is therefore an external schedule
// hitting an endpoint that does the work synchronously.
func (d *Drainer) Drain(ctx context.Context) (DrainResult, error) {
	now := d.now().UTC()

	ok, err := d.store.AcquireTickLease(ctx, LeaseTTL, now)
	if err != nil {
		return DrainResult{}, fmt.Errorf("acquire tick lease: %w", err)
	}
	if !ok {
		d.log.Infof("drain: another drain holds the lease, skipping this tick")
		return DrainResult{Skipped: true}, nil
	}
	defer func() {
		if err := d.store.ReleaseTickLease(ctx); err != nil {
			// Not fatal: the lease has a TTL, so a failed release costs
			// at most LeaseTTL of delay, not a wedged queue.
			d.log.Errorf("drain: release tick lease: %v", err)
		}
	}()

	due, err := d.store.ListDue(ctx, now, BatchLimit)
	if err != nil {
		return DrainResult{}, fmt.Errorf("list due deliveries: %w", err)
	}

	chans, err := d.channels(ctx)
	if err != nil {
		return DrainResult{}, fmt.Errorf("resolve channels: %w", err)
	}

	res := DrainResult{Examined: len(due)}
	for _, row := range due {
		switch d.attempt(ctx, row, chans) {
		case outcomeDelivered:
			res.Delivered++
		case outcomeGaveUp:
			res.GaveUp++
		case outcomeFailed:
			res.Failed++
		}
	}
	if res.Examined > 0 {
		d.log.Infof("drain: examined=%d delivered=%d failed=%d gaveUp=%d",
			res.Examined, res.Delivered, res.Failed, res.GaveUp)
	}
	return res, nil
}

type outcome int

const (
	outcomeDelivered outcome = iota
	outcomeFailed
	outcomeGaveUp
)

// attempt makes one send attempt and persists the result.
//
// The attempt counter is incremented and the row written BEFORE the send
// on the give-up path, so a crash mid-send cannot produce a row that
// looks untried. The alternative — count after a successful send — is
// what lets a crash loop re-send the same message forever, which is the
// 2026-09-19 incident in one sentence.
func (d *Drainer) attempt(ctx context.Context, row *Delivery, chans map[string]channel.Channel) outcome {
	now := d.now().UTC()
	ch, found := chans[row.Channel]

	row.Attempts++
	row.LastTriedAt = &now

	if !found {
		// The channel was deleted or renamed after the row was
		// enqueued. Permanent: no number of retries conjures it back.
		row.LastError = fmt.Sprintf("channel %q no longer configured", row.Channel)
		row.GiveUpAt = &now
		row.Permanent = true
		d.log.Errorf("drain: delivery %s: %s", row.ID, row.LastError)
		d.persist(ctx, row)
		return outcomeGaveUp
	}

	msg := channel.Message{
		ID:       row.Event.ID,
		Title:    row.Event.Title,
		Body:     row.Event.Body,
		Severity: row.Event.Severity,
		Source:   row.Event.Source,
		Kind:     row.Event.Kind,
		Subject:  row.Event.Subject,
		URL:      row.Event.URL,
		Labels:   row.Event.Labels,
		At:       row.Event.At,
	}

	// Logged BEFORE the send, deliberately. forge-notifier had sends
	// that failed in a way nothing recorded, and the only reason it was
	// diagnosable at all was a line like this one: if the "sending" line
	// appears with no outcome line after it, the process died inside the
	// send.
	d.log.Infof("drain: sending delivery %s via channel %q (attempt %d/%d)",
		row.ID, row.Channel, row.Attempts, MaxDeliveryAttempts)

	sendCtx, cancel := context.WithTimeout(ctx, channel.SendTimeout)
	err := ch.Send(sendCtx, msg)
	cancel()

	if err == nil {
		row.DeliveredAt = &now
		row.LastError = ""
		d.log.Infof("drain: delivered %s via channel %q on attempt %d", row.ID, row.Channel, row.Attempts)
		d.persist(ctx, row)
		return outcomeDelivered
	}

	row.LastError = err.Error()
	switch {
	case channel.IsPermanent(err):
		row.GiveUpAt = &now
		row.Permanent = true
		d.log.Errorf("drain: delivery %s permanently failed on attempt %d: %v", row.ID, row.Attempts, err)
		d.persist(ctx, row)
		return outcomeGaveUp
	case row.Attempts >= MaxDeliveryAttempts:
		row.GiveUpAt = &now
		d.log.Errorf("drain: delivery %s exhausted %d attempts, giving up: %v", row.ID, row.Attempts, err)
		d.persist(ctx, row)
		return outcomeGaveUp
	default:
		d.log.Errorf("drain: delivery %s attempt %d/%d failed, will retry: %v",
			row.ID, row.Attempts, MaxDeliveryAttempts, err)
		d.persist(ctx, row)
		return outcomeFailed
	}
}

// persist writes the row back, logging rather than returning a failure.
//
// A write failure here is the dangerous case: the message may ALREADY
// have been delivered, and a row that fails to record that will be
// re-sent on the next tick. It cannot be fixed from here, so it is
// logged loudly as the duplicate-risk it is.
func (d *Drainer) persist(ctx context.Context, row *Delivery) {
	if err := d.store.PutDelivery(ctx, row); err != nil {
		d.log.Errorf("drain: FAILED TO PERSIST delivery %s (attempts=%d delivered=%v) — "+
			"this row may be retried and double-send: %v",
			row.ID, row.Attempts, row.DeliveredAt != nil, err)
	}
}
