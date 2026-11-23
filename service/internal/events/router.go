// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package events

import (
	"context"
	"fmt"
	"time"

	"github.com/simple-container-com/sc-component-notifier/service/internal/channel"
)

// Route describes one channel's routing rules, independent of how that
// channel delivers. Built from either a managed ChannelConfig or an
// SPA-created ChannelRecord, so the gate logic has exactly one
// implementation.
type Route struct {
	Channel     string
	MinSeverity string
	Sources     []string
	Kinds       []string
	Enabled     bool
}

// Matches reports whether ev should be delivered to this route, and why
// not when it should not.
//
// The reason string is returned rather than logged because "it was not
// delivered and here is which gate stopped it" is the single most
// useful thing the SPA can show. A boolean here would mean every
// non-delivery looked identical to a broken notifier.
func (r Route) Matches(ev *Event) (bool, string) {
	if !r.Enabled {
		return false, "channel disabled"
	}

	min := r.MinSeverity
	if min == "" {
		min = channel.SeverityWarning
	}
	// Both ranks come from the same map, so an unknown severity on
	// EITHER side ranks 0 and fails the comparison — closed, not open.
	if channel.SeverityRank(ev.Severity) < channel.SeverityRank(min) {
		return false, fmt.Sprintf("severity %q below channel minimum %q", ev.Severity, min)
	}
	if len(r.Sources) > 0 && !contains(r.Sources, ev.Source) {
		return false, fmt.Sprintf("source %q not in channel allowlist", ev.Source)
	}
	if len(r.Kinds) > 0 && !contains(r.Kinds, ev.Kind) {
		return false, fmt.Sprintf("kind %q not in channel allowlist", ev.Kind)
	}
	return true, ""
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

// RouteDecision records what happened to one (event, channel) pair.
type RouteDecision struct {
	Channel   string
	Delivered bool   // a Delivery row was enqueued
	Reason    string // why not, when Delivered is false
}

// Router turns an accepted event into delivery rows.
type Router struct {
	store    Store
	cooldown time.Duration
}

// NewRouter builds a router. A zero cooldown uses DefaultCooldown; pass
// a negative value to disable suppression entirely (tests do).
func NewRouter(store Store, cooldown time.Duration) *Router {
	if cooldown == 0 {
		cooldown = DefaultCooldown
	}
	return &Router{store: store, cooldown: cooldown}
}

// Route fans ev out into one Delivery row per matching route.
//
// The cooldown is checked ONCE for the event, not per channel: the
// suppression question is "has this source/subject/kind already been
// reported recently", and asking it per channel would let a flapping
// service through once for every configured channel.
func (r *Router) Route(ctx context.Context, ev *Event, routes []Route, now time.Time) ([]RouteDecision, error) {
	decisions := make([]RouteDecision, 0, len(routes))

	// Evaluate the gates first so a fully-filtered event never starts a
	// cooldown. Otherwise an `info` event nothing is subscribed to would
	// silence the `error` that follows it 30 seconds later.
	matched := make([]Route, 0, len(routes))
	for _, rt := range routes {
		ok, reason := rt.Matches(ev)
		if !ok {
			decisions = append(decisions, RouteDecision{Channel: rt.Channel, Reason: reason})
			continue
		}
		matched = append(matched, rt)
	}
	if len(matched) == 0 {
		return decisions, nil
	}

	if r.cooldown > 0 {
		key := CooldownKey(ev.Source, ev.Subject, ev.Kind)
		active, err := r.store.CooldownActive(ctx, key, r.cooldown, now)
		if err != nil {
			return nil, fmt.Errorf("cooldown check: %w", err)
		}
		if active {
			for _, rt := range matched {
				decisions = append(decisions, RouteDecision{
					Channel: rt.Channel,
					Reason:  fmt.Sprintf("suppressed: %s already reported within %s", key, r.cooldown),
				})
			}
			return decisions, nil
		}
	}

	for _, rt := range matched {
		d := &Delivery{
			ID:        DeliveryID(ev.ID, rt.Channel),
			EventID:   ev.ID,
			Channel:   rt.Channel,
			Event:     *ev,
			CreatedAt: now,
		}
		if err := r.store.PutDelivery(ctx, d); err != nil {
			return nil, fmt.Errorf("enqueue delivery for channel %q: %w", rt.Channel, err)
		}
		decisions = append(decisions, RouteDecision{Channel: rt.Channel, Delivered: true})
	}
	return decisions, nil
}
