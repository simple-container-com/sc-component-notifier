// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// Package registry turns channel CONFIGURATION into live channels.
//
// Two origins feed it, which is decision D3 ("both"):
//
//   - MANAGED channels, declared in the operator's server.yaml. Re-seeded
//     on every boot, authoritative, read-only in the SPA. Their
//     credentials come from the SC secrets registry and live in memory
//     only.
//   - SPA channels, created by an operator in the UI. Persisted, fully
//     editable, credential sealed at rest.
//
// A channel has exactly one origin, so "which one wins" is never a
// question an operator has to reason about. The one case where the
// origins can collide — the same name declared in both — is resolved
// loudly in Seed below rather than by precedence rules nobody reads.
//
// Everything above this package sees one `map[string]channel.Channel`
// and one `[]events.Route`, so the routing gate and the delivery drain
// have a single implementation regardless of where a channel came from.
package registry

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/simple-container-com/sc-component-notifier/service/internal/channel"
	"github.com/simple-container-com/sc-component-notifier/service/internal/channel/telegram"
	"github.com/simple-container-com/sc-component-notifier/service/internal/channel/webhook"
	"github.com/simple-container-com/sc-component-notifier/service/internal/crypto"
	"github.com/simple-container-com/sc-component-notifier/service/internal/events"
)

// CredentialScope is the seal scope for SPA-written channel credentials.
//
// Channels are the notifier's own operator-level configuration, not a
// publisher's, so there is one scope rather than one per tenant. The
// scope exists so the derivation is namespaced and a future per-tenant
// secret cannot be opened with this key.
const CredentialScope = "channels"

// Channel types this component implements.
const (
	TypeTelegram = "telegram"
	TypeWebhook  = "webhook"
)

// Managed is one declaratively-configured channel, with its credential
// already resolved from the secrets registry.
//
// This mirrors api.ChannelConfig by hand because the service module does
// NOT import .../api — that module drags in the whole SC core and the
// Pulumi SDK, which have no business inside a Lambda. The field names
// are identical so the two are greppable together.
type Managed struct {
	Name        string
	Type        string
	MinSeverity string
	Sources     []string
	Kinds       []string

	// Credential is the resolved Telegram bot token or webhook HMAC
	// secret. Held in memory for the process lifetime and NEVER written
	// to the store.
	Credential string

	ChatID string
	URL    string
}

// Logger is the minimal logging seam, matching events.Logger.
type Logger interface {
	Infof(format string, args ...any)
	Errorf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Infof(string, ...any)  {}
func (nopLogger) Errorf(string, ...any) {}

// Registry resolves the live channel set.
type Registry struct {
	store   events.Store
	sealer  *crypto.Sealer
	managed map[string]Managed
	log     Logger
	now     func() time.Time
}

// Config wires a Registry.
type Config struct {
	Store events.Store

	// Sealer opens SPA-written credentials. May be nil, in which case
	// SPA channels are listed but unusable with a stated reason — which
	// is the honest rendering of "this build cannot read its own
	// secrets" and is strictly better than a channel that looks fine
	// and silently sends nothing.
	Sealer *crypto.Sealer

	// Managed is the declarative set, credentials already resolved.
	Managed []Managed

	Log Logger
	Now func() time.Time
}

func New(cfg Config) *Registry {
	log := cfg.Log
	if log == nil {
		log = nopLogger{}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	managed := make(map[string]Managed, len(cfg.Managed))
	for _, m := range cfg.Managed {
		managed[m.Name] = m
	}
	return &Registry{store: cfg.Store, sealer: cfg.Sealer, managed: managed, log: log, now: now}
}

// Seed reconciles the store's managed records against the declaration.
//
// Called once at boot. Three things happen, and the third is the one
// that is easy to forget:
//
//  1. Each declared channel is upserted as a managed record — WITHOUT
//     its credential, which stays in memory.
//  2. A record that was managed and is no longer declared is DELETED.
//     Without this, removing a channel from server.yaml would leave it
//     alive in storage and still delivering: an operator who deleted a
//     channel from their config would keep getting messages from it,
//     with nothing in the config to explain why.
//  3. A declared name that already exists as an SPA-created channel is
//     TAKEN OVER, loudly. server.yaml is the operator's authoritative
//     statement and must win; refusing the declaration instead would
//     mean a channel the operator declared does not deliver, and a
//     notifier that silently does not notify is the worst outcome
//     available. The old credential is dropped, which is why this logs
//     at error level rather than quietly reconciling.
func (r *Registry) Seed(ctx context.Context) error {
	now := r.now().UTC()

	existing, err := r.store.ListChannels(ctx)
	if err != nil {
		return fmt.Errorf("list channels: %w", err)
	}
	byName := make(map[string]*events.ChannelRecord, len(existing))
	for _, c := range existing {
		byName[c.Name] = c
	}

	for name, m := range r.managed {
		if prev, ok := byName[name]; ok && !prev.Managed {
			r.log.Errorf("channel %q is declared in server.yaml and also exists as a UI-created channel; "+
				"the declaration wins and the UI channel's stored credential is discarded", name)
		}

		rec := &events.ChannelRecord{
			Name:        name,
			Type:        m.Type,
			MinSeverity: m.MinSeverity,
			Sources:     m.Sources,
			Kinds:       m.Kinds,
			ChatID:      m.ChatID,
			URL:         m.URL,
			Managed:     true,
			Enabled:     true,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if prev, ok := byName[name]; ok && !prev.CreatedAt.IsZero() {
			rec.CreatedAt = prev.CreatedAt
		}
		if err := r.store.PutChannel(ctx, rec); err != nil {
			return fmt.Errorf("seed channel %q: %w", name, err)
		}
	}

	for _, c := range existing {
		if !c.Managed {
			continue
		}
		if _, stillDeclared := r.managed[c.Name]; stillDeclared {
			continue
		}
		if err := r.store.DeleteChannel(ctx, c.Name); err != nil {
			return fmt.Errorf("remove undeclared managed channel %q: %w", c.Name, err)
		}
		r.log.Infof("channel %q was managed but is no longer declared in server.yaml; removed", c.Name)
	}
	return nil
}

// Routes returns the routing rules for every enabled channel.
//
// Read from the STORE, not from the declaration, so managed and
// SPA-created channels go through identical gate logic. The store is
// authoritative for routing because Seed has already reconciled it.
func (r *Registry) Routes(ctx context.Context) ([]events.Route, error) {
	recs, err := r.store.ListChannels(ctx)
	if err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}
	routes := make([]events.Route, 0, len(recs))
	for _, c := range recs {
		routes = append(routes, events.Route{
			Channel:     c.Name,
			MinSeverity: c.MinSeverity,
			Sources:     c.Sources,
			Kinds:       c.Kinds,
			Enabled:     c.Enabled,
		})
	}
	return routes, nil
}

// Channels builds the live, sendable channel set.
//
// Resolved on every call rather than cached, because this is what the
// drain calls each tick: a credential changed in the SPA between ticks
// must take effect on the next one, and a cached map would keep using a
// revoked token until the Lambda container recycled.
//
// A channel that cannot be constructed is OMITTED, with the reason
// logged. The drain treats a missing channel as a PERMANENT delivery
// failure, so the row shows up in the SPA with the reason rather than
// retrying five times against a channel that cannot exist.
func (r *Registry) Channels(ctx context.Context) (map[string]channel.Channel, error) {
	recs, err := r.store.ListChannels(ctx)
	if err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}

	out := make(map[string]channel.Channel, len(recs))
	for _, rec := range recs {
		if !rec.Enabled {
			continue
		}
		ch, err := r.build(rec)
		if err != nil {
			r.log.Errorf("channel %q (%s) is configured but unusable: %v", rec.Name, rec.Type, err)
			continue
		}
		out[rec.Name] = ch
	}
	return out, nil
}

// build turns one record into a channel, pairing it with its credential.
func (r *Registry) build(rec *events.ChannelRecord) (channel.Channel, error) {
	cred, err := r.credential(rec)
	if err != nil {
		return nil, err
	}

	switch rec.Type {
	case TypeTelegram:
		if cred == "" {
			return nil, fmt.Errorf("no bot token")
		}
		if rec.ChatID == "" {
			return nil, fmt.Errorf("no chat id")
		}
		return telegram.New(telegram.Config{Name: rec.Name, Token: cred, ChatID: rec.ChatID}), nil

	case TypeWebhook:
		if rec.URL == "" {
			return nil, fmt.Errorf("no url")
		}
		// An empty secret is legitimate here (a receiver behind its own
		// auth), so unlike Telegram's token this is not an error. The
		// webhook channel sends NO signature header rather than one
		// computed over an empty key.
		return webhook.New(webhook.Config{Name: rec.Name, URL: rec.URL, Secret: cred}), nil

	default:
		return nil, fmt.Errorf("unsupported channel type %q (this build supports %q and %q)",
			rec.Type, TypeTelegram, TypeWebhook)
	}
}

// credential returns the plaintext credential for a record.
func (r *Registry) credential(rec *events.ChannelRecord) (string, error) {
	if rec.Managed {
		m, ok := r.managed[rec.Name]
		if !ok {
			// Only reachable if Seed did not run, or ran against a
			// different declaration than the one this process holds.
			return "", fmt.Errorf("managed channel is in storage but not in this process's configuration " +
				"(stale seed — redeploy to reconcile)")
		}
		return m.Credential, nil
	}
	if !rec.HasCredential() {
		return "", nil
	}
	if r.sealer == nil {
		return "", fmt.Errorf("a stored credential exists but this process has no encryption key to open it")
	}
	return r.sealer.Open(CredentialScope, rec.Credential)
}

// Status is one channel's state for the SPA.
//
// Reason is populated whenever Usable is false. Capabilities() and
// Health() exist on the Channel interface precisely so this struct can
// be filled: an unconfigured channel must render as ABSENT WITH A
// REASON, never as silently fine.
type Status struct {
	Name          string               `json:"name"`
	Type          string               `json:"type"`
	Managed       bool                 `json:"managed"`
	Enabled       bool                 `json:"enabled"`
	MinSeverity   string               `json:"minSeverity"`
	Sources       []string             `json:"sources,omitempty"`
	Kinds         []string             `json:"kinds,omitempty"`
	Destination   string               `json:"destination,omitempty"`
	HasCredential bool                 `json:"hasCredential"`
	Usable        bool                 `json:"usable"`
	Reason        string               `json:"reason,omitempty"`
	Capabilities  channel.Capabilities `json:"capabilities"`
}

// Statuses describes every configured channel, usable or not.
//
// Health() is NOT called here: it costs a network round trip per
// channel, and this is read by a SPA page load. A dedicated
// probe-on-demand endpoint is the right home for that.
func (r *Registry) Statuses(ctx context.Context) ([]Status, error) {
	recs, err := r.store.ListChannels(ctx)
	if err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}

	out := make([]Status, 0, len(recs))
	for _, rec := range recs {
		st := Status{
			Name:          rec.Name,
			Type:          rec.Type,
			Managed:       rec.Managed,
			Enabled:       rec.Enabled,
			MinSeverity:   rec.MinSeverity,
			Sources:       rec.Sources,
			Kinds:         rec.Kinds,
			HasCredential: rec.HasCredential(),
		}
		if st.MinSeverity == "" {
			// Show the EFFECTIVE minimum, not the empty literal. An
			// operator reading "minSeverity: (blank)" would reasonably
			// assume everything gets through, and the one thing this
			// component has to be unambiguous about is which severities
			// are silently not being delivered.
			st.MinSeverity = channel.SeverityWarning
		}
		switch rec.Type {
		case TypeTelegram:
			st.Destination = "chat " + rec.ChatID
		case TypeWebhook:
			st.Destination = rec.URL
		}
		if rec.Managed {
			// A managed channel's credential is in memory, not in the
			// record, so HasCredential() on the record is always false.
			// Report what the operator actually wants to know: does
			// this channel have a usable credential right now.
			m, ok := r.managed[rec.Name]
			st.HasCredential = ok && m.Credential != ""
		}

		ch, err := r.build(rec)
		switch {
		case err != nil:
			st.Reason = err.Error()
		case !rec.Enabled:
			st.Reason = "disabled by operator"
		default:
			st.Usable = true
			st.Capabilities = ch.Capabilities()
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
