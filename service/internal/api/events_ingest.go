// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/simple-container-com/sc-component-notifier/service/internal/channel"
	"github.com/simple-container-com/sc-component-notifier/service/internal/events"
	"github.com/simple-container-com/sc-component-notifier/service/internal/tenant"
)

// RouteSource supplies the routing rules in force. Satisfied by
// *registry.Registry; an interface here so this package does not depend
// on how channels are configured.
type RouteSource interface {
	Routes(ctx context.Context) ([]events.Route, error)
}

// IngestResponse is what a publisher gets back.
//
// The per-channel decisions are returned rather than just a status,
// because the single most confusing failure for a publisher is a 200
// followed by no message arriving. With this, a backup component's own
// logs can record "accepted, suppressed: severity below channel
// minimum" — which is the actual answer — instead of "accepted".
type IngestResponse struct {
	Status    string                 `json:"status"` // "accepted" | "duplicate"
	EventID   string                 `json:"eventId"`
	Decisions []events.RouteDecision `json:"decisions,omitempty"`
}

// EventsIngest returns the handler for POST /api/v1/events.
//
// Auth: an HS256 JWT with op:"event". The publishing stack is taken from
// the token's tenant claim and OVERWRITES any `source` in the body — a
// publisher cannot attribute an event to another publisher, and the
// field is documented as server-filled rather than silently ignored.
//
// `tenants` may be nil, in which case the lazy-registration step below is
// skipped; every other behaviour is unchanged.
func EventsIngest(store events.Store, router *events.Router, routes RouteSource, tenants tenant.Store, now func() time.Time) http.HandlerFunc {
	if now == nil {
		now = time.Now
	}
	return func(w http.ResponseWriter, r *http.Request) {
		claims := ClaimsFromContext(r.Context())
		if claims == nil || claims.Op != "event" {
			http.Error(w, "forbidden: token op must be 'event'", http.StatusForbidden)
			return
		}

		var ev events.Event
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			http.Error(w, "malformed JSON body: "+err.Error(), http.StatusBadRequest)
			return
		}

		ev.Source = claims.Tenant
		if ev.Source == "" {
			http.Error(w, "forbidden: token carries no tenant claim", http.StatusForbidden)
			return
		}

		// Each of these is a 400, not a default. An event with no id has
		// no idempotency key, so a retried publisher would duplicate the
		// alert; an event with no title renders as a blank Telegram
		// message. Both are publisher bugs and both should surface at
		// the publisher, now, rather than as a weird notification later.
		switch {
		case ev.ID == "":
			http.Error(w, "id is required (it is the idempotency key — derive it from the event, not at random)", http.StatusBadRequest)
			return
		case ev.Kind == "":
			http.Error(w, "kind is required", http.StatusBadRequest)
			return
		case ev.Title == "":
			http.Error(w, "title is required", http.StatusBadRequest)
			return
		}

		// An unknown severity is REJECTED rather than accepted. The
		// routing gate ranks an unknown string 0, so it would fail
		// every channel minimum and the event would vanish with a 200 —
		// the exact silent-drop shape that made every `info`
		// notification in forge-notifier invisible for months. A typo
		// in a publisher's severity must be a loud 400.
		if !channel.KnownSeverity(ev.Severity) {
			http.Error(w, "severity must be one of info, warning, error, critical (got "+
				jsonQuote(ev.Severity)+")", http.StatusBadRequest)
			return
		}

		receipt := now().UTC()
		ev.ReceivedAt = receipt
		if ev.At.IsZero() {
			ev.At = receipt
		}

		// SeenEvent records as it checks, so this cannot be done without
		// the record. On a storage error it fails CLOSED (500): the
		// publisher retries, and a retry is cheap because the id is
		// idempotent. Failing open would risk a duplicate storm, which
		// is the failure this component already had once.
		dup, err := store.SeenEvent(r.Context(), ev.ID, receipt)
		if err != nil {
			slog.Error("dedupe check failed", "eventId", ev.ID, "source", ev.Source, "err", err)
			http.Error(w, "dedupe check failed", http.StatusInternalServerError)
			return
		}
		if dup {
			// 200, not 409: a duplicate is the system working. A
			// publisher that treats a non-2xx as "retry" would hammer
			// the endpoint for an event already handled.
			writeJSON(w, http.StatusOK, IngestResponse{Status: "duplicate", EventID: ev.ID})
			return
		}

		// Lazy tenant registration. The publisher's plugin tries to
		// push-register at deploy time, but that call is best-effort by
		// design — on a first provision the notifier's own Lambda does not
		// exist yet, so requiring it would deadlock a stack that declares
		// both the notifier and a publisher. See the call-site comment in
		// plugin/internal/compute.go.
		//
		// This is the path that makes the row appear regardless. The
		// `tenant` claim is already authenticated and is already the
		// authority for `ev.Source` two dozen lines up, so creating a row
		// from it grants nothing the caller did not already have.
		//
		// Bookkeeping NEVER fails an ingest: a publisher that cannot be
		// recorded in the tenant list can still be delivering the alert
		// that matters, and losing the alert to fix the list is the wrong
		// trade. Errors warn.
		ensureTenant(r.Context(), tenants, ev.Source, receipt)

		rules, err := routes.Routes(r.Context())
		if err != nil {
			slog.Error("resolve routes failed", "eventId", ev.ID, "err", err)
			http.Error(w, "resolve routes failed", http.StatusInternalServerError)
			return
		}

		decisions, err := router.Route(r.Context(), &ev, rules, receipt)
		if err != nil {
			slog.Error("routing failed", "eventId", ev.ID, "err", err)
			http.Error(w, "routing failed", http.StatusInternalServerError)
			return
		}

		// Logged at info with the enqueued count, because "the event
		// arrived and matched nothing" is a real operator question and
		// the alternative is inferring it from the absence of a
		// notification.
		enqueued := 0
		for _, d := range decisions {
			if d.Delivered {
				enqueued++
			}
		}
		slog.Info("event accepted",
			"eventId", ev.ID, "source", ev.Source, "kind", ev.Kind,
			"severity", ev.Severity, "subject", ev.Subject, "enqueued", enqueued)

		// 202 Accepted, not 200: delivery happens on the next scheduler
		// tick, so the work is genuinely not done when this returns.
		writeJSON(w, http.StatusAccepted, IngestResponse{
			Status:    "accepted",
			EventID:   ev.ID,
			Decisions: decisions,
		})
	}
}

// ensureTenant records that `stackName` is alive, creating the row if the
// publisher never managed to push-register.
//
// Also refreshes LastSeenAt on an existing row, which is the field that
// makes the SPA's tenant list worth reading: without it a publisher's
// "last seen" is its last DEPLOY, not its last sign of life, and a
// publisher that has gone quiet looks identical to one that is healthy
// and simply has nothing to report.
//
// A soft-deleted tenant that publishes is revived, and says so in the
// log. The alternative — a publisher that is demonstrably alive and
// sending, but invisible in the operator's list — is worse than a row
// that came back.
//
// Every failure path here warns and returns. Nothing in this function may
// affect the response.
func ensureTenant(ctx context.Context, store tenant.Store, stackName string, at time.Time) {
	if store == nil || stackName == "" {
		return
	}

	existing, found, err := store.Get(ctx, stackName)
	if err != nil {
		slog.Warn("tenant lookup failed; event unaffected", "stackName", stackName, "err", err)
		return
	}

	record := existing
	switch {
	case !found:
		record = tenant.Record{StackName: stackName, CreatedAt: at}
		slog.Info("tenant auto-registered from its first event", "stackName", stackName)
	case existing.SoftDeleted:
		record.SoftDeleted = false
		slog.Info("tenant revived: soft-deleted but publishing", "stackName", stackName)
	}
	record.LastSeenAt = at

	if err := store.Upsert(ctx, record); err != nil {
		slog.Warn("tenant upsert failed; event unaffected", "stackName", stackName, "err", err)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// jsonQuote renders a string for an error message, so a severity of ""
// or one containing a newline is visible in the response rather than
// producing a confusingly blank or multi-line error.
func jsonQuote(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		return `"?"`
	}
	return string(b)
}
