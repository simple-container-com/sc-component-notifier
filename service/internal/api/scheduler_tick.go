// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/simple-container-com/sc-component-notifier/service/internal/events"
)

// Draining is the delivery drain, as this package needs it. Satisfied by
// *events.Drainer.
type Draining interface {
	Drain(ctx context.Context) (events.DrainResult, error)
}

// DeliveryTick returns the handler for POST /api/v1/scheduler/delivery/tick.
//
// This endpoint is the notifier's clock. There is no ticker anywhere in
// this service and there must never be one: a Lambda FREEZES background
// goroutines the instant the handler returns, so a `time.Ticker` started
// in main() would fire once during the cold-start request and then never
// again — while the service kept reporting healthy and delivering
// nothing. Every periodic job in this fleet is therefore an external
// schedule (`cloudExtras.lambdaSchedules`, `rate(1 minute)`) hitting an
// endpoint that does the work INLINE.
//
// Auth: an HS256 JWT with op:"scheduler", minted by the plugin and
// embedded in the schedule's request body. It is a separate op from
// "event" so a publisher's token cannot drive the drain, and from
// "register" so a tenant's cannot either.
//
// The response carries the full DrainResult, which matters more than it
// looks: `{"skipped": true}` is the NORMAL answer whenever a previous
// drain still holds the single-flight lease, and an operator reading
// CloudWatch needs to be able to tell that apart from an error.
//
// replicationTick drives go-embedb's five background replication loops
// once, synchronously; nil on any deploy not using the embed. The comment
// above about never having a ticker applies to go-embedb's internals too,
// and that is the whole reason this parameter exists: the engine starts
// five wall-clock-ticker goroutines (slot heartbeat, lease renewal, reader
// reconcile, snapshot, GC sweeper) at handler construction, and on Lambda
// a loop fires only if ONE invocation outlives its period. Measured across
// the fleet 2026-11-25: slots stuck at `heartbeat == joinedAt` to the
// nanosecond — never renewed since joining — while the Lambda served
// traffic normally. This endpoint is already the notifier's clock, so it
// is also the engine's.
//
// See forge/docs/roadmap/items/go-embedb-snapshot-ticker-starves-on-lambda.md.
func DeliveryTick(drainer Draining, replicationTick func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := ClaimsFromContext(r.Context())
		if claims == nil || claims.Op != "scheduler" {
			http.Error(w, "forbidden: token op must be 'scheduler'", http.StatusForbidden)
			return
		}

		// Never fatal, and bounded independently of the drain.
		//
		// Not fatal because a replication error is retried on the next
		// tick, whereas 500-ing here would stop DELIVERY — the thing
		// this endpoint exists for — over a transient S3 error.
		// Bounded because a snapshot plus a GC sweep on a cold bucket
		// can outlast the Lambda timeout, and the drain must still get
		// its share of the invocation.
		if replicationTick != nil {
			repCtx, repCancel := context.WithTimeout(r.Context(), 20*time.Second)
			if err := replicationTick(repCtx); err != nil {
				slog.WarnContext(repCtx, "delivery tick: replication tick failed", "err", err)
			}
			repCancel()
		}

		res, err := drainer.Drain(r.Context())
		if err != nil {
			// 500 so a failing drain shows up in the schedule's own
			// error metric. A swallowed 200 here would make a notifier
			// that has stopped delivering look perfectly healthy.
			slog.Error("delivery drain failed", "err", err)
			http.Error(w, "drain failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		if res.Skipped {
			slog.Info("delivery tick skipped (lease held by another drain)")
		} else {
			slog.Info("delivery tick",
				"examined", res.Examined, "delivered", res.Delivered,
				"failed", res.Failed, "gaveUp", res.GaveUp)
		}

		writeJSON(w, http.StatusOK, res)
	}
}
