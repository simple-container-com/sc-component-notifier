// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package api

import (
	"context"
	"log/slog"
	"net/http"

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
func DeliveryTick(drainer Draining) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := ClaimsFromContext(r.Context())
		if claims == nil || claims.Op != "scheduler" {
			http.Error(w, "forbidden: token op must be 'scheduler'", http.StatusForbidden)
			return
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
