// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package api

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/simple-container-com/sc-component-notifier/service/internal/tenant"
)

// TenantsList returns the handler for GET /api/v1/tenants.
//
// Consumed by the embedded SPA. Returns all currently-registered tenants,
// including soft-deleted ones for operator visibility.
//
// Gated on `op: "operator"`, not merely on a valid signature. The
// original comment here said "a signed token is enough for the notifier
// — real components may gate by operator role", which was wrong in a way
// that mattered: every publisher stack holds a valid `event` token, so
// any one tenant could enumerate all of them. See requireOperator.
func TenantsList(store tenant.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireOperator(w, r) {
			return
		}
		rows, err := store.List(r.Context())
		if err != nil {
			slog.Error("tenants list failed", "err", err)
			http.Error(w, "list failed", http.StatusInternalServerError)
			return
		}
		if rows == nil {
			rows = []tenant.Record{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tenants": rows,
			"count":   len(rows),
		})
	}
}
