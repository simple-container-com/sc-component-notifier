// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package api

import "net/http"

// Healthz returns 200 and the build serving the request.
// Unauthenticated per sc-components-platform design doc §P3 — the smoke
// needs a probe that doesn't depend on the JWT path, and k8s / ECS /
// Lambda health checks typically don't carry a Bearer.
//
// `version` is here because a pin bump was otherwise unverifiable from
// outside: the binary logged its build once at boot and exposed it
// nowhere, and reading that log needs AWS credentials the person
// smoking a deploy usually does not have. So "the provision went green"
// was the only available evidence that a new image was serving — which
// is evidence that the apply ran, not that this process is the build
// you merged.
//
// Unauthenticated on purpose, and that is a correction. This was first
// written gated behind an operator session, on the reasoning that the
// value is an exact commit SHA of a private repo. The reasoning does
// not hold: the build string is CalVer `vYYYY.MM.DD-<short-sha>`, so it
// ALREADY contains the SHA — gating it bought nothing a public
// `version` does not also give away, while costing the one thing the
// field exists for, a smoke that needs no credentials.
// sc-component-observatory has published exactly this at
// `monitor.simple-forge.com/healthz` since v2026.11.23, which settles
// the precedent.
//
// What is deliberately NOT here is observatory's second field, the full
// 40-char `commit`. It is redundant with the short SHA in `version` and
// is the only part that discloses something the CalVer does not.
func Healthz(version string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"status":  "ok",
			"version": version,
		})
	}
}
