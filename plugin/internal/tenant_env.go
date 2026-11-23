// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package notifier

import "strings"

// TenantEnvironmentsKey is the operator config key holding the
// environments whose deploys may register as publishers.
const TenantEnvironmentsKey = "tenantEnvironments"

// EnvMayRegister reports whether a consumer deploying to `env` should
// become a publisher.
//
// WHY THIS EXISTS, and why it matters MORE here than on the dashboard
// component it was first written for. An SC parent stack can be cloned
// into short-lived ISOLATED stacks — the Forge fleet spins one per
// agent-workflow run, and each service gets a stack like
// `forge-conductor--3ec321`. Those blocks inherit the base config with
// a YAML merge key (`<<: *staging-cfg`), and a merge key CANNOT delete
// an inherited entry. So an isolated deploy inherits
// `uses: [… my-notifier]` and there is no way to opt out from the YAML
// side.
//
// On the observatory component the consequence was cosmetic-ish: six
// dead stacks on the dashboard, every one `health: unknown`. Here the
// consequence is that a throwaway stack from a workflow run holds a
// valid event token and ALERTS THE OPERATOR — on Telegram, at 3am,
// about a backup that failed inside a stack destroyed hours ago. An
// alerting path that cries wolf is worse than none, because the next
// real alert is the one that gets ignored.
//
// The filter is an ALLOWLIST rather than a deny-pattern because the
// thing being excluded has no stable shape: a workflow id is a random
// hex string, so any pattern matching it would also match a legitimate
// environment someone names later. The set of environments an operator
// actually wants paged about is small, known, and theirs to state.
//
// An EMPTY list allows everything — the pre-filter behaviour, so adding
// this key is opt-in and no existing deployment changes.
//
// Comparison is exact after trimming. Deliberately NOT prefix-matching:
// `staging` must not admit `staging-ru`, which is a real and separate
// environment in this fleet, and an operator who wants both says both.
//
// A refusal here is NOT the same failure as an unresolvable `notify:`,
// and the two must not be conflated. An unresolvable notifier URL is a
// MISCONFIGURATION and fails the deploy. An excluded environment is a
// DELIBERATE operator choice and skips registration with a log line
// naming the environment and the allowlist — so an operator who adds a
// real environment and forgets to list it learns it from the deploy
// that should have registered it, rather than from the alert that never
// arrives.
func EnvMayRegister(allowed []string, env string) bool {
	if len(allowed) == 0 {
		return true
	}
	env = strings.TrimSpace(env)
	if env == "" {
		return false
	}
	for _, a := range allowed {
		if strings.TrimSpace(a) == env {
			return true
		}
	}
	return false
}

// TenantEnvironmentsFromConfig reads the allowlist off the component's
// operator config.
//
// Tolerates the shapes that reach a plugin: a []string, a single string
// (one environment), and the []any a list becomes after the JSON
// round-trip over the plugin bus — which is the shape it ACTUALLY
// arrives in, the others being defensive.
func TenantEnvironmentsFromConfig(raw map[string]any) []string {
	if raw == nil {
		return nil
	}
	switch v := raw[TenantEnvironmentsKey].(type) {
	case []string:
		return v
	case string:
		if s := strings.TrimSpace(v); s != "" {
			return []string{s}
		}
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, strings.TrimSpace(s))
			}
		}
		return out
	}
	return nil
}
