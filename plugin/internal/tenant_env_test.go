// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package notifier

import (
	"encoding/json"
	"testing"

	. "github.com/onsi/gomega"
)

func TestEmptyAllowlistAllowsEveryEnvironment(t *testing.T) {
	RegisterTestingT(t)

	// Opt-in by construction: adding the key changes behaviour, omitting
	// it changes nothing. An operator who upgrades the plugin must not
	// lose registrations they never asked to filter.
	Expect(EnvMayRegister(nil, "staging")).To(BeTrue())
	Expect(EnvMayRegister([]string{}, "anything")).To(BeTrue())
	Expect(EnvMayRegister(nil, "")).To(BeTrue(), "no filter means no opinion, even about a blank env")
}

func TestAllowlistMatchesExactlyNotByPrefix(t *testing.T) {
	RegisterTestingT(t)

	allowed := []string{"staging", "prod"}
	Expect(EnvMayRegister(allowed, "staging")).To(BeTrue())
	Expect(EnvMayRegister(allowed, "prod")).To(BeTrue())

	// `staging-ru` is a real, separate environment in this fleet. A
	// prefix match would silently admit it on an operator's `staging`
	// entry, which is the opposite of an allowlist.
	Expect(EnvMayRegister(allowed, "staging-ru")).To(BeFalse())
	Expect(EnvMayRegister(allowed, "stag")).To(BeFalse())

	// The case this exists for: an isolated stack from an agent-workflow
	// run, which inherits `uses:` through a YAML merge key and cannot
	// opt out from the YAML side. Registering it means paging the
	// operator about a stack that no longer exists.
	Expect(EnvMayRegister(allowed, "3ec321")).To(BeFalse())

	// A blank env cannot pass a non-empty allowlist. Refusing is the
	// safe direction: the cost of a missed registration is one log line
	// the operator can find, the cost of a wrong one is a 3am page.
	Expect(EnvMayRegister(allowed, "")).To(BeFalse())
}

func TestAllowlistIgnoresSurroundingWhitespace(t *testing.T) {
	RegisterTestingT(t)

	Expect(EnvMayRegister([]string{" staging ", "prod"}, "staging")).To(BeTrue())
	Expect(EnvMayRegister([]string{"staging"}, "  staging")).To(BeTrue())
}

func TestTenantEnvironmentsFromConfigReadsTheShapeTheBusDelivers(t *testing.T) {
	RegisterTestingT(t)

	// []any is what a YAML list becomes after the plugin bus's JSON
	// round-trip, so this is the shape that matters in production; the
	// other two are defensive and tested so a refactor cannot drop them
	// while the []any path keeps passing.
	var decoded map[string]any
	Expect(json.Unmarshal([]byte(`{"tenantEnvironments":["staging"," prod "]}`), &decoded)).To(Succeed())
	Expect(TenantEnvironmentsFromConfig(decoded)).To(Equal([]string{"staging", "prod"}))

	Expect(TenantEnvironmentsFromConfig(map[string]any{
		"tenantEnvironments": []string{"staging"},
	})).To(Equal([]string{"staging"}))

	Expect(TenantEnvironmentsFromConfig(map[string]any{
		"tenantEnvironments": "staging",
	})).To(Equal([]string{"staging"}), "a single environment may be written as a bare string")

	Expect(TenantEnvironmentsFromConfig(nil)).To(BeEmpty())
	Expect(TenantEnvironmentsFromConfig(map[string]any{})).To(BeEmpty())
	Expect(TenantEnvironmentsFromConfig(map[string]any{"tenantEnvironments": " "})).To(BeEmpty(),
		"a whitespace-only value is not a filter — it must not lock every environment out")
}

// A list whose entries are not strings must not silently become a filter
// that excludes everything: that would turn one typo into "no publisher
// anywhere can register", with no alert and no error.
func TestNonStringEntriesAreSkippedNotFatal(t *testing.T) {
	RegisterTestingT(t)

	var decoded map[string]any
	Expect(json.Unmarshal([]byte(`{"tenantEnvironments":["staging",42,null,""]}`), &decoded)).To(Succeed())
	Expect(TenantEnvironmentsFromConfig(decoded)).To(Equal([]string{"staging"}))
}
