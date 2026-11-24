// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package main

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	. "github.com/onsi/gomega"

	notifierApi "github.com/simple-container-com/sc-component-notifier/api"
)

// Schema() is this component's published API: the allowlist decides what
// an operator may write in server.yaml, and ComponentSecretEnvMap decides
// what the deployed service receives. Both are plain maps with no
// compiler keeping them honest, and the two ways they go wrong are both
// silent:
//
//   - A secret-env entry whose key is NOT in the allowlist is dead
//     wiring. Core dual-uses ComponentAllowedOverrides as the
//     allowed-top-level-keys list, so the operator's config line is
//     refused at parse time with "unknown or disallowed top-level key"
//     and the binding never fires.
//   - A secret-env entry the SERVICE reads with a bare os.Getenv gets
//     the SecretsManager ARN, not the secret. Nothing errors; the value
//     is simply wrong forever. That one has been live-caught twice in
//     this fleet (signingKey on backup-smoke-1d 2026-11-14,
//     NOTIFIER_OPERATOR_PASSWORD on the first backup.simple-forge.com
//     login 2026-11-18) — see secret_resolver.go.

// coreWellKnownComponentKeys are accepted by core without appearing in
// ComponentAllowedOverrides (pkg/api/component_validation.go).
var coreWellKnownComponentKeys = map[string]bool{
	"deployTo":   true,
	"image":      true,
	"signingKey": true,
	"env":        true,
	"ingress":    true,
}

func notifierSchema(t *testing.T) (allowed map[string]bool, secretEnv map[string]string) {
	t.Helper()
	g := NewWithT(t)

	resp, err := (&notifierPlugin{}).Schema(context.Background(), nil)
	g.Expect(err).NotTo(HaveOccurred())

	allowed = map[string]bool{}
	for _, k := range resp.GetComponentAllowedOverrides()[notifierApi.ResourceType].GetValues() {
		allowed[k] = true
	}
	secretEnv = resp.GetComponentSecretEnvMap()[notifierApi.ResourceType].GetEntries()
	g.Expect(secretEnv).NotTo(BeEmpty(), "the component bound no secrets at all")
	return allowed, secretEnv
}

// TestSecretEnvKeysAreOperatorWritable is the allowlist half.
func TestSecretEnvKeysAreOperatorWritable(t *testing.T) {
	g := NewWithT(t)
	allowed, secretEnv := notifierSchema(t)

	for cfgKey := range secretEnv {
		g.Expect(allowed[cfgKey] || coreWellKnownComponentKeys[cfgKey]).To(BeTrue(),
			"config key %q is bound as a secret env var but is in neither "+
				"ComponentAllowedOverrides nor core's well-known set — an "+
				"operator writing it fails the WHOLE stack at parse time, so "+
				"the binding can never fire", cfgKey)
	}
}

// TestSecretEnvVarNamesAreSane catches a copy-paste that would make two
// config fields fight over one env var, or bind one to "".
func TestSecretEnvVarNamesAreSane(t *testing.T) {
	g := NewWithT(t)
	_, secretEnv := notifierSchema(t)

	seen := map[string]string{}
	for cfgKey, envVar := range secretEnv {
		g.Expect(envVar).NotTo(BeEmpty(), "config key %q binds to an empty env var name", cfgKey)
		g.Expect(envVar).To(MatchRegexp(`^[A-Z][A-Z0-9_]*$`),
			"env var %q (from %q) is not an upper-snake name", envVar, cfgKey)
		if prev, dup := seen[envVar]; dup {
			t.Fatalf("env var %q is bound from both %q and %q — one would silently win", envVar, prev, cfgKey)
		}
		seen[envVar] = cfgKey
	}
}

// TestMigrationTokenIsBoundAsASecret pins the specific wire this change
// added: `migrationToken:` → ADMIN_MIGRATION_TOKEN, as a SECRET rather
// than a plain env var. The migration endpoint is a full write path over
// both stores, so its bearer must not sit in a Lambda's environment at
// rest.
func TestMigrationTokenIsBoundAsASecret(t *testing.T) {
	g := NewWithT(t)
	allowed, secretEnv := notifierSchema(t)

	g.Expect(allowed).To(HaveKeyWithValue("migrationToken", true))
	g.Expect(secretEnv).To(HaveKeyWithValue("migrationToken", "ADMIN_MIGRATION_TOKEN"))
}

// TestSecretEnvVarsAreResolvedByTheService is the service half, and the
// reason it reads source rather than calling code: the binding lives in
// the plugin module and the reader lives in the service module, so no
// ordinary test spans them. A grep is coarse, but the failure it prevents
// is the one that has actually happened — a bound secret the service
// never passes through resolveAWSSecretARN, which logs identically to a
// working deploy.
func TestSecretEnvVarsAreResolvedByTheService(t *testing.T) {
	g := NewWithT(t)
	_, secretEnv := notifierSchema(t)

	path := filepath.Join("..", "..", "..", "service", "cmd", "notifier", "main.go")
	src, err := os.ReadFile(path)
	g.Expect(err).NotTo(HaveOccurred(),
		"cannot read %s — if the service module moved, move this assertion with it "+
			"rather than deleting it", path)

	// Match resolveAWSSecretARN(ctx, "NAME", …) and tolerate the label
	// being built from the env var name rather than literal.
	resolved := map[string]bool{}
	re := regexp.MustCompile(`resolveAWSSecretARN\([^,]+,\s*"([A-Z][A-Z0-9_]*)"`)
	for _, m := range re.FindAllStringSubmatch(string(src), -1) {
		resolved[m[1]] = true
	}
	g.Expect(resolved).NotTo(BeEmpty(), "found no resolveAWSSecretARN call sites in "+path)

	var missing []string
	for cfgKey, envVar := range secretEnv {
		if !resolved[envVar] {
			missing = append(missing, cfgKey+" → "+envVar)
		}
	}
	g.Expect(missing).To(BeEmpty(),
		"%s never passes these bound secrets through resolveAWSSecretARN, so the "+
			"service compares against the SecretsManager ARN instead of the secret: %s",
		path, strings.Join(missing, ", "))
}
