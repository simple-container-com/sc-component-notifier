// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package notifier

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"

	"github.com/simple-container-com/api/pkg/api"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
)

// configRaw reads the component's opaque config map off the resource
// descriptor, returning an empty map rather than nil so callers can
// index without guarding.
//
// SC core carries Component config as an untyped `.Raw` map because the
// production `sc` binary does not link plugin /api modules — the typed
// NotifierConfig in this repo's api/ package is documentation and
// plugin-side validation only (platform doc §P1 v1.5). Asserting a
// core-defined Go interface against one of these values compiles
// locally and fails in production; see the package doc on
// cmd/sc-component-notifier/main.go.
func configRaw(input api.ResourceInput) map[string]any {
	if raw, ok := extractConfigRaw(input.Descriptor); ok && raw != nil {
		return raw
	}
	// The ComponentRaw side-channel is the other carrier; on the
	// compute-process path SC core packs the component's map there.
	if input.ComponentRaw != nil {
		return input.ComponentRaw
	}
	return map[string]any{}
}

func extractConfigRaw(desc *api.ResourceDescriptor) (map[string]any, bool) {
	if desc == nil || desc.Config.Config == nil {
		return nil, false
	}
	type rawHaver interface{ GetRaw() map[string]any }
	if rh, ok := desc.Config.Config.(rawHaver); ok {
		return rh.GetRaw(), true
	}
	// Tests and local dev hand a plain map.
	if m, ok := desc.Config.Config.(map[string]any); ok {
		return m, true
	}
	return nil, false
}

// readSigningKey pulls the HS256 key the service validates publisher
// tokens with.
//
// Must be the same value the service ends up holding. On AWS the
// service receives it as a SecretsManager ARN and resolves it at
// startup; the plugin receives the resolved secret because SC
// substitutes `${secret:...}` before dispatch. A mismatch here is the
// single most likely cause of a 401 on every registration AND on every
// event afterwards — which is the silent failure this whole component
// exists to prevent, so pushRegister treats 401/403 as non-transient
// and says so.
func (p *Processor) readSigningKey(input api.ResourceInput) (string, error) {
	if v, ok := configRaw(input)["signingKey"].(string); ok && v != "" {
		return v, nil
	}
	return "", errors.New("signingKey not found on the component config — operator must declare `config.signingKey: ${secret:...}`")
}

// readServiceURL resolves the deployed notifier service's base URL.
//
// Cascade, in order of trustworthiness:
//
//  1. `_endpoint_url` / `_deployed_service` from the dependency outputs
//     SC core's Path A sidecar publishes after dispatching the compute
//     template. This is the real, live URL.
//  2. `ingress.hostname` off the component config. Correct once DNS has
//     propagated, and the only option available when SC core has not
//     populated dependency outputs — which it does not do on the
//     consumer-side ComputeProcess path today
//     (api/pkg/clouds/pulumi/provision.go:373). Both existing
//     components hit exactly this and had to add the fallback.
//
// Returns an error naming both options when neither is present, rather
// than defaulting to something that will silently fail to connect. A
// publisher that cannot resolve the notifier URL must fail its deploy:
// deploying it with notifications quietly off is strictly worse than a
// red build, because nobody finds out until the incident.
func (p *Processor) readServiceURL(input api.ResourceInput, params pApi.ProvisionParams) (string, error) {
	// Literals, not the const: the canonical ComponentEndpointURLKey
	// lives in SC core's engine package (pkg/clouds/pulumi), which a
	// plugin must not import — plugins talk to core over the bus, not
	// by linking it. If core ever renames these, the dependency-output
	// lookup silently stops matching and the ingress.hostname fallback
	// below takes over, which is a degraded-but-working outcome rather
	// than a failure.
	for _, key := range []string{"_endpoint_url", "_deployed_service"} {
		if u := lookupDepString(params, key); u != "" {
			return normalizeURL(u), nil
		}
	}
	if ing, ok := configRaw(input)["ingress"].(map[string]any); ok {
		if host, ok := ing["hostname"].(string); ok && host != "" {
			return normalizeURL(host), nil
		}
	}
	return "", errors.New(
		"cannot resolve the notifier service URL: no `_endpoint_url`/`_deployed_service` in the dependency outputs and no `config.ingress.hostname` on the component",
	)
}

// lookupDepString searches the sibling-resource outputs for a string
// value under `key`, across every resource that published one.
//
// The published values are NOT on the `*api.ResourceOutput` itself —
// that type is `struct{ Ref any }` and nothing else. A plugin-owned
// resource leaves an `*api.PluginResourceOutput` on `.Ref`, and the
// values live in its `Outputs map[string]PluginOutputValue`
// (api/pkg/api/plugin_resource_output.go). This function previously
// type-asserted the *ResourceOutput pointer itself against
// `map[string]any` — a struct pointer can never satisfy a map type, so
// the whole first branch of readServiceURL's cascade was unreachable
// and every resolution silently fell through to `ingress.hostname`.
// Inherited from sc-component-observatory, which has the same dead
// branch; found 2026-11-23 while writing config_test.go, which is the
// reason that test exists.
//
// `map[string]any` is still accepted on `.Ref` because an in-tree
// (non-plugin) resource may put its own typed output there, and a plain
// map is what a hand-built test fixture is most likely to carry.
//
// Resource names are visited in sorted order. Go map iteration is
// randomised, so when two dependencies both publish the key an
// unsorted walk picks a different one per run — i.e. a publisher that
// posts events to a different notifier on every provision, with
// nothing in the diff to explain it.
func lookupDepString(params pApi.ProvisionParams, key string) string {
	names := make([]string, 0, len(params.ResourceOutputs))
	for name := range params.ResourceOutputs {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		out := params.ResourceOutputs[name]
		if out == nil || out.Ref == nil {
			continue
		}
		switch ref := out.Ref.(type) {
		case *api.PluginResourceOutput:
			if v, found := ref.Lookup(key); found {
				if s, ok := v.Value.(string); ok && s != "" {
					return s
				}
			}
		case map[string]any:
			if v, ok := ref[key].(string); ok && v != "" {
				return v
			}
		}
	}
	return ""
}

// normalizeURL accepts a bare hostname or a full URL and returns a
// scheme-qualified base with no trailing slash.
//
// Bare hostnames get https:// — an SC Component always sits behind TLS
// (a Lambda Function URL or a registrar-managed record), and defaulting
// to http would produce a redirect-or-fail on every event POST.
//
// Whitespace comes off FIRST: trimming slashes from a value that still
// has a trailing space is a no-op, so `"https://host/ "` kept its
// slash and every event POST went to `//api/v1/events`. Caught by
// TestNormalizeURL on first run, 2026-11-23.
func normalizeURL(s string) string {
	s = strings.TrimRight(strings.TrimSpace(s), "/")
	if s == "" {
		return ""
	}
	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
		return s
	}
	return "https://" + s
}

// SelfURLFromConfig derives the component's own public URL from its
// `ingress.hostname`, for the PreProvision tpl extension that tells the
// deployed service where it lives — and, via
// `${resource:my-notifier.self-url}`, tells a PUBLISHER where to send.
//
// Empty when no ingress is declared. That is legitimate for the
// service's own logs, and it is exactly what makes a publisher's
// `notify: my-notifier` fail loudly rather than resolve to nothing: see
// NotifyTargetFromConfig.
func SelfURLFromConfig(raw map[string]any) string {
	ing, ok := raw["ingress"].(map[string]any)
	if !ok {
		return ""
	}
	host, ok := ing["hostname"].(string)
	if !ok || host == "" {
		return ""
	}
	return normalizeURL(host)
}

// DefaultBucketName is the storage bucket the plugin names when the
// operator names none, keyed to the component + environment so two
// components in one account cannot collide.
func DefaultBucketName(resourceName, env string) string {
	name := fmt.Sprintf("sc-notifier-%s-%s", resourceName, env)
	// S3 bucket names: lowercase, 3–63 chars, no underscores.
	name = strings.ToLower(strings.ReplaceAll(name, "_", "-"))
	if len(name) > 63 {
		name = name[:63]
	}
	return strings.Trim(name, "-")
}

// BucketForStorageRef resolves the operator's `storage:` declaration to
// the physical bucket the deployed service should read and write.
//
// The operator's side of the contract is one word — the NAME of another
// SC resource declared in the same server.yaml:
//
//	sc-notifier-data:
//	  type: s3-bucket
//	  config: { credentials: "${auth:aws}", allowOnlyHttps: true }
//
//	my-notifier:
//	  type: sc-notifier
//	  config: { storage: sc-notifier-data, ... }
//
// No bucket name, no region, no credential. That indirection is the
// point: repointing storage at a different resource is a one-word
// change with no component code involved, which is what keeps the
// component generic across deploy templates.
//
// Why a CONVENTION and not a lookup: the referenced resource is
// provisioned in the same parent-stack update as this component, so at
// PreProvision time it does not exist yet and its outputs cannot be
// read. The sc-plugin-aws s3-bucket resource names its bucket
// `<resourceName>--<env>` when the operator sets no explicit
// `bucketName`, so that is derivable without a lookup — verified
// against a real preview plan on the observatory component rather than
// from reading the other plugin (forge run 44469566225).
//
// Accepted shapes, in order:
//
//  1. `storage: {bucket: "literal-name"}` — explicit, wins over
//     everything. The escape hatch for an s3-bucket resource carrying
//     its own `bucketName`, which the convention cannot see, and for a
//     bucket provisioned outside SC entirely.
//  2. `storage: {name: "resource-name"}` — the reference, map form.
//  3. `storage: "resource-name"` — the reference, the shape an
//     operator actually writes.
//  4. absent — the component names its own bucket. Keeps a
//     zero-config deploy working, and is why `storage:` is optional.
func BucketForStorageRef(raw map[string]any, resourceName, env string) string {
	switch st := raw["storage"].(type) {
	case string:
		if name := strings.TrimSpace(st); name != "" {
			return scResourceBucketName(name, env)
		}
	case map[string]any:
		if b, ok := st["bucket"].(string); ok && strings.TrimSpace(b) != "" {
			return strings.TrimSpace(b)
		}
		if n, ok := st["name"].(string); ok && strings.TrimSpace(n) != "" {
			return scResourceBucketName(strings.TrimSpace(n), env)
		}
	}
	return DefaultBucketName(resourceName, env)
}

// scResourceBucketName mirrors the s3-bucket resource's own naming:
// `<resourceName>--<env>`.
//
// The double hyphen is SC's resource-name/environment separator and is
// not cosmetic — a single one would collide with a resource name that
// itself ends in `-staging`.
func scResourceBucketName(resourceName, env string) string {
	if env == "" {
		return resourceName
	}
	return resourceName + "--" + env
}

// StorageRegionFromConfig defaults to eu-central-1, matching the
// fleet's primary. An operator in another region sets `storage.region`
// once.
func StorageRegionFromConfig(raw map[string]any) string {
	if st, ok := raw["storage"].(map[string]any); ok {
		if r, ok := st["region"].(string); ok && r != "" {
			return r
		}
	}
	return "eu-central-1"
}

// StoragePrefixFromConfig reads the optional key namespace, so two
// components can share one bucket.
//
// Empty is the normal answer and resolves to an empty env value, which
// the service reads as "no prefix" — the tpl extension is emitted
// unconditionally because ComponentDefaults is static JSON and cannot
// include a key only when it has a value.
func StoragePrefixFromConfig(raw map[string]any) string {
	if st, ok := raw["storage"].(map[string]any); ok {
		if p, ok := st["prefix"].(string); ok {
			return strings.Trim(strings.TrimSpace(p), "/")
		}
	}
	return ""
}

// SchedulerTokenFromConfig is the operator-declared credential the
// deploy template's timer presents on the delivery tick.
//
// Returns "" when unset, and the schedule then carries an empty bearer
// and the tick 401s. That is deliberately LOUD rather than silently
// skipping the schedule: a notifier whose delivery queue is never
// drained accepts every event, answers 202, and sends nothing — which
// is indistinguishable from a quiet week. A 401 every minute in the
// service's own log is a thing an operator can find.
func SchedulerTokenFromConfig(raw map[string]any) string {
	if v, ok := raw["schedulerToken"].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

// CooldownFromConfig reads the per-(source, subject, kind) quiet period
// as a Go duration string, for NOTIFIER_COOLDOWN.
//
// Not parsed here. The plugin would have to re-implement the service's
// "0 means off, empty means default" mapping to validate it, and two
// copies of that rule is how they drift; the service parses it once at
// boot and exits 2 with the offending value when it will not parse.
// Passing it through verbatim keeps the error in one place.
func CooldownFromConfig(raw map[string]any) string {
	switch v := raw["cooldown"].(type) {
	case string:
		return strings.TrimSpace(v)
	case int:
		// A bare number in YAML is seconds — `cooldown: 900`.
		return fmt.Sprintf("%ds", v)
	case float64:
		// What an int becomes after the plugin bus's JSON round-trip.
		return fmt.Sprintf("%ds", int64(v))
	}
	return ""
}
