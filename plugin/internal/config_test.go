// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package notifier

import (
	"testing"

	. "github.com/onsi/gomega"

	"github.com/simple-container-com/api/pkg/api"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
)

// inputWith builds the ResourceInput shape the compute-process path
// actually receives: the component's config map, carried untyped.
func inputWith(raw map[string]any) api.ResourceInput {
	return api.ResourceInput{
		Descriptor: &api.ResourceDescriptor{
			Type:   "sc-notifier",
			Name:   "my-notifier",
			Config: api.Config{Config: raw},
		},
	}
}

// depOutputs builds dependency outputs the way SC core's
// DependencyOutputsFromComputeProcessRequest builds them
// (api/pkg/plugin/runtime/compute.go:125) — a *ResourceOutput whose
// only field is Ref, holding a *PluginResourceOutput. Constructing a
// fixture any other way is what let the dead `map[string]any` assertion
// in lookupDepString survive unnoticed in two components.
func depOutputs(resource string, values map[string]any) pApi.ResourcesOutputs {
	outs := map[string]api.PluginOutputValue{}
	for k, v := range values {
		outs[k] = api.PluginOutputValue{Value: v}
	}
	return pApi.ResourcesOutputs{
		resource: &api.ResourceOutput{
			Ref: &api.PluginResourceOutput{
				PluginName: "sc-plugin-aws",
				Urn:        "urn:pulumi:test::forge::" + resource,
				Outputs:    outs,
			},
		},
	}
}

func TestConfigRawReadsBothCarriers(t *testing.T) {
	RegisterTestingT(t)

	Expect(configRaw(inputWith(map[string]any{"signingKey": "k"}))).
		To(HaveKeyWithValue("signingKey", "k"))

	// The ComponentRaw side-channel: the descriptor carries no config at
	// all on the dispatched-template path.
	Expect(configRaw(api.ResourceInput{ComponentRaw: map[string]any{"cooldown": "5m"}})).
		To(HaveKeyWithValue("cooldown", "5m"))

	// Never nil — every caller indexes the result without guarding.
	Expect(configRaw(api.ResourceInput{})).ToNot(BeNil())
}

// The first branch of readServiceURL's cascade, proven rather than
// assumed. It was structurally unreachable until 2026-11-23: the old
// lookupDepString asserted the *ResourceOutput POINTER against
// map[string]any, which a struct pointer can never satisfy, so a live
// `_endpoint_url` was ignored and `ingress.hostname` always won.
func TestDependencyEndpointURLWinsOverIngressHostname(t *testing.T) {
	RegisterTestingT(t)

	in := inputWith(map[string]any{
		"ingress": map[string]any{"hostname": "notify.simple-forge.com"},
	})
	params := pApi.ProvisionParams{
		ResourceOutputs: depOutputs("my-notifier", map[string]any{
			"_endpoint_url": "https://abc123.lambda-url.eu-central-1.on.aws/",
		}),
	}

	p := NewProcessor(ProcessorConfig{})
	url, err := p.readServiceURL(in, params)
	Expect(err).ToNot(HaveOccurred())
	Expect(url).To(Equal("https://abc123.lambda-url.eu-central-1.on.aws"),
		"the live endpoint must beat the DNS name, and the trailing slash must be stripped")
}

func TestDependencyEndpointFallsBackToDeployedService(t *testing.T) {
	RegisterTestingT(t)

	p := NewProcessor(ProcessorConfig{})
	url, err := p.readServiceURL(inputWith(map[string]any{}), pApi.ProvisionParams{
		ResourceOutputs: depOutputs("my-notifier", map[string]any{
			"_deployed_service": "abc123.lambda-url.eu-central-1.on.aws",
		}),
	})
	Expect(err).ToNot(HaveOccurred())
	Expect(url).To(Equal("https://abc123.lambda-url.eu-central-1.on.aws"),
		"a bare hostname must acquire https:// — an SC Component is always behind TLS")
}

func TestIngressHostnameIsTheFallbackNotTheOnlyPath(t *testing.T) {
	RegisterTestingT(t)

	p := NewProcessor(ProcessorConfig{})
	url, err := p.readServiceURL(inputWith(map[string]any{
		"ingress": map[string]any{"hostname": "notify.simple-forge.com"},
	}), pApi.ProvisionParams{})
	Expect(err).ToNot(HaveOccurred())
	Expect(url).To(Equal("https://notify.simple-forge.com"))
}

// A publisher that cannot resolve the notifier URL must fail its
// deploy. Deploying it with notifications quietly off is strictly worse
// than a red build: nobody finds out until the incident. The message
// has to name BOTH options, because the operator's fix is to supply one
// of them and the plugin source is not something they can grep.
func TestUnresolvableServiceURLFailsAndNamesBothOptions(t *testing.T) {
	RegisterTestingT(t)

	p := NewProcessor(ProcessorConfig{})
	cases := []struct {
		name string
		raw  map[string]any
	}{
		{"no config at all", map[string]any{}},
		{"ingress present but empty", map[string]any{"ingress": map[string]any{}}},
		{"hostname present but blank", map[string]any{"ingress": map[string]any{"hostname": ""}}},
	}
	for _, tc := range cases {
		_, err := p.readServiceURL(inputWith(tc.raw), pApi.ProvisionParams{})
		Expect(err).To(HaveOccurred(), tc.name)
		Expect(err.Error()).To(ContainSubstring("_endpoint_url"), tc.name)
		Expect(err.Error()).To(ContainSubstring("ingress.hostname"), tc.name)
	}

	// An output published under the right key but with a non-string
	// value is not a URL. Falling through to the error is correct;
	// rendering "%!s(int=8080)" into a POST target is not.
	_, err := p.readServiceURL(inputWith(map[string]any{}), pApi.ProvisionParams{
		ResourceOutputs: depOutputs("my-notifier", map[string]any{"_endpoint_url": 8080}),
	})
	Expect(err).To(HaveOccurred())
}

func TestSigningKeyIsRequired(t *testing.T) {
	RegisterTestingT(t)

	p := NewProcessor(ProcessorConfig{})

	key, err := p.readSigningKey(inputWith(map[string]any{"signingKey": "a-32-byte-or-longer-signing-key-value"}))
	Expect(err).ToNot(HaveOccurred())
	Expect(key).To(Equal("a-32-byte-or-longer-signing-key-value"))

	// An absent OR blank key must fail here rather than mint a token
	// signed with "" — that token validates against a service whose own
	// key is also blank, which is an open ingest endpoint.
	for _, raw := range []map[string]any{
		{},
		{"signingKey": ""},
		{"signingKey": 42},
	} {
		_, err := p.readSigningKey(inputWith(raw))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("config.signingKey"),
			"the error must tell the operator the exact key to declare")
	}
}

func TestNormalizeURL(t *testing.T) {
	RegisterTestingT(t)

	Expect(normalizeURL("notify.example.com")).To(Equal("https://notify.example.com"))
	Expect(normalizeURL("https://notify.example.com/")).To(Equal("https://notify.example.com"))
	Expect(normalizeURL("  https://notify.example.com///  ")).To(Equal("https://notify.example.com"))
	Expect(normalizeURL("http://localhost:8080")).To(Equal("http://localhost:8080"),
		"an explicit http:// must survive — it is how a local smoke runs")
	Expect(normalizeURL("")).To(BeEmpty())
	Expect(normalizeURL("   ")).To(BeEmpty())
}

func TestSelfURLFromConfig(t *testing.T) {
	RegisterTestingT(t)

	Expect(SelfURLFromConfig(map[string]any{
		"ingress": map[string]any{"hostname": "notify.simple-forge.com"},
	})).To(Equal("https://notify.simple-forge.com"))

	// Empty is legitimate and load-bearing: it is what makes a
	// publisher's `notify: my-notifier` fail loudly instead of resolving
	// to a URL that goes nowhere.
	Expect(SelfURLFromConfig(map[string]any{})).To(BeEmpty())
	Expect(SelfURLFromConfig(map[string]any{"ingress": map[string]any{}})).To(BeEmpty())
	Expect(SelfURLFromConfig(map[string]any{"ingress": "notify.example.com"})).To(BeEmpty())
}

func TestBucketForStorageRefFourShapes(t *testing.T) {
	RegisterTestingT(t)

	// 1. explicit bucket wins over everything.
	Expect(BucketForStorageRef(map[string]any{
		"storage": map[string]any{"bucket": "my-own-bucket", "name": "ignored-ref"},
	}, "my-notifier", "staging")).To(Equal("my-own-bucket"))

	// 2. the reference, map form.
	Expect(BucketForStorageRef(map[string]any{
		"storage": map[string]any{"name": "sc-notifier-data"},
	}, "my-notifier", "staging")).To(Equal("sc-notifier-data--staging"))

	// 3. the reference, the shape an operator actually writes.
	Expect(BucketForStorageRef(map[string]any{
		"storage": "sc-notifier-data",
	}, "my-notifier", "staging")).To(Equal("sc-notifier-data--staging"))

	// 4. absent — the component names its own bucket, so a zero-config
	// deploy still works.
	Expect(BucketForStorageRef(map[string]any{}, "my-notifier", "staging")).
		To(Equal("sc-notifier-my-notifier-staging"))

	// A blank reference is not a reference.
	Expect(BucketForStorageRef(map[string]any{"storage": "  "}, "my-notifier", "staging")).
		To(Equal("sc-notifier-my-notifier-staging"))
}

// The separator is `--`, mirroring the sc-plugin-aws s3-bucket
// resource's own naming (verified against a real preview plan, forge
// run 44469566225). A single hyphen would collide with a resource name
// that itself ends in `-staging`, and the component would then read and
// write a bucket belonging to something else.
func TestResourceBucketNameUsesTheDoubleHyphenSeparator(t *testing.T) {
	RegisterTestingT(t)

	Expect(scResourceBucketName("sc-notifier-data", "staging")).To(Equal("sc-notifier-data--staging"))
	Expect(scResourceBucketName("sc-notifier-data-staging", "")).To(Equal("sc-notifier-data-staging"),
		"with no environment the resource name is the bucket name, unsuffixed")
	Expect(scResourceBucketName("data", "staging")).ToNot(Equal("data-staging"))
}

func TestDefaultBucketNameObeysS3Rules(t *testing.T) {
	RegisterTestingT(t)

	Expect(DefaultBucketName("My_Notifier", "Staging")).To(Equal("sc-notifier-my-notifier-staging"),
		"S3 bucket names are lowercase and carry no underscores")

	long := DefaultBucketName("a-very-long-component-name-that-an-operator-might-plausibly-write-out", "staging")
	Expect(len(long)).To(BeNumerically("<=", 63))
	Expect(long).ToNot(HaveSuffix("-"), "a truncation that lands on a hyphen is an invalid bucket name")
}

func TestStorageRegionAndPrefix(t *testing.T) {
	RegisterTestingT(t)

	Expect(StorageRegionFromConfig(map[string]any{
		"storage": map[string]any{"region": "us-east-1"},
	})).To(Equal("us-east-1"))
	Expect(StorageRegionFromConfig(map[string]any{})).To(Equal("eu-central-1"))
	Expect(StorageRegionFromConfig(map[string]any{"storage": "sc-notifier-data"})).To(Equal("eu-central-1"),
		"the string form of `storage:` carries no region, so the default applies")

	Expect(StoragePrefixFromConfig(map[string]any{
		"storage": map[string]any{"prefix": "/notifier/"},
	})).To(Equal("notifier"), "leading and trailing slashes are stripped so keys cannot double up")
	Expect(StoragePrefixFromConfig(map[string]any{})).To(BeEmpty())
}

func TestSchedulerTokenFromConfig(t *testing.T) {
	RegisterTestingT(t)

	Expect(SchedulerTokenFromConfig(map[string]any{"schedulerToken": " tok "})).To(Equal("tok"))
	Expect(SchedulerTokenFromConfig(map[string]any{})).To(BeEmpty())
}

// `cooldown: 900` is a bare YAML number, which reaches the plugin as a
// float64 after the bus's JSON round-trip. Rendering that with %v would
// produce "900" (no unit) or "9e+02", and the service's duration parse
// then exits 2 at boot — i.e. the notifier never starts, which is the
// failure this component exists to avoid.
func TestCooldownFromConfigRendersADurationStringFromEveryYAMLShape(t *testing.T) {
	RegisterTestingT(t)

	Expect(CooldownFromConfig(map[string]any{"cooldown": "15m"})).To(Equal("15m"))
	Expect(CooldownFromConfig(map[string]any{"cooldown": " 15m "})).To(Equal("15m"))
	Expect(CooldownFromConfig(map[string]any{"cooldown": 900})).To(Equal("900s"))
	Expect(CooldownFromConfig(map[string]any{"cooldown": float64(900)})).To(Equal("900s"))

	// Absent means "service default", and `0` means "off" — both are
	// decided by the service, which is why neither is resolved here.
	Expect(CooldownFromConfig(map[string]any{})).To(BeEmpty())
	Expect(CooldownFromConfig(map[string]any{"cooldown": 0})).To(Equal("0s"))
}
