// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package notifier

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/pulumi/pulumi/sdk/v3/go/common/resource"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/samber/lo"

	"github.com/simple-container-com/api/pkg/api"
	"github.com/simple-container-com/api/pkg/api/logger"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
	scpluginv1 "github.com/simple-container-com/api/pkg/plugin/proto/v1"
	"github.com/simple-container-com/api/pkg/plugin/runtime"
)

// These tests cover what Process INJECTS, as opposed to what it registers.
// The distinction is the whole design: wiring is load-bearing and
// hard-fails, registration is bookkeeping and warns.

// noResources is enough Pulumi runtime to get a *sdk.Context. Process
// registers no cloud resources — it mints tokens and writes to the
// collector — so nothing here needs to produce realistic outputs.
type noResources struct{}

func (noResources) Call(a sdk.MockCallArgs) (resource.PropertyMap, error) { return a.Args.Copy(), nil }

func (noResources) NewResource(a sdk.MockResourceArgs) (string, resource.PropertyMap, error) {
	return a.Name, a.Inputs.Copy(), nil
}

// consumerInput is the shape Process receives when `consumerStack` declares
// `uses: [my-notifier]`. `ingress.hostname` is the branch that actually
// fires on the consumer-side path — core populates no dependency outputs
// there (see readServiceURL's doc comment).
func consumerInput(consumerStack, host string) api.ResourceInput {
	return api.ResourceInput{
		Descriptor: &api.ResourceDescriptor{
			Type: "sc-notifier",
			Name: "my-notifier",
			Config: api.Config{Config: map[string]any{
				"signingKey": testSigningKey,
				"ingress":    map[string]any{"hostname": host},
			}},
		},
		StackParams: &api.StackParams{StackName: consumerStack},
	}
}

func newCollector(stack string) pApi.ComputeContextCollector {
	return pApi.NewComputeContextCollector(context.Background(), logger.New(), stack, "staging")
}

// runProcessWith drives Process inside a mocked Pulumi runtime and returns
// the error Process itself produced, separately from any runtime error.
func runProcessWith(t *testing.T, in api.ResourceInput, collector pApi.ComputeContextCollector, cfg ProcessorConfig) error {
	t.Helper()
	var procErr error
	if err := sdk.RunErr(func(ctx *sdk.Context) error {
		_, procErr = NewProcessor(cfg).Process(ctx, api.Stack{}, in, collector, pApi.ProvisionParams{})
		return nil
	}, sdk.WithMocks("project", "stack", noResources{})); err != nil {
		t.Fatalf("pulumi runtime: %v", err)
	}
	return procErr
}

func runProcess(t *testing.T, in api.ResourceInput, collector pApi.ComputeContextCollector) error {
	t.Helper()
	return runProcessWith(t, in, collector, ProcessorConfig{})
}

func envVar(t *testing.T, vars []pApi.ComputeEnvVariable, name string) pApi.ComputeEnvVariable {
	t.Helper()
	v, found := lo.Find(vars, func(v pApi.ComputeEnvVariable) bool { return v.Name == name })
	Expect(found).To(BeTrue(), "env variable %q was never injected", name)
	return v
}

// The injected names ARE the contract: a publisher reads them by literal
// string, and the `SC_` prefix is what makes auto-injection safe — SC core
// refuses an injected name outside that namespace, so a provider plugin
// cannot shadow an application's own NOTIFY_URL. Asserted literally
// because a rename here breaks every publisher silently: they read env and
// find nothing, which looks exactly like "no notifier configured".
func TestProcessInjectsPrefixedWiring(t *testing.T) {
	RegisterTestingT(t)

	c := newCollector("my-backup")
	Expect(runProcess(t, consumerInput("my-backup", "notify.simple-forge.com"), c)).To(Succeed())

	url := envVar(t, c.EnvVariables(), "SC_NOTIFY_URL")
	Expect(url.Value).To(Equal("https://notify.simple-forge.com"))
	Expect(url.Secret).To(BeFalse())

	// The token must land in the SECRET set. A plain env var is stored in
	// the clear on the deployed function; this one lets its holder post
	// events as the publisher until the signing key rotates.
	tok := envVar(t, c.SecretEnvVariables(), "SC_NOTIFY_TOKEN")
	Expect(tok.Secret).To(BeTrue())
	Expect(claimsOf(t, tok.Value)["tenant"]).To(Equal("my-backup"))

	// And neither may appear on the other side of the split.
	Expect(c.EnvVariables()).ToNot(ContainElement(HaveField("Name", "SC_NOTIFY_TOKEN")),
		"the publisher's event token was injected as a PLAIN env var")
}

// D9: the same two values are reachable as `${resource:my-notifier.url}`
// and `.token`, for a consumer whose config needs them somewhere other
// than its env.
func TestProcessPublishesResolvablePlaceholders(t *testing.T) {
	RegisterTestingT(t)

	c := newCollector("my-backup")
	Expect(runProcess(t, consumerInput("my-backup", "notify.simple-forge.com"), c)).To(Succeed())

	cfg := map[string]any{
		"endpoint": "${resource:my-notifier.url}/api/v1/events",
		"token":    "${resource:my-notifier.token}",
	}
	Expect(c.ResolvePlaceholders(&cfg)).To(Succeed())

	Expect(cfg["endpoint"]).To(Equal("https://notify.simple-forge.com/api/v1/events"))
	Expect(cfg["token"]).ToNot(ContainSubstring("${resource:"))
	Expect(claimsOf(t, cfg["token"].(string))["tenant"]).To(Equal("my-backup"))
}

// Hazard 2, and the reason the url/token split is structural rather than
// stylistic. `ResolvePlaceholders` fails OPEN: a name it has no extension
// for is handed back as the LITERAL string. So a consumer that references
// `${resource:my-notifier.token}` without declaring `uses:` does not get
// an error here — it gets the placeholder text deployed as its token, and
// discovers it as a 401 at the first incident.
//
// This test exists to pin that behaviour down as a characterisation, so
// that SC core's refusal of a `uses:`-derived value still containing
// `${resource:` (plan step 6) is understood as load-bearing rather than
// belt-and-braces. If this test ever starts failing because the literal
// no longer survives, the core guard may be redundant — check before
// deleting it.
func TestPlaceholderWithoutUsesFailsOpenToTheLiteral(t *testing.T) {
	RegisterTestingT(t)

	// A collector that no Process call ever touched: exactly the state of
	// a resource that did not declare `uses: [my-notifier]`.
	c := newCollector("my-backup")

	cfg := map[string]any{"token": "${resource:my-notifier.token}"}
	Expect(c.ResolvePlaceholders(&cfg)).To(Succeed(),
		"resolution is expected to SUCCEED here — that is the hazard")
	Expect(cfg["token"]).To(Equal("${resource:my-notifier.token}"),
		"the literal no longer survives; re-check whether SC core's ${resource: refusal is still needed")
}

// Hazard 1 — the one a smoke cannot see. The token is per-consumer
// because its `tenant` claim is, and the ingest handler takes an event's
// `source` from that claim. Two publishers sharing one token means every
// alert from one is attributed to the other, and a channel's `sources:`
// allowlist silently stops meaning what it says.
func TestTokensAreScopedPerConsumer(t *testing.T) {
	RegisterTestingT(t)

	backup := newCollector("my-backup")
	observatory := newCollector("my-observatory")

	Expect(runProcess(t, consumerInput("my-backup", "notify.simple-forge.com"), backup)).To(Succeed())
	Expect(runProcess(t, consumerInput("my-observatory", "notify.simple-forge.com"), observatory)).To(Succeed())

	bTok := envVar(t, backup.SecretEnvVariables(), "SC_NOTIFY_TOKEN").Value
	oTok := envVar(t, observatory.SecretEnvVariables(), "SC_NOTIFY_TOKEN").Value

	Expect(claimsOf(t, bTok)["tenant"]).To(Equal("my-backup"))
	Expect(claimsOf(t, oTok)["tenant"]).To(Equal("my-observatory"))
	Expect(bTok).ToNot(Equal(oTok), "both publishers were handed the same credential")
}

// The same hazard, from the other side: it is the COLLECTOR that has to be
// per-consumer, and both of the collector's write paths silently keep the
// first value. `addEnvVarIfNotExist` is first-write-wins (it logs
// "already exists, skipping") and `AddResourceTplExtension` overwrites by
// resource name — so a shared collector hands consumer #2 consumer #1's
// token, with no error anywhere and nothing a single-publisher smoke can
// detect.
//
// Characterised here so that "each consumer gets a fresh collector" in SC
// core reads as a requirement with a known failure mode behind it, rather
// than a preference.
func TestSharedCollectorLeaksTheFirstConsumersToken(t *testing.T) {
	RegisterTestingT(t)

	shared := newCollector("infra")
	Expect(runProcess(t, consumerInput("my-backup", "notify.simple-forge.com"), shared)).To(Succeed())
	Expect(runProcess(t, consumerInput("my-observatory", "notify.simple-forge.com"), shared)).To(Succeed())

	// One env var, not two: the second consumer's injection was skipped.
	tokens := lo.Filter(shared.SecretEnvVariables(), func(v pApi.ComputeEnvVariable, _ int) bool {
		return v.Name == "SC_NOTIFY_TOKEN"
	})
	Expect(tokens).To(HaveLen(1))
	Expect(claimsOf(t, tokens[0].Value)["tenant"]).To(Equal("my-backup"),
		"first-write-wins no longer holds; re-check whether SC core still needs a per-consumer collector")

	// The tpl extension went the OTHER way — last write wins — so the two
	// surfaces of a shared collector do not even disagree consistently.
	cfg := map[string]any{"token": "${resource:my-notifier.token}"}
	Expect(shared.ResolvePlaceholders(&cfg)).To(Succeed())
	Expect(claimsOf(t, cfg["token"].(string))["tenant"]).To(Equal("my-observatory"),
		"AddResourceTplExtension no longer overwrites; the shared-collector hazard has changed shape")
}

// Hazard 6, plugin half. An unreachable notifier is the NORMAL state on a
// first provision — its Lambda does not exist yet — so registration
// failure must not fail the consumer. Before this, maxAttempts of 500s
// returned an error and took the publisher's deploy with it, which in a
// stack declaring both the notifier and a publisher is a deadlock: the
// provision that would create the notifier is the one that fails.
func TestRegistrationFailureStillInjectsWiring(t *testing.T) {
	RegisterTestingT(t)

	saved := backoffSeconds
	backoffSeconds = []int{0, 0, 0}
	defer func() { backoffSeconds = saved }()

	// TLS, because readServiceURL prepends https:// to a bare hostname —
	// an SC Component is always behind TLS. srv.Client() trusts the
	// generated cert, so the request reaches the handler and fails on the
	// STATUS, exercising doPost and the retry loop rather than a dial
	// error. The hit count below is what keeps this test from passing
	// vacuously if the register call ever stops going out at all.
	var hits int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "https://")

	c := newCollector("my-backup")
	err := runProcessWith(t, consumerInput("my-backup", host), c,
		ProcessorConfig{HTTPClient: srv.Client()})
	Expect(err).To(Succeed(), "a notifier that is not up yet failed the publisher's deploy")
	Expect(atomic.LoadInt32(&hits)).To(Equal(int32(maxAttempts)),
		"the registration call was not actually attempted, so this test proves nothing")

	// Wiring injected regardless: the values are computed locally and
	// nothing about them depended on the call that failed.
	Expect(envVar(t, c.EnvVariables(), "SC_NOTIFY_URL").Value).To(Equal("https://" + host))
	Expect(claimsOf(t, envVar(t, c.SecretEnvVariables(), "SC_NOTIFY_TOKEN").Value)["tenant"]).
		To(Equal("my-backup"))

	cfg := map[string]any{"token": "${resource:my-notifier.token}"}
	Expect(c.ResolvePlaceholders(&cfg)).To(Succeed())
	Expect(cfg["token"]).ToNot(ContainSubstring("${resource:"))
}

// Everything above drives Process against `pApi.Collector` — the
// engine-side implementation. The LIVE path uses a different one:
// `runtime.Collector` inside the plugin subprocess, whose `toResponse`
// marshals the collector's writes into the ComputeProcessResponse proto
// that crosses the plugin bus. A write Process makes to a field that
// implementation does not forward is lost silently, and the deploy log
// reads identically either way — the "resolved but read by nothing" shape.
//
// So this test asserts the seam: drive the real
// `runtime.RunComputeProcess` and read the proto it hands back to SC core.
func TestWiringSurvivesThePluginBus(t *testing.T) {
	RegisterTestingT(t)

	in := consumerInput("my-backup", "notify.simple-forge.com")

	var resp *scpluginv1.ComputeProcessResponse
	if err := sdk.RunErr(func(ctx *sdk.Context) error {
		// WithContextFactory hands RunComputeProcess the mocked Pulumi
		// context instead of dialling a monitor address.
		rctx, err := runtime.NewContext(context.Background(), &scpluginv1.RequestHeader{StackName: "my-backup"},
			runtime.WithContextFactory(func(context.Context, *scpluginv1.RequestHeader) (*sdk.Context, error) {
				return ctx, nil
			}))
		if err != nil {
			return err
		}
		resp, _, err = runtime.RunComputeProcess(
			context.Background(), rctx, api.Stack{Name: "my-backup"}, in,
			pApi.ProvisionParams{Log: logger.New()},
			NewProcessor(ProcessorConfig{}).Process,
		)
		return err
	}, sdk.WithMocks("project", "stack", noResources{})); err != nil {
		t.Fatalf("run compute process: %v", err)
	}

	// Env vars crossed, with the secret flag intact — it is what decides
	// whether the token lands in SecretsManager or in plain function env.
	byName := lo.SliceToMap(resp.GetEnvVars(),
		func(v *scpluginv1.ComputeEnvVariable) (string, *scpluginv1.ComputeEnvVariable) { return v.GetName(), v })
	Expect(byName).To(HaveKey("SC_NOTIFY_URL"))
	Expect(byName).To(HaveKey("SC_NOTIFY_TOKEN"))
	Expect(byName["SC_NOTIFY_URL"].GetSecret()).To(BeFalse())
	Expect(byName["SC_NOTIFY_TOKEN"].GetSecret()).To(BeTrue(),
		"the secret flag did not survive the bus; the token would be stored in the clear")

	// And the tpl extensions crossed as RESOURCE-kind, which is the kind
	// SC core's sidecar routes to AddResourceTplExtension. A
	// DEPENDENCY-kind extension would be applied under a different key and
	// `${resource:my-notifier.token}` would quietly never resolve.
	res, found := lo.Find(resp.GetTplExtensions(), func(e *scpluginv1.TemplateExtension) bool {
		return e.GetKind() == scpluginv1.TemplateExtensionKind_TEMPLATE_EXTENSION_KIND_RESOURCE &&
			e.GetResName() == "my-notifier"
	})
	Expect(found).To(BeTrue(), "no RESOURCE tpl extension for my-notifier crossed the bus — ${resource:} would never resolve")
	Expect(res.GetValues()).To(HaveKeyWithValue("url", "https://notify.simple-forge.com"))
	Expect(res.GetValues()).To(HaveKey("token"))
	Expect(claimsOf(t, res.GetValues()["token"])["tenant"]).To(Equal("my-backup"))
}

// The line registration-warns must not blur: UNRESOLVABLE WIRING STILL
// HARD-FAILS. A publisher with no URL or no signing key can never alert
// anyone, and a notifier that silently does not notify is the worst
// outcome available — strictly worse than a red build, because nobody
// finds out until the incident.
func TestUnresolvableWiringStillFailsTheDeploy(t *testing.T) {
	RegisterTestingT(t)

	noHost := consumerInput("my-backup", "")
	c := newCollector("my-backup")
	err := runProcess(t, noHost, c)
	Expect(err).To(HaveOccurred(), "a publisher with no resolvable notifier URL deployed anyway")
	Expect(err.Error()).To(ContainSubstring("ingress.hostname"))
	Expect(c.EnvVariables()).To(BeEmpty(), "partial wiring was injected before the failure")

	noKey := consumerInput("my-backup", "notify.simple-forge.com")
	delete(noKey.Descriptor.Config.Config.(map[string]any), "signingKey")
	c2 := newCollector("my-backup")
	err = runProcess(t, noKey, c2)
	Expect(err).To(HaveOccurred(), "a publisher with no signingKey deployed anyway")
	Expect(err.Error()).To(ContainSubstring("signingKey"))
	Expect(c2.SecretEnvVariables()).To(BeEmpty(), "a token was injected without a key to sign it")
}
