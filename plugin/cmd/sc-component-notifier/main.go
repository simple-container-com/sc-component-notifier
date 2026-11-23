// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// Command sc-component-notifier is the SC plugin half of the
// sc-notifier Component — the fleet's alerting egress.
//
// It implements five RPCs that matter:
//
//	Schema          — declares the resource type, the operator-config
//	                  allowlist, plugin-side defaults, and which config
//	                  fields become per-function secrets.
//	PreProvision    — lands derived values (self-URL, storage bucket,
//	                  the rendered channel set) as tpl extensions BEFORE
//	                  the compute template is dispatched, so the deployed
//	                  service receives them as env without the operator
//	                  writing any.
//	ComputeProcess  — fires per publisher that lists this component in
//	                  `uses:`; push-registers the deploy and injects the
//	                  publisher's NOTIFY_URL + NOTIFY_TOKEN.
//	PreDestroy      — best-effort cleanup when the component itself is
//	                  torn down.
//	LogStream       — drains this plugin's progress lines into the deploy
//	                  log. Without it a refused channel is invisible.
//
// Provision is a near-no-op: SC core's Path A sidecar dispatches the
// operator-chosen compute template (aws-lambda, etc.) BEFORE this
// plugin's Provision runs, so the service already exists by then.
//
// # Two things called "the notifier"
//
// THIS is the public, MIT-licensed SC Component: an operator declares it
// in `server.yaml`, other components `uses:` it, and it fans operational
// events out to Telegram and webhooks. `forge-notifier` is a separate,
// closed-source, Forge-fleet-internal transactional service. They share
// no code and no signing key, and a token minted for one is rejected by
// the other.
//
// # The trap this plugin must never fall into
//
// Never type-assert a core-defined Go interface against a plugin-owned
// config value. The production `sc` binary does NOT link this plugin's
// /api module; plugin-owned values arrive at core as opaque
// *api.PluginFieldConfig with .Raw carrying the operator's YAML.
// Dispatch is Schema-advertised ComponentResourceTypes + a `.Raw`
// reader (plugin/internal/config.go), not Go-interface assertion.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/hashicorp/go-hclog"
	hplugin "github.com/hashicorp/go-plugin"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"

	"github.com/simple-container-com/api/pkg/api"
	"github.com/simple-container-com/api/pkg/api/logger"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
	scPlugin "github.com/simple-container-com/api/pkg/plugin"
	scpluginv1 "github.com/simple-container-com/api/pkg/plugin/proto/v1"
	"github.com/simple-container-com/api/pkg/plugin/runtime"

	notifierApi "github.com/simple-container-com/sc-component-notifier/api"
	notifier "github.com/simple-container-com/sc-component-notifier/plugin/internal"
)

// version is stamped by the release workflow via -ldflags.
var version = "dev"

type notifierPlugin struct {
	scpluginv1.UnimplementedSCCloudPluginServer
	progress *scPlugin.Publisher
	broker   *hplugin.GRPCBroker
}

// SetBroker implements scPlugin.BrokerAware.
func (p *notifierPlugin) SetBroker(b *hplugin.GRPCBroker) { p.broker = b }

// componentDefaultsJSON is merged UNDER the operator's config at
// dispatch time — operator wins per key.
//
// Every entry here is a line an operator would otherwise write by hand,
// and two of them are measurements rather than guesses:
//
//   - maxMemory 512 / timeout 30. NOT SC's 128 default and not the 2048
//     sc-component-backup over-corrected to after OOMing: observatory
//     measured 97–104 MB steady state for the same Go-binary-plus-
//     embedded-SPA shape, and this service's working set is smaller
//     still (no dump buffering, no log queries — the largest thing it
//     holds is one outbound message body).
//   - lambdaRoutingType function-url. The APIGW path mounts the lambda
//     under a stage named "api", so every request arrives path-prefixed
//     and a SPA serving its own routes answers 404 for all of them —
//     the stage-prefix doubling that cost sc-component-backup a deploy
//     cycle.
var componentDefaultsJSON = func() []byte {
	b, err := json.Marshal(map[string]any{
		"deployTo":  "lambda",
		"maxMemory": 512,
		"timeout":   30,
		"storage":   map[string]any{"type": "s3"},
		// The permissions this component needs, declared BY THE
		// COMPONENT and granted BY THE TEMPLATE, in the template's own
		// vocabulary. The component must not know whether the chosen
		// cloud expresses that as an IAM policy, a folder role or a
		// ClusterRole — that is what keeps it portable.
		//
		// This set is deliberately TINY compared with observatory's.
		// A notifier reads no logs, no metrics and no infrastructure:
		// it accepts events over HTTP, keeps a queue in its own bucket,
		// and makes outbound HTTPS calls. Outbound HTTPS needs no IAM at
		// all, and `secretsmanager:GetSecretValue` is already on the
		// execution role the aws-lambda template builds
		// (sc-plugin-aws plugin/internal/aws_lambda.go:272-295), which
		// is what lets a channel credential be an operator-supplied
		// SecretsManager ARN pointing at a secret this component never
		// created.
		"cloudExtras": map[string]any{
			"lambdaRoutingType": "function-url",
			// NO lambdaSchedules HERE, and the delivery queue does not
			// work without one. The reason is a resolution gap, not an
			// oversight:
			//
			// Lambda freezes background goroutines the instant the
			// handler returns, so the delivery drain cannot be a ticker
			// inside the process. It is POST
			// /api/v1/scheduler/delivery/tick, which runs the drain
			// INLINE when called — and something external has to call
			// it. On aws-lambda that is an EventBridge rule built from
			// `cloudExtras.lambdaSchedules`.
			//
			// That rule's input must carry a bearer token, and SC
			// resolves `${secret:NAME}` inside a schedule's request
			// payload but NOT the `$$sc:${resource:<name>.key}`
			// tpl-extension form. Live-proven on observatory
			// 2026-11-19: the rule was created correctly, fired on
			// time, and the tick answered 401 twice because its input
			// held the literal string
			// `Bearer $$sc:${resource:${name}.scheduler-token}`.
			//
			// A generic component cannot hardcode one stack's secret
			// NAME, and the only form that resolves there needs exactly
			// that. So the schedule block lives in the operator's
			// server.yaml — the README spells it out verbatim,
			// including the `name:` <= 14 character limit, which is
			// rejected at APPLY after preview passes — and
			// `schedulerToken` stays on the allowlist so the credential
			// is declared once.
			//
			// Shipping a non-resolving schedule in these defaults would
			// be WORSE than shipping none: a rule that fires every
			// minute and 401s reads as "the notifier is broken" rather
			// than "configure the token".
			"awsRoles": []string{
				// The component's own bucket: the event log, the
				// delivery queue, the dedup keys, the tick lease.
				"s3:PutObject", "s3:GetObject", "s3:ListBucket",
				"s3:DeleteObject", "s3:AbortMultipartUpload",
			},
		},
		"env": map[string]any{
			// Where the service lives, derived from ingress.hostname at
			// PreProvision. Used in its own logs and as the base for the
			// deep links it puts in outbound messages — a Telegram alert
			// whose "open the dashboard" link is missing is half an
			// alert.
			"NOTIFIER_SELF_URL": "$$sc:${resource:${name}.self-url}",
			// Storage wiring. STORAGE_* rather than NOTIFIER_S3_*: the
			// generic storage-block → env translation in sc-plugin-aws
			// already emits these names, and sc-component-backup lost a
			// deploy cycle to exactly that mismatch (its service
			// demanded BACKUP_S3_BUCKET while the platform emitted
			// STORAGE_BUCKET, and the dispatch silently fell back to a
			// broken path).
			"STORAGE_BUCKET": "$$sc:${resource:${name}.storage-bucket}",
			"STORAGE_REGION": "$$sc:${resource:${name}.storage-region}",
			"STORAGE_PREFIX": "$$sc:${resource:${name}.storage-prefix}",
			// The operator's declarative channel set, rendered and
			// validated at PreProvision. See channelsJSON for why a
			// bad channel fails the deploy instead of arriving here.
			"NOTIFIER_CHANNELS": "$$sc:${resource:${name}.channels-json}",
			// Per-(source, subject, kind) quiet period. Passed through
			// verbatim; the service parses it once at boot and exits 2
			// naming the offending value.
			"NOTIFIER_COOLDOWN": "$$sc:${resource:${name}.cooldown}",
		},
	})
	if err != nil {
		// A bad defaults payload would otherwise cascade into every
		// Schema() RPC; fail at load instead.
		panic("sc-component-notifier: componentDefaultsJSON: " + err.Error())
	}
	return b
}()

func (p *notifierPlugin) Schema(_ context.Context, _ *scpluginv1.SchemaRequest) (*scpluginv1.SchemaResponse, error) {
	return &scpluginv1.SchemaResponse{
		PluginVersion:          "sc-component-notifier-" + version,
		ResourceTypes:          []string{notifierApi.ResourceType},
		ComponentResourceTypes: []string{notifierApi.ResourceType},
		ComputeProcessorTypes:  []string{notifierApi.ResourceType},
		PulumiPlugins:          []*scpluginv1.PulumiPluginRequirement{},
		VmDrivers:              []*scpluginv1.VmDriverSpec{},

		ComponentDefaults: map[string][]byte{
			notifierApi.ResourceType: componentDefaultsJSON,
		},

		// PreProvision must run BEFORE the compute template is
		// dispatched so its tpl extensions are available for the
		// `${resource:<name>.*}` placeholders in ComponentDefaults
		// above. Without this flag SC core never dials PreProvision and
		// the deployed service receives literal unresolved tokens in
		// its env — which looks like a working deploy right up until
		// the service cannot find its bucket, or routes every event to
		// a channel set that is the string "$$sc:...".
		SupportsPreProvision: true,

		// Operator-config allowlist. SC core dual-uses this as the
		// allowed-top-level-keys list for the component config
		// (pkg/api/component_validation.go:132), so EVERY key an
		// operator may legitimately write must appear here — or be one
		// of core's five well-known keys (deployTo, image, signingKey,
		// env, ingress) — or parse-time validation rejects the WHOLE
		// stack with "unknown or disallowed top-level key".
		//
		// Learned the hard way on sc-component-backup: `maxMemory` was
		// missing, so a config the plugin otherwise honoured was
		// refused at provision — and once allowlisted it still did not
		// take effect until sc-plugin-aws's overlay learned to write it
		// through. Both halves are needed; both are in place upstream
		// now, so every compute-shape key is listed here from commit
		// one.
		ComponentAllowedOverrides: map[string]*scpluginv1.StringList{
			notifierApi.ResourceType: {Values: []string{
				// Template-side overlays.
				"ingressRoutes", "imagePullSecretRef", "cloudExtras",
				// Compute shape — operator wins over the defaults above.
				"maxMemory", "timeout",
				// Component-specific top-level keys.
				"storage", "operatorPassword",
				// The declarative channel set (decision D3's first
				// half). Validated at PreProvision and rendered into
				// NOTIFIER_CHANNELS; channels an operator adds in the
				// SPA live in storage and are unaffected.
				"channels",
				// Quiet period per (source, subject, kind), so a
				// flapping service cannot storm a channel.
				"cooldown",
				// Which publisher environments may register. Empty or
				// absent allows all. This matters more here than on a
				// dashboard: an isolated stack from a workflow run
				// inherits `uses:` through a YAML merge key, cannot opt
				// out from the YAML side, and would page the operator
				// about a stack destroyed hours ago.
				"tenantEnvironments",
				// The `op: "scheduler"` token the EventBridge rule
				// presents on the delivery tick. Scoped to that one
				// route, so the credential sitting in the rule's input
				// can only drain the queue.
				"schedulerToken",
				// Grandfather for the aws-lambda template's
				// per-Component resource naming (sc-plugin-aws#28):
				// keeps a Component deployed BEFORE that scoping on its
				// un-scoped names, because switching over is a pure
				// rename and Pulumi matches by name — so the plan would
				// create new resources and DELETE the live ones.
				//
				// This component has never deployed un-scoped, so it
				// will never need the flag. It is listed so the key
				// means the same thing on every Component rather than
				// being refused here and accepted next door. Read by
				// the template plugin, not by this one.
				"legacyResourceNames",
			}},
		},

		// Generic secret-env wire: tells the template plugin which env
		// var to bind each `${secret:...}` config field to on the
		// dispatched service. The template plugin binds these to
		// per-function SecretsManager secrets rather than plain env, so
		// plaintext never lands in a Lambda's environment — and the
		// service resolves the ARN at startup
		// (service/cmd/notifier/secret_resolver.go).
		//
		// `signingKey` is the root of every token this component
		// accepts AND the KEK under which SPA-written channel
		// credentials are sealed, so it is the one value that must
		// never travel as plain env.
		ComponentSecretEnvMap: map[string]*scpluginv1.StringMap{
			notifierApi.ResourceType: {Entries: map[string]string{
				"signingKey":       "SERVICE_JWT_SECRET",
				"operatorPassword": "NOTIFIER_OPERATOR_PASSWORD",
			}},
		},
	}, nil
}

// PreProvision lands derived values as tpl extensions.
//
// Everything computed here is deterministic from the operator's config —
// no cloud calls — so it cannot fail for environmental reasons. The one
// thing that CAN fail is the channel set, and it fails the provision on
// purpose: see channelsJSON.
func (p *notifierPlugin) PreProvision(stdctx context.Context, req *scpluginv1.PreProvisionRequest) (resp *scpluginv1.PreProvisionResponse, err error) {
	rctx, err := runtime.NewContext(stdctx, req.GetHeader(), runtime.WithLocalEngineFallback(p.progress))
	if err != nil {
		return nil, err
	}
	defer runtime.Settle(rctx, &resp, &err)

	resName := req.GetResourceName()
	env := req.GetHeader().GetEnvironment()
	p.progress.Info("pre-provision", fmt.Sprintf("notifier component %q (env %q)", resName, env))

	var raw map[string]any
	if b := req.GetComponentRaw(); len(b) > 0 {
		if jerr := json.Unmarshal(b, &raw); jerr != nil {
			return nil, fmt.Errorf("decode ComponentRaw: %w", jerr)
		}
	}
	if raw == nil {
		raw = map[string]any{}
	}

	channelsJSON, summaries, cErr := notifier.ChannelsEnvJSON(raw)
	if cErr != nil {
		// FAIL, loudly, here. A channel that cannot send is the one
		// defect this component cannot absorb: the service would come
		// up, answer 202 to every event, and deliver nothing — which
		// is indistinguishable from a quiet week. Provision time is the
		// last moment an operator is still looking at the output.
		return nil, fmt.Errorf("notifier component %q has an invalid `channels` block: %w", resName, cErr)
	}

	tpl := map[string]string{
		"self-url":       notifier.SelfURLFromConfig(raw),
		"storage-bucket": notifier.BucketForStorageRef(raw, resName, env),
		"storage-region": notifier.StorageRegionFromConfig(raw),
		"storage-prefix": notifier.StoragePrefixFromConfig(raw),
		// The credential the deploy template's timer presents on the
		// delivery tick. Landed as a tpl extension rather than named as
		// a secret inside ComponentDefaults, because those defaults
		// ship to every operator and must not assume one stack's secret
		// names.
		"scheduler-token": notifier.SchedulerTokenFromConfig(raw),
		"channels-json":   channelsJSON,
		"cooldown":        notifier.CooldownFromConfig(raw),
	}

	return runtime.RunPreProvision(
		stdctx, rctx,
		api.Stack{Name: req.GetHeader().GetStackName()},
		api.ResourceInput{Descriptor: &api.ResourceDescriptor{
			Type: req.GetResourceType(),
			Name: resName,
		}},
		pApi.ProvisionParams{},
		func(_ *sdk.Context, _ api.Stack, _ api.ResourceInput, collector pApi.ComputeContextCollector, _ pApi.ProvisionParams) error {
			collector.AddResourceTplExtension(resName, tpl)
			p.progress.Info("pre-provision", fmt.Sprintf(
				"notifier %q derived: self-url=%q bucket=%q region=%q prefix=%q cooldown=%q",
				resName, tpl["self-url"], tpl["storage-bucket"], tpl["storage-region"],
				tpl["storage-prefix"], tpl["cooldown"],
			))
			logChannelSummaries(p.progress, resName, summaries)
			return nil
		},
	)
}

// logChannelSummaries puts the configured channels in the deploy log
// WITHOUT their credentials, and warns per channel whose credential
// travels as a literal.
//
// Two reasons this is worth the lines. First, "no channels" is a legal
// deploy (the SPA can add them later, decision D3) and is indentical on
// the outside to a channel block that silently failed to parse — so the
// log has to say which happened. Second, a literal credential is
// substituted by SC and travels inside NOTIFIER_CHANNELS, which lands in
// the deployed function's environment and is readable by anyone who can
// describe the function. That is fine for a webhook HMAC secret scoped
// to one endpoint and poor for a bot token shared with another service,
// so the plugin states which form was used and leaves the judgement to
// the operator rather than refusing or quietly accepting.
func logChannelSummaries(progress *scPlugin.Publisher, resName string, summaries []notifier.ChannelSummary) {
	if len(summaries) == 0 {
		progress.Info("pre-provision", fmt.Sprintf(
			"notifier %q declares no `channels:` — events will be accepted and routed nowhere until a channel is added in the SPA",
			resName,
		))
		return
	}
	for _, s := range summaries {
		progress.Info("pre-provision", fmt.Sprintf(
			"notifier %q channel %q: type=%s minSeverity=%s credential=%s",
			resName, s.Name, s.Type, s.MinSeverity, credentialDesc(s.CredentialForm),
		))
		if s.CredentialForm == "literal" {
			progress.Warn("pre-provision", fmt.Sprintf(
				"notifier %q channel %q carries a literal credential: it travels in the deployed function's environment, readable by anyone who can describe the function. "+
					"To keep it out of env, put a SecretsManager ARN in the field instead — the service resolves it at startup and the execution role already permits the read",
				resName, s.Name,
			))
		}
	}
}

func credentialDesc(form string) string {
	switch form {
	case "arn":
		return "secretsmanager-arn (resolved at startup)"
	case "literal":
		return "literal (in function env)"
	default:
		return "none"
	}
}

// Provision is a near-no-op by design: Path A dispatches the
// operator-chosen compute template before this runs, so the service is
// already deployed. Kept so the RPC is answered rather than erroring,
// and so the deploy log records that the component was seen.
func (p *notifierPlugin) Provision(stdctx context.Context, req *scpluginv1.ProvisionRequest) (resp *scpluginv1.ProvisionResponse, err error) {
	rctx, err := runtime.NewContext(stdctx, req.GetHeader(), runtime.WithLocalEngineFallback(p.progress))
	if err != nil {
		return nil, err
	}
	defer runtime.Settle(rctx, &resp, &err)
	p.progress.Info("provision", fmt.Sprintf(
		"notifier component %q provisioned via the dispatched template (nothing further to do)",
		req.GetResourceName(),
	))
	return &scpluginv1.ProvisionResponse{}, nil
}

// ComputeProcess fires once per publisher that lists this component in
// `uses:`. It push-registers that deploy as a tenant and injects the
// publisher's runtime wiring: NOTIFY_URL (where to send) and
// NOTIFY_TOKEN (what to send it with).
func (p *notifierPlugin) ComputeProcess(stdctx context.Context, req *scpluginv1.ComputeProcessRequest) (resp *scpluginv1.ComputeProcessResponse, err error) {
	rctx, err := runtime.NewContext(stdctx, req.GetHeader(), runtime.WithLocalEngineFallback(p.progress))
	if err != nil {
		return nil, err
	}
	defer runtime.Settle(rctx, &resp, &err)

	p.progress.Info("compute-process", fmt.Sprintf("resource=%q type=%q publisher=%q",
		req.GetResourceName(), req.GetResourceType(), req.GetHeader().GetStackName()))

	parent, err := runtime.ParentInfoFromRequest(req)
	if err != nil {
		return nil, fmt.Errorf("parent decode: %w", err)
	}
	deps, err := runtime.DependencyOutputsFromComputeProcessRequest(req)
	if err != nil {
		return nil, fmt.Errorf("deps decode: %w", err)
	}
	componentRaw, err := runtime.ComponentRawFromComputeProcessRequest(req)
	if err != nil {
		return nil, fmt.Errorf("component_raw decode: %w", err)
	}
	if componentRaw == nil {
		componentRaw = map[string]any{}
	}
	input := runtime.ResourceInputForComputeProcess(req, parent, componentRaw)

	// Environment gate — BEFORE any work, and before any credential is
	// minted. See notifier.EnvMayRegister for why an alerting component
	// needs this more than a dashboard does: a throwaway stack that
	// registers here holds a valid event token and pages the operator
	// at 3am about a backup that failed inside a stack destroyed hours
	// ago, and an alerting path that cries wolf makes the next real
	// alert the one that gets ignored.
	//
	// Skipping is NOT silent: the log names the refused environment and
	// the allowlist, so an operator who adds a real environment and
	// forgets to list it learns it from the deploy that should have
	// registered, not from the alert that never arrives.
	publisherEnv := publisherEnvironment(req, parent)
	if allowed := notifier.TenantEnvironmentsFromConfig(componentRaw); !notifier.EnvMayRegister(allowed, publisherEnv) {
		p.progress.Info("compute-process", fmt.Sprintf(
			"skipping notifier registration for %q: environment %q is not in tenantEnvironments %v",
			req.GetHeader().GetStackName(), publisherEnv, allowed,
		))
		return &scpluginv1.ComputeProcessResponse{}, nil
	}

	processor := notifier.NewProcessor(notifier.ProcessorConfig{
		Progress: p.progress,
	})

	// WithIncomingEnvVars seeds the collector from the shared compute
	// context, so this processor sees env emitted by PRECEDING `uses:`
	// entries — which is where VERSION and GITHUB_SHA come from. Hence
	// the documented requirement that `my-notifier` be listed LAST in a
	// publisher's `uses:`.
	resp, _, err = runtime.RunComputeProcess(
		stdctx, rctx,
		api.Stack{Name: req.GetHeader().GetStackName()}, input,
		pApi.ProvisionParams{Log: logger.New(), ParentStack: parent, ResourceOutputs: deps},
		processor.Process,
		runtime.WithIncomingEnvVars(req),
	)
	if err != nil {
		p.progress.Error("compute-process", err.Error())
		return nil, err
	}
	p.progress.Info("compute-process", fmt.Sprintf("emitted %d env var(s)", len(resp.GetEnvVars())))
	return resp, nil
}

// PreDestroy fires when the component's OWN resource is destroyed — NOT
// when a publisher is.
//
// There is no "consumer destroyed" event on the plugin bus. The
// consequence here is specific and worth stating: a destroyed
// publisher's event token stays valid, because it is signed rather than
// stored, and only a `signingKey` rotation revokes it. What that token
// can do is POST events attributed to a stack that no longer exists, so
// the blast radius is a misleading alert rather than access to anything.
// The service ships DELETE /api/v1/tenants/{stack} so an operator can
// drop the row, and a stale tenant is badged in the SPA.
func (p *notifierPlugin) PreDestroy(ctx context.Context, req *scpluginv1.PreDestroyRequest) (*scpluginv1.PreDestroyResponse, error) {
	if err := notifier.PreDestroy(ctx, p.progress, req); err != nil {
		p.progress.Error("pre-destroy", err.Error())
	}
	return &scpluginv1.PreDestroyResponse{}, nil
}

// LogStream drains the progress Publisher onto the gRPC stream. SC core
// opens one of these per plugin after Schema() returns; without it the
// Info/Warn lines this plugin emits never reach the deploy log — which
// would make a refused channel and a skipped registration both
// invisible.
func (p *notifierPlugin) LogStream(_ *scpluginv1.LogStreamRequest, stream scpluginv1.SCCloudPlugin_LogStreamServer) error {
	ctx := stream.Context()
	for {
		batch, ok := p.progress.Drain()
		if !ok {
			return nil
		}
		for _, ev := range batch {
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// publisherEnvironment resolves the environment the PUBLISHER is being
// deployed to.
//
// The header is authoritative. ParentInfo.StackEnv is the fallback for a
// dispatch shape that leaves the header's env empty, and the stack
// name's `--<env>` suffix is the last resort — it is what the service
// itself derives for display, so a value found there is at least
// consistent with what an operator sees.
func publisherEnvironment(req *scpluginv1.ComputeProcessRequest, parent *pApi.ParentInfo) string {
	if e := strings.TrimSpace(req.GetHeader().GetEnvironment()); e != "" {
		return e
	}
	if parent != nil {
		if e := strings.TrimSpace(parent.StackEnv); e != "" {
			return e
		}
	}
	if stack := req.GetHeader().GetStackName(); stack != "" {
		if i := strings.LastIndex(stack, "--"); i >= 0 && i+2 < len(stack) {
			return stack[i+2:]
		}
	}
	return ""
}

// Keeps the api package referenced even if a future edit drops its only
// other use, mirroring the other two components.
var _ = api.Credentials{}

func main() {
	impl := &notifierPlugin{progress: scPlugin.NewPublisher(0)}
	scPlugin.Serve(scPlugin.ServerConfig{
		Impl: impl,
		Logger: hclog.New(&hclog.LoggerOptions{
			Name:   "sc-component-notifier",
			Level:  hclog.Warn,
			Output: os.Stderr,
		}),
	})
}
