// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package notifier

import (
	"github.com/pkg/errors"

	"github.com/simple-container-com/api/pkg/api"
)

// NotifierConfig documents the expected `.Raw` keys for a `type: sc-notifier`
// resource. **Core does NOT import this struct**; the production `sc`
// binary reaches this plugin only via go-plugin gRPC, so SC core's
// sidecar wrapper (slice 1a) reads these keys off `PluginFieldConfig.Raw`
// directly. The struct exists for:
//
//   - Local tooling / IDE hints when authoring a server.yaml
//   - Test fixtures in the plugin module that need a typed view
//   - Documentation of the on-the-wire shape
//
// See the forge memory
// `interface_assertion_against_plugin_owned_value_fails_in_prod` and
// sc-components-platform design doc §P1 v1.5 for why this is doc-only.
type NotifierConfig struct {
	// DeployTo names a top-level template from the parent server.yaml's
	// `templates:` block. SC core's sidecar wrapper (slice 1a) looks this
	// up in `stack.Server.Templates`, merges the component's inputs over
	// the operator's template config (plugin wins on image/env/
	// ingressRoutes; operator wins on compute shape — memory/timeout/
	// vpc/iam/etc.), and dispatches
	// `ProvisionFuncByType[tpl.Type](ctx, stack, merged, params)` on the
	// same engine.
	//
	// Required. Must reference an existing template name in the parent
	// stack. Validated at parse time.
	DeployTo string `json:"deployTo" yaml:"deployTo"`

	// Image is the Dockerhub (or other registry) reference for the
	// notifier service container. Required.
	Image string `json:"image" yaml:"image"`

	// SigningKey is a `${secret:...}` reference to a 32+-byte HS256 key
	// shared between the plugin (for minting per-tenant JWTs at
	// ComputeProcess time) and the deployed service (for verifying
	// them). Path A's dispatch injects this as `SERVICE_JWT_SECRET` env
	// on the container.
	//
	// Rotation: edit the secret value in SC's secrets registry +
	// re-provision — both sides pick up the new key on the next run.
	// Required.
	SigningKey string `json:"signingKey" yaml:"signingKey"`

	// Env is an optional map of plain env vars to inject into the
	// deployed container (merged with the template's own env; plugin
	// values win on key collisions). Per the §P1 component_allowed_overrides
	// allowlist.
	Env map[string]string `json:"env,omitempty" yaml:"env,omitempty"`

	// OperatorPassword is a `${secret:...}` reference to the shared
	// password the SPA's login validates. Required for the operator UI.
	OperatorPassword string `json:"operatorPassword,omitempty" yaml:"operatorPassword,omitempty"`

	// SchedulerToken is a `${secret:...}` reference to the bearer the
	// delivery tick presents. The tick endpoint drains the delivery
	// queue, so it is a write path and must not be open.
	SchedulerToken string `json:"schedulerToken,omitempty" yaml:"schedulerToken,omitempty"`

	// MigrationToken is a `${secret:...}` reference to the bearer that
	// gates `POST /admin/migrate-s3-to-goembedb` (the `X-Admin-Token`
	// header) during the S3 → go-embedb cutover.
	//
	// Optional, and **omitting it is the safe default**: the service
	// registers the route only when the value is non-empty, so a
	// notifier whose operator never opted in has no migration surface
	// at all rather than an open one. Declare it for the cutover,
	// delete the line afterwards — the route disappears on the next
	// provision.
	MigrationToken string `json:"migrationToken,omitempty" yaml:"migrationToken,omitempty"`

	// Storage names ANOTHER SC RESOURCE to keep the notifier's own state
	// in — channels, the event dedupe window and the delivery queue.
	//
	// A reference, not a configuration: the operator declares (say) an
	// `s3-bucket` resource in the same server.yaml and points at it by
	// name. Swapping S3 for an EFS volume or a future document store is
	// a change of which resource is named, not a change to this config
	// and not a change to the notifier's code.
	//
	// Optional. Omitted means the plugin provisions its own bucket.
	Storage *StorageRef `json:"storage,omitempty" yaml:"storage,omitempty"`

	// Channels declaratively bootstraps delivery channels. These are
	// MANAGED: re-seeded from this config on every boot, authoritative,
	// and rendered read-only in the SPA. Channels the operator creates
	// in the SPA live in storage and are fully editable there. One
	// channel has exactly one origin, so "which wins" never arises.
	//
	// Optional — a notifier with no channels ingests and routes events
	// but delivers nowhere, which the SPA reports as such rather than
	// looking healthy.
	Channels []ChannelConfig `json:"channels,omitempty" yaml:"channels,omitempty"`

	// Ingress declares the service's public entry point. Routed via the
	// parent's registrar (default: the `default: true` registrar in
	// parent's resources.registrars:).
	Ingress *NotifierIngress `json:"ingress,omitempty" yaml:"ingress,omitempty"`

	// api.Credentials embedded inline so a resource can optionally carry
	// a `${auth:...}` reference. Rarely needed for a component (templates
	// carry their own credentials); kept for forward-compat.
	api.Credentials `json:",inline" yaml:",inline"`
}

// NotifierIngress models the component's public entry point. Optional — a
// smoke run can disable ingress via `ingress: null` and expose the service
// on a high port with no TLS.
type NotifierIngress struct {
	// Hostname is the FQDN the service answers on (e.g.
	// `my-notifier.example.com`). Routed via the parent's registrar.
	Hostname string `json:"hostname" yaml:"hostname"`

	// Registrar is an optional override naming a specific registrar from
	// the parent's resources.registrars: block. Defaults to the
	// `default: true` registrar.
	Registrar string `json:"registrar,omitempty" yaml:"registrar,omitempty"`
}

// StorageRef points at another SC resource by name and type.
//
// YAML accepts either form:
//
//	storage: sc-notifier-data          # type inferred as s3-bucket
//
//	storage:
//	  name: sc-notifier-data
//	  type: s3-bucket
//
// The bare-string form exists because naming the resource is the whole
// decision in the common case. The plugin's `.Raw` reader accepts both.
type StorageRef struct {
	// Name is the SC resource name, as declared elsewhere in the same
	// server.yaml `resources:` block.
	Name string `json:"name,omitempty" yaml:"name,omitempty"`

	// Type is the referenced resource's SC type. Defaults to
	// "s3-bucket". Declared explicitly so the plugin knows which of that
	// resource's outputs to read — rather than sniffing or guessing.
	Type string `json:"type,omitempty" yaml:"type,omitempty"`

	// Prefix optionally namespaces the notifier's keys inside the
	// referenced resource, so one bucket can host several components.
	Prefix string `json:"prefix,omitempty" yaml:"prefix,omitempty"`
}

// Referenced storage resource types the plugin knows how to read.
const (
	// StorageResourceS3Bucket is an SC `s3-bucket` resource. The plugin
	// reads its `<name>-bucket-name` and `<name>-bucket-region` exports.
	StorageResourceS3Bucket = "s3-bucket"

	// StorageResourceVolume is a filesystem-backed resource. Declared so
	// the error message for it can say "not implemented yet" instead of
	// "unknown".
	StorageResourceVolume = "volume"
)

// Channel types implemented in v1.
const (
	// ChannelTypeTelegram delivers via the Bot API `sendMessage`.
	ChannelTypeTelegram = "telegram"

	// ChannelTypeWebhook POSTs signed JSON to an arbitrary URL. The
	// generic escape hatch — Slack, Discord, Mattermost and an incident
	// manager are all this one channel with a different URL.
	ChannelTypeWebhook = "webhook"

	// ChannelTypeEmail is deferred (SES identity, configuration set,
	// bounce/complaint suppression). Declared for the same reason as
	// StorageResourceVolume: a precise refusal beats a vague one.
	ChannelTypeEmail = "email"
)

// Severity ordinals. The ZERO for an unknown string is deliberate and
// load-bearing: a typo'd severity then fails every `minSeverity` gate
// rather than passing the lowest one, so a malformed event cannot storm
// a channel.
//
// This is forge-notifier's scale, reused verbatim because the operator
// already reads alerts on it — not because the code was shared (it was
// not; that service is closed-source and this repo is MIT).
const (
	SeverityInfo     = "info"
	SeverityWarning  = "warning"
	SeverityError    = "error"
	SeverityCritical = "critical"
)

// SeverityOrder maps a severity to its ordinal. Absent → 0.
var SeverityOrder = map[string]int{
	SeverityInfo:     1,
	SeverityWarning:  2,
	SeverityError:    3,
	SeverityCritical: 4,
}

// SeverityRank returns the ordinal for s, or 0 if s is not a known
// severity. Callers gate with `SeverityRank(ev) >= SeverityRank(min)`,
// which an unknown severity on EITHER side fails closed.
func SeverityRank(s string) int { return SeverityOrder[s] }

// DefaultMinSeverity is `warning`, and the choice is a scar.
//
// In forge-notifier every `simple-container-com` Telegram subscription
// was `severityAtLeast: warning` while a good deal of what it sent was
// `info` — so every `info` notification ever emitted was silently
// dropped, and nobody noticed for months because the send path reported
// success. Defaulting to `warning` here makes that the DOCUMENTED
// default rather than an accident, and the rule it implies is the one
// publishers must follow: anything operator-actionable is emitted at
// `warning` or above. `info` is opt-in.
const DefaultMinSeverity = SeverityWarning

// ChannelConfig declares one managed delivery channel.
type ChannelConfig struct {
	// Name is the operator's label, e.g. "ops-telegram". Unique within
	// a notifier; it is how the SPA and the delivery log refer to the
	// channel.
	Name string `json:"name" yaml:"name"`

	// Type is one of the ChannelType* constants.
	Type string `json:"type" yaml:"type"`

	// MinSeverity gates delivery: an event is routed to this channel
	// only if its severity ordinal is >= this one. Defaults to
	// DefaultMinSeverity (`warning`).
	MinSeverity string `json:"minSeverity,omitempty" yaml:"minSeverity,omitempty"`

	// Sources optionally restricts the channel to events from these
	// publisher stacks. Empty means all sources.
	Sources []string `json:"sources,omitempty" yaml:"sources,omitempty"`

	// Kinds optionally restricts the channel to these event kinds
	// (e.g. "backup.failed"). Empty means all kinds.
	Kinds []string `json:"kinds,omitempty" yaml:"kinds,omitempty"`

	// Token is a `${secret:...}` reference — the Telegram bot token for
	// a telegram channel, or the HMAC signing secret for a webhook.
	Token string `json:"token,omitempty" yaml:"token,omitempty"`

	// ChatID is the Telegram chat the message is sent to. Telegram only.
	ChatID string `json:"chatId,omitempty" yaml:"chatId,omitempty"`

	// URL is the webhook endpoint. Webhook only.
	URL string `json:"url,omitempty" yaml:"url,omitempty"`
}

// ResolvedMinSeverity applies the default.
func (c ChannelConfig) ResolvedMinSeverity() string {
	if c.MinSeverity == "" {
		return DefaultMinSeverity
	}
	return c.MinSeverity
}

// Validate checks one channel declaration.
func (c ChannelConfig) Validate() error {
	if c.Name == "" {
		return errors.Errorf("every entry in `channels` needs a `name` (the operator label the SPA and delivery log use)")
	}
	if _, ok := SeverityOrder[c.ResolvedMinSeverity()]; !ok {
		return errors.Errorf("channel %q has unknown `minSeverity` %q — expected one of %s/%s/%s/%s",
			c.Name, c.MinSeverity, SeverityInfo, SeverityWarning, SeverityError, SeverityCritical)
	}
	for _, s := range c.Kinds {
		if s == "" {
			return errors.Errorf("channel %q has an empty entry in `kinds`", c.Name)
		}
	}
	switch c.Type {
	case ChannelTypeTelegram:
		if c.Token == "" {
			return errors.Errorf("channel %q (telegram) needs a `token` (${secret:...} reference to the bot token)", c.Name)
		}
		if c.ChatID == "" {
			return errors.Errorf("channel %q (telegram) needs a `chatId`", c.Name)
		}
	case ChannelTypeWebhook:
		if c.URL == "" {
			return errors.Errorf("channel %q (webhook) needs a `url`", c.Name)
		}
	case ChannelTypeEmail:
		return errors.Errorf("channel %q declares `type: %s`, which is not implemented yet — use %s or %s; see the component README for the slice that adds email",
			c.Name, ChannelTypeEmail, ChannelTypeTelegram, ChannelTypeWebhook)
	case "":
		return errors.Errorf("channel %q needs a `type` (%s or %s)", c.Name, ChannelTypeTelegram, ChannelTypeWebhook)
	default:
		return errors.Errorf("channel %q has unknown `type` %q — expected %s or %s", c.Name, c.Type, ChannelTypeTelegram, ChannelTypeWebhook)
	}
	return nil
}

// ProviderType satisfies api.Config.
func (c *NotifierConfig) ProviderType() string { return ProviderType }

// ResolvedStorage returns the storage reference with defaults applied.
func (c *NotifierConfig) ResolvedStorage() StorageRef {
	if c.Storage == nil {
		return StorageRef{Type: StorageResourceS3Bucket}
	}
	s := *c.Storage
	if s.Type == "" {
		s.Type = StorageResourceS3Bucket
	}
	return s
}

// Validate performs minimum shape checks. Production SC validates
// against `.Raw` directly in the slice 1a sidecar wrapper (louder +
// earlier at server.yaml READ time); this method is for local tests.
func (c *NotifierConfig) Validate() error {
	if c.DeployTo == "" {
		return errors.Errorf("`deployTo` is required for %q (name of a template in the parent server.yaml's templates: block)", ResourceType)
	}
	if c.Image == "" {
		return errors.Errorf("`image` is required for %q (Docker image reference)", ResourceType)
	}
	if c.SigningKey == "" {
		return errors.Errorf("`signingKey` is required for %q (${secret:...} reference to a 32+-byte HS256 key)", ResourceType)
	}
	if err := c.validateStorage(); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(c.Channels))
	for _, ch := range c.Channels {
		if err := ch.Validate(); err != nil {
			return err
		}
		if _, dup := seen[ch.Name]; dup {
			return errors.Errorf("duplicate channel name %q in `channels` — names are the delivery log's identifier and must be unique", ch.Name)
		}
		seen[ch.Name] = struct{}{}
	}
	return nil
}

func (c *NotifierConfig) validateStorage() error {
	if c.Storage == nil {
		return nil
	}
	s := c.ResolvedStorage()
	switch s.Type {
	case StorageResourceS3Bucket:
		if s.Name == "" {
			return errors.Errorf("`storage.name` is required when `storage` is declared for %q (the name of an s3-bucket resource in the same server.yaml)", ResourceType)
		}
	case StorageResourceVolume:
		return errors.Errorf("`storage.type: %s` is declared but not implemented yet for %q — use %q (the default) for now", s.Type, ResourceType, StorageResourceS3Bucket)
	default:
		return errors.Errorf("unknown `storage.type` %q for %q — expected %q (or the not-yet-implemented %q)", s.Type, ResourceType, StorageResourceS3Bucket, StorageResourceVolume)
	}
	return nil
}

// ReadNotifierConfig is the api.ConfigReaderFunc registered in init.go.
func ReadNotifierConfig(config *api.Config) (api.Config, error) {
	return api.ConvertConfig(config, &NotifierConfig{})
}
