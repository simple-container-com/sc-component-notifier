// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package notifier

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"

	notifierApi "github.com/simple-container-com/sc-component-notifier/api"
)

// secretARNPrefix marks a credential the SERVICE resolves at startup
// rather than one that travels in the function's environment.
//
// The aws-lambda template's execution role grants
// `secretsmanager:GetSecretValue` on `*` (sc-plugin-aws
// plugin/internal/aws_lambda.go:272-295), so an ARN the operator writes
// by hand — pointing at a secret this component never created — is
// readable without any extra IAM. That is what makes the ARN form a
// real escape hatch rather than advice that does not work.
const secretARNPrefix = "arn:aws:secretsmanager:"

// unresolvedMarkers are the placeholder forms SC substitutes BEFORE a
// plugin sees the config. One still present in a channel field means
// the substitution did not happen for that key, and the value would
// travel to the service as the literal string.
//
// This is not hypothetical. The observatory component's healthcheck
// timer shipped with `Bearer $$sc:${resource:${name}.scheduler-token}`
// in its schedule input: the rule was created correctly, fired on time,
// and answered 401 twice before anyone looked (live-caught 2026-11-19).
// SC resolves `${secret:NAME}` in some positions and the
// `$$sc:${resource:...}` tpl-extension form in others, and the two sets
// are not the same. Failing the deploy here costs an operator one
// error message; shipping it costs them a channel that looks configured
// and silently cannot send.
var unresolvedMarkers = []string{"${secret:", "${resource:", "$$sc:", "${auth:"}

// ChannelSummary is the loggable view of one managed channel: enough
// for an operator reading the deploy log to see what was configured,
// with no credential in it.
type ChannelSummary struct {
	Name        string
	Type        string
	MinSeverity string
	// CredentialForm is "", "arn" or "literal" — see
	// secretARNPrefix for why the distinction is worth a log line.
	CredentialForm string
}

// ChannelsEnvJSON renders the operator's declarative `channels:` block
// into the NOTIFIER_CHANNELS value the service parses at boot
// (service/cmd/notifier/main.go loadManagedChannels).
//
// Returns ("", nil, nil) when no channels are declared. That is a valid
// deploy — the SPA can add channels later (decision D3) — and the
// service logs a warning at boot saying events will be routed nowhere.
//
// Validation is the api module's ChannelConfig.Validate, reused rather
// than re-stated, so the rules an operator reads in the typed config
// are the rules enforced at deploy time. Everything here fails the
// PROVISION: a channel that cannot send is the one defect this
// component cannot absorb, and parse time is the only moment an
// operator is still looking.
func ChannelsEnvJSON(raw map[string]any) (string, []ChannelSummary, error) {
	declared, ok := raw["channels"]
	if !ok || declared == nil {
		return "", nil, nil
	}

	// Round-trip through JSON rather than reaching into the []any by
	// hand: the plugin bus already delivered this as decoded JSON, so
	// re-encoding and decoding into the typed struct is both shorter
	// and the only form that keeps the field names in ONE place — the
	// api module's struct tags, which are also what the service reads.
	encoded, err := json.Marshal(declared)
	if err != nil {
		return "", nil, errors.Wrap(err, "re-encode the `channels` block")
	}
	var channels []notifierApi.ChannelConfig
	if err := json.Unmarshal(encoded, &channels); err != nil {
		return "", nil, errors.Wrap(err, "`channels` must be a list of channel declarations")
	}

	seen := make(map[string]struct{}, len(channels))
	summaries := make([]ChannelSummary, 0, len(channels))
	for _, ch := range channels {
		if err := ch.Validate(); err != nil {
			return "", nil, err
		}
		if _, dup := seen[ch.Name]; dup {
			return "", nil, errors.Errorf(
				"duplicate channel name %q in `channels` — names are the delivery log's identifier and must be unique", ch.Name,
			)
		}
		seen[ch.Name] = struct{}{}

		if err := rejectUnresolved(ch); err != nil {
			return "", nil, err
		}
		summaries = append(summaries, ChannelSummary{
			Name:           ch.Name,
			Type:           ch.Type,
			MinSeverity:    ch.ResolvedMinSeverity(),
			CredentialForm: credentialForm(ch.Token),
		})
	}

	// MinSeverity is passed through VERBATIM, not resolved to the
	// default here. The service applies DefaultMinSeverity when the
	// field is blank, and two copies of that rule is how they drift —
	// the summary above resolves it only for the log line.
	out, err := json.Marshal(channels)
	if err != nil {
		return "", nil, errors.Wrap(err, "encode NOTIFIER_CHANNELS")
	}
	return string(out), summaries, nil
}

// rejectUnresolved fails a channel whose credential or address still
// carries an SC placeholder. See unresolvedMarkers.
func rejectUnresolved(ch notifierApi.ChannelConfig) error {
	for _, f := range []struct{ key, value string }{
		{"token", ch.Token},
		{"chatId", ch.ChatID},
		{"url", ch.URL},
	} {
		for _, marker := range unresolvedMarkers {
			if strings.Contains(f.value, marker) {
				return errors.Errorf(
					"channel %q has an unresolved placeholder in `%s`: the value still contains %q, so SC did not substitute it before dispatch. "+
						"Declare the secret in the parent stack's secrets registry and reference it as `%s: \"${secret:<name>}\"`, "+
						"or pass a SecretsManager ARN the service resolves at startup",
					ch.Name, f.key, marker, f.key,
				)
			}
		}
	}
	return nil
}

// credentialForm classifies a channel credential for the deploy log.
//
// A "literal" credential is substituted by SC and travels inside
// NOTIFIER_CHANNELS, which lands in the deployed function's
// environment — readable by anyone who can describe the function. That
// is acceptable for a webhook HMAC secret scoped to one endpoint and
// poor for a bot token shared with another service, so the plugin says
// which form each channel used rather than deciding for the operator.
func credentialForm(token string) string {
	switch {
	case strings.TrimSpace(token) == "":
		return ""
	case strings.HasPrefix(token, secretARNPrefix):
		return "arn"
	default:
		return "literal"
	}
}
