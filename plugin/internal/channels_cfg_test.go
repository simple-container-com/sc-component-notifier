// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package notifier

import (
	"encoding/json"
	"testing"

	. "github.com/onsi/gomega"
)

// The shape the plugin bus actually delivers: a []any of map[string]any,
// because the component config crossed a JSON boundary before this
// plugin saw it. Tests that hand over []ChannelConfig would pass while
// the real path failed, so every fixture here goes through the decoded
// form.
func rawChannels(t *testing.T, yamlish string) map[string]any {
	t.Helper()
	var out map[string]any
	Expect(json.Unmarshal([]byte(yamlish), &out)).To(Succeed())
	return out
}

func TestNoChannelsIsALegalDeploy(t *testing.T) {
	RegisterTestingT(t)

	// Decision D3 is "both": declarative channels AND SPA-managed ones.
	// A notifier declared with no `channels:` at all is therefore valid
	// — the operator adds them in the UI. Failing here would make the
	// SPA half unreachable.
	for _, raw := range []map[string]any{
		{},
		{"channels": nil},
	} {
		out, summaries, err := ChannelsEnvJSON(raw)
		Expect(err).ToNot(HaveOccurred())
		Expect(out).To(BeEmpty(), "an absent channels block must not render an env value")
		Expect(summaries).To(BeEmpty())
	}
}

func TestChannelsRenderWithFieldNamesTheServiceReads(t *testing.T) {
	RegisterTestingT(t)

	raw := rawChannels(t, `{"channels":[
		{"name":"ops-telegram","type":"telegram","token":"123:abc","chatId":"-100","minSeverity":"error"},
		{"name":"ims","type":"webhook","url":"https://ims.example.com/hook","token":"hmac-secret"}
	]}`)

	out, summaries, err := ChannelsEnvJSON(raw)
	Expect(err).ToNot(HaveOccurred())

	// The service parses NOTIFIER_CHANNELS with its OWN struct
	// (managedChannelJSON in service/cmd/notifier/main.go). If the key
	// spelling drifts — chatId vs chat_id, minSeverity vs min_severity —
	// every field silently arrives zero and a telegram channel is
	// rejected at boot for "no chat id". Asserting the wire keys, not
	// just a successful round-trip through this package's own structs,
	// is the only version of this test that can catch that.
	var decoded []map[string]any
	Expect(json.Unmarshal([]byte(out), &decoded)).To(Succeed())
	Expect(decoded).To(HaveLen(2))
	Expect(decoded[0]).To(HaveKeyWithValue("name", "ops-telegram"))
	Expect(decoded[0]).To(HaveKeyWithValue("type", "telegram"))
	Expect(decoded[0]).To(HaveKeyWithValue("chatId", "-100"))
	Expect(decoded[0]).To(HaveKeyWithValue("token", "123:abc"))
	Expect(decoded[0]).To(HaveKeyWithValue("minSeverity", "error"))
	Expect(decoded[1]).To(HaveKeyWithValue("url", "https://ims.example.com/hook"))

	Expect(summaries).To(HaveLen(2))
	Expect(summaries[0].CredentialForm).To(Equal("literal"))
	Expect(summaries[1].CredentialForm).To(Equal("literal"))
}

// A blank minSeverity must stay blank on the wire. The service applies
// DefaultMinSeverity (`warning`) itself; writing the resolved value here
// would put the same rule in two places, and the day they disagree the
// operator's channel silently changes behaviour on a plugin upgrade they
// did not ask for.
func TestBlankMinSeverityIsNotResolvedIntoThePayload(t *testing.T) {
	RegisterTestingT(t)

	raw := rawChannels(t, `{"channels":[{"name":"ops","type":"telegram","token":"t","chatId":"c"}]}`)
	out, summaries, err := ChannelsEnvJSON(raw)
	Expect(err).ToNot(HaveOccurred())
	Expect(out).ToNot(ContainSubstring("minSeverity"),
		"the omitempty field must be absent so the SERVICE decides the default")

	// The summary is the one place it IS resolved — that line is for a
	// human reading the deploy log, who should see the effective gate
	// rather than an empty string.
	Expect(summaries[0].MinSeverity).To(Equal("warning"))
}

// The guard that exists because of a live incident: observatory's
// healthcheck timer shipped with `Bearer $sc:${resource:...}` as a
// literal string, the rule fired on time, and the tick 401'd twice
// before anyone looked (2026-11-19). The same class of fault in a
// channel credential produces a notifier that accepts every event and
// delivers none.
func TestUnresolvedPlaceholderFailsTheProvision(t *testing.T) {
	RegisterTestingT(t)

	cases := []struct {
		name, body, wantsKey string
	}{
		{
			name:     "secret ref in token",
			body:     `{"channels":[{"name":"ops","type":"telegram","token":"${secret:tg-token}","chatId":"-100"}]}`,
			wantsKey: "token",
		},
		{
			name:     "tpl extension in chatId",
			body:     `{"channels":[{"name":"ops","type":"telegram","token":"t","chatId":"$sc:${resource:n.chat}"}]}`,
			wantsKey: "chatId",
		},
		{
			name:     "resource ref in url",
			body:     `{"channels":[{"name":"ims","type":"webhook","url":"${resource:ims.endpoint}"}]}`,
			wantsKey: "url",
		},
	}
	for _, tc := range cases {
		_, _, err := ChannelsEnvJSON(rawChannels(t, tc.body))
		Expect(err).To(HaveOccurred(), tc.name)
		// The message has to name the channel, the field and a way out
		// — an operator reading a failed provision cannot grep the
		// plugin source.
		Expect(err.Error()).To(ContainSubstring(tc.wantsKey), tc.name)
		Expect(err.Error()).To(ContainSubstring("unresolved placeholder"), tc.name)
		Expect(err.Error()).To(ContainSubstring("SecretsManager ARN"), tc.name)
	}
}

// A SecretsManager ARN is NOT an unresolved placeholder — it is the
// recommended form, resolved by the service at startup. Rejecting it
// would leave the operator with only the literal-in-env option, which
// is the thing the ARN form exists to avoid.
func TestSecretARNIsAcceptedAndClassified(t *testing.T) {
	RegisterTestingT(t)

	raw := rawChannels(t, `{"channels":[{"name":"ops","type":"telegram",`+
		`"token":"arn:aws:secretsmanager:eu-central-1:1234:secret:tg-token-AbCdEf","chatId":"-100"}]}`)

	out, summaries, err := ChannelsEnvJSON(raw)
	Expect(err).ToNot(HaveOccurred())
	Expect(out).To(ContainSubstring("arn:aws:secretsmanager:"),
		"the ARN travels to the service verbatim; it resolves it at startup")
	Expect(summaries[0].CredentialForm).To(Equal("arn"))
}

func TestInvalidChannelFailsRatherThanDeployingQuietly(t *testing.T) {
	RegisterTestingT(t)

	cases := []struct{ name, body, want string }{
		{
			"nameless channel",
			`{"channels":[{"type":"telegram","token":"t","chatId":"c"}]}`,
			"needs a `name`",
		},
		{
			"telegram without a chat id",
			`{"channels":[{"name":"ops","type":"telegram","token":"t"}]}`,
			"chatId",
		},
		{
			"webhook without a url",
			`{"channels":[{"name":"ims","type":"webhook"}]}`,
			"url",
		},
		{
			"unknown type",
			`{"channels":[{"name":"ops","type":"carrier-pigeon"}]}`,
			"carrier-pigeon",
		},
		{
			"unknown minSeverity",
			`{"channels":[{"name":"ops","type":"telegram","token":"t","chatId":"c","minSeverity":"urgent"}]}`,
			"minSeverity",
		},
		{
			"duplicate name",
			`{"channels":[{"name":"ops","type":"telegram","token":"t","chatId":"c"},` +
				`{"name":"ops","type":"webhook","url":"https://x.example.com"}]}`,
			"duplicate channel name",
		},
		{
			"not a list",
			`{"channels":{"name":"ops"}}`,
			"must be a list",
		},
	}
	for _, tc := range cases {
		out, _, err := ChannelsEnvJSON(rawChannels(t, tc.body))
		Expect(err).To(HaveOccurred(), tc.name)
		Expect(err.Error()).To(ContainSubstring(tc.want), tc.name)
		Expect(out).To(BeEmpty(), tc.name+": nothing may be rendered from a config that failed")
	}
}
