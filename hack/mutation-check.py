#!/usr/bin/env python3
"""Mutation check: break one guard at a time, assert its test catches it.

    python3 hack/mutation-check.py            # all mutations
    python3 hack/mutation-check.py events     # one group
    python3 hack/mutation-check.py -l         # list without running

Why this exists
---------------
A test that passes the first time it is written has demonstrated nothing
— it has only shown that the code and the test agree today. Every guard
in this repo exists because something went wrong in production once, so
each one gets an entry below that breaks it deliberately and asserts the
corresponding test goes red with a message a human can act on.

Three failure modes this harness reports, which are easy to confuse:

  NOT CAUGHT    the test passes with the guard broken. The test is
                decorative. Fix the test.
  INVALID       the mutated code does not compile (typically an
                unused variable), so nothing was proven either way.
                Fix the mutation, not the test.
  HUNG          the test deadlocks instead of failing. Worse than no
                test: it burns the whole CI timeout and reports
                nothing. Fix the test's synchronisation.

It covers BOTH the service and plugin modules. The plugin
half earns its place: the `plugin/dependency outputs read off the
pointer, not .Ref` entry encodes a branch that was dead in TWO shipped
components — lookupDepString type-asserted an `*api.ResourceOutput`
struct pointer against `map[string]any`, which can never match, so the
most-trustworthy branch of the service-URL cascade was unreachable and
everything silently fell back to `ingress.hostname`. No test went red,
because no test built a dependency-output fixture the way the plugin bus
builds one.

Five real test/code defects were found this way on 2026-11-23, and none
was visible from a green run:

  * TestMissingCredentialsArePermanent reached the REAL api.telegram.org
    (apiBase was never redirected) and passed only because the live API
    answers 404, which happens to classify as permanent. With the guard
    removed it still passed. Now the base URL points at a closed port.
  * TestSendSurfacesTelegramDescription passed off describeError's
    raw-body fallback rather than its JSON parse, so gutting the parse
    changed nothing. Now it also asserts no raw JSON leaks into the
    message.
  * normalizeURL trimmed slashes BEFORE whitespace, so a value with a
    trailing space kept its trailing slash and every event POST would
    have gone to `//api/v1/events`. Caught by TestNormalizeURL's first
    run, and kept honest by the `URL trimmed in the wrong order` entry.
  * TestEventsIngest_TenantStoreFailureDoesNotFailTheEvent asserted that a
    tenant-store outage cannot fail an ingest, but its fake failed the
    LOOKUP — so ensureTenant returned at the lookup warn and the upsert
    branch was never reached. A panic planted in that branch passed. Now
    the fake fails either call on demand and the test asserts the exact
    Get/Upsert call counts, so a test that stops reaching the failing call
    goes red instead of green. Two entries, one per branch.
  * TestEventsIngest_SubsequentEventRefreshesLastSeen compared two real
    wall-clock stamps with `>=`, which holds whether the refresh happened
    or not. It now runs on an injected clock and asserts the exact value.

One thing to watch for when ADDING a mutation: where a guard is
deliberately layered, breaking one layer proves nothing because the other
still holds, and the harness will report NOT CAUGHT on a test that is
perfectly good. crypto's scope binding is the example — the scope goes
into both the derived key and the GCM additional data, and a mutation
removing either alone still fails the round trip. The fix is a test per
layer (TestDEKDependsOnTheScope, TestOpenPassesTheScopeAsAdditionalData),
not one mutation that breaks both. Where breaking both really is the only
observable fault, say so and set the occurrence count to 2 — see
"delivered rows stay due (both Done() guards)".

The working tree is restored after every mutation, including on failure
and on Ctrl-C. Run it on a clean tree anyway: a crash mid-write would
leave one file mutated, and `git diff` is the check.
"""

import pathlib
import shutil
import signal
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent

# The repo is three Go modules and the guards worth mutating live in two
# of them. File paths below are REPO-relative and their first segment
# names the module, which is also the directory `go test` runs in — a
# plugin mutation tested from service/ would simply not find the package.
# A cross-module contract test is the one exception and says so by
# prefixing its package with the module it runs in ("plugin:./cmd/...").

EVENTS = "./internal/events/"
TG_PKG = "./internal/channel/telegram/"
WH_PKG = "./internal/channel/webhook/"
CRYPTO_PKG = "./internal/crypto/"
REG_PKG = "./internal/registry/"
API_PKG = "./internal/api/"
TENANT_PKG = "./internal/tenant/"
ADMIN_PKG = "./internal/goembedbadmin/"
PLUGIN_PKG = "./internal/"
PLG_CMD_PKG = "./cmd/sc-component-notifier/"
# The SERVICE module's cmd package. Easy to confuse with PLG_CMD_PKG
# above — both are "the cmd package", and reaching for the wrong one
# does not fail loudly: the mutation still applies (the find string is
# in the service's main.go), the harness runs a test package in the
# OTHER module where that test name does not exist, `go test -run`
# reports "no tests to run" and exits 0, and the entry lands as NOT
# CAUGHT rather than as a mistake in the entry. Live-caught writing the
# version-wire guard below.
SVC_CMD_PKG = "./cmd/notifier/"

# A test package that lives in a DIFFERENT module from the file being
# mutated is written "<module>:<pkg>" — see the module resolution in
# main(). Only one guard needs it, and it needs it for a real reason:
# the secret-env binding is declared in the plugin module and consumed
# in the service module, so the contract test that spans them can only
# live on one side.
PLG_CMD_PKG_XMOD = "plugin:" + PLG_CMD_PKG

ROUTER = "service/internal/events/router.go"
DRAIN = "service/internal/events/drain.go"
BLOB = "service/internal/events/store_blob.go"
CHANNEL = "service/internal/channel/channel.go"
ERRORS = "service/internal/channel/errors.go"
HTTPC = "service/internal/channel/http.go"
TG = "service/internal/channel/telegram/telegram.go"
WH = "service/internal/channel/webhook/webhook.go"
SEAL = "service/internal/crypto/seal.go"
REG = "service/internal/registry/registry.go"
TENANT = "service/internal/tenant/store_blob.go"
INGEST = "service/internal/api/events_ingest.go"
TICK = "service/internal/api/scheduler_tick.go"
SESSION = "service/internal/api/session.go"
HEALTHZ = "service/internal/api/healthz.go"
JWTMW = "service/internal/api/jwt_middleware.go"
TENANTS_LIST = "service/internal/api/tenants_list.go"
ADMIN = "service/internal/goembedbadmin/migration.go"
SVC_MAIN = "service/cmd/notifier/main.go"

PLG_CONFIG = "plugin/internal/config.go"
PLG_CHANNELS = "plugin/internal/channels_cfg.go"
PLG_TENANTENV = "plugin/internal/tenant_env.go"
PLG_COMPUTE = "plugin/internal/compute.go"
PLG_MAIN = "plugin/cmd/sc-component-notifier/main.go"

# (group, name, file, test-package, find, replace, test-regex, occurrences)
MUTATIONS = [
    # ---------------------------------------------------------------- events
    ("events", "default minSeverity warning -> info", ROUTER, EVENTS,
     "min = channel.SeverityWarning", "min = channel.SeverityInfo",
     "TestSeverityGate", 1),

    ("events", "unknown severity fails OPEN", CHANNEL, EVENTS,
     "func SeverityRank(s string) int { return severityOrder[s] }",
     "func SeverityRank(s string) int {\n\tif r, ok := severityOrder[s]; ok {\n\t\treturn r\n\t}\n\treturn 99\n}",
     "TestSeverityGate", 1),

    ("events", "retry cap removed", DRAIN, EVENTS,
     "case row.Attempts >= MaxDeliveryAttempts:", "case false:",
     "TestGiveUpAfterMaxAttempts", 1),

    ("events", "permanent errors retried like transient", DRAIN, EVENTS,
     "case channel.IsPermanent(err):", "case false:",
     "TestPermanentErrorGivesUpImmediately", 1),

    ("events", "single-flight lease ignored", DRAIN, EVENTS,
     "if !ok {", "if ok && false {",
     "TestOverlappingDrainsDeliverOnce", 1),

    # Scoped by the preceding `return false, err` so it hits SeenEvent and
    # not CooldownActive, which has the same shape but fails open.
    ("events", "dedupe lookup disabled", BLOB, EVENTS,
     "\t\treturn false, err\n\t}\n\tif found && now.Before(rec.ExpiresAt) {",
     "\t\treturn false, err\n\t}\n\tif false && found && now.Before(rec.ExpiresAt) {",
     "TestDedupeDropsReplayedEventID", 1),

    ("events", "dedupe window never expires", BLOB, EVENTS,
     "ExpiresAt: now.Add(DedupeWindow),", "ExpiresAt: now.Add(1000 * DedupeWindow),",
     "TestDedupeDropsReplayedEventID", 1),

    ("events", "cooldown key ignores subject", ROUTER, EVENTS,
     "key := CooldownKey(ev.Source, ev.Subject, ev.Kind)",
     'key := CooldownKey(ev.Source, "", ev.Kind)',
     "TestCooldownSuppressesFlapButNotSiblings", 1),

    ("events", "cooldown evaluated BEFORE the gates", ROUTER, EVENTS,
     "if len(matched) == 0 {\n\t\treturn decisions, nil\n\t}",
     "if false {\n\t\treturn decisions, nil\n\t}",
     "TestFullyFilteredEventDoesNotStartCooldown", 1),

    ("events", "expired lease never cleared", BLOB, EVENTS,
     'if err := b.st.Delete(ctx, key); err != nil && !errors.Is(err, storage.ErrNotFound) {\n\t\t\treturn false, fmt.Errorf("clear expired tick lease: %w", err)\n\t\t}',
     'if err := error(nil); err != nil {\n\t\t\treturn false, fmt.Errorf("clear expired tick lease: %w", err)\n\t\t}',
     "TestExpiredLeaseIsReclaimed", 1),

    ("events", "ListDue returns newest-first", BLOB, EVENTS,
     "sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })",
     "sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })",
     "TestListDueOrdersOldestFirst", 1),

    ("events", "vanished channel treated as transient", DRAIN, EVENTS,
     'row.LastError = fmt.Sprintf("channel %q no longer configured", row.Channel)\n\t\trow.GiveUpAt = &now\n\t\trow.Permanent = true',
     'row.LastError = fmt.Sprintf("channel %q no longer configured", row.Channel)\n\t\trow.GiveUpAt = &now\n\t\trow.Permanent = false',
     "TestMissingChannelIsPermanent", 1),

    # Both Done() sites at once, deliberately. PutDelivery's move to done/
    # and ListDue's own filter are belt-and-braces: breaking either alone
    # is still covered by the other, which is the point of having both.
    # Only breaking BOTH makes a delivered row due again.
    ("events", "delivered rows stay due (both Done() guards)", BLOB, EVENTS,
     "if d.Done() {", "if false {",
     "TestRetrySucceedsAndStopsBeingDue", 2),

    # -------------------------------------------------------------- telegram
    ("telegram", "'<' left unescaped", TG, TG_PKG,
     's = strings.ReplaceAll(s, "<", "&lt;")', "_ = s",
     "TestRenderEscapesHTML", 1),

    ("telegram", "naive byte-cut truncation", TG, TG_PKG,
     "for cut > 0 && !utf8Start(out[cut]) {\n\t\t\tcut--\n\t\t}",
     "for cut > 0 && false {\n\t\t\tcut--\n\t\t}",
     "TestRenderTruncatesOnRuneBoundary", 1),

    ("telegram", "truncation marker dropped", TG, TG_PKG,
     'const marker = "\\n…(truncated)"', 'const marker = ""',
     "TestRenderTruncatesOnRuneBoundary", 1),

    ("telegram", "labels unsorted", TG, TG_PKG,
     "sort.Strings(keys)", "_ = sort.Strings",
     "TestRenderLabelsAreSorted", 1),

    ("telegram", "default Go User-Agent", TG, TG_PKG,
     'req.Header.Set("User-Agent", channel.UserAgent)', "_ = channel.UserAgent",
     "TestSendUsesBrowserUserAgent", 1),

    ("telegram", "response body capped at 4KB", TG, TG_PKG,
     "respBody, _ := io.ReadAll(resp.Body)",
     "respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))",
     "TestDescribeErrorHandlesWholeBody", 1),

    ("telegram", "Health probes by sending a message", TG, TG_PKG,
     'status, respBody, err := c.post(ctx, "getMe", []byte("{}"))',
     'status, respBody, err := c.post(ctx, "sendMessage", []byte(`{"chat_id":"x","text":"health"}`))',
     "TestHealthUsesGetMeNotSendMessage", 1),

    ("telegram", "all failures retryable", TG, TG_PKG,
     "if !channel.RetryableFromStatus(status) {\n\t\treturn channel.Permanent(err)\n\t}",
     "if false {\n\t\treturn channel.Permanent(err)\n\t}",
     "TestPermanentVsRetryableStatus", 1),

    ("telegram", "missing creds degrade gracefully", TG, TG_PKG,
     'if c.token == "" {\n\t\treturn channel.Permanentf("telegram channel %q has no bot token", c.name)\n\t}\n\tif c.chatID == "" {\n\t\treturn channel.Permanentf("telegram channel %q has no chat id", c.name)\n\t}\n\n\tpayload := map[string]any{',
     'if false {\n\t\treturn channel.Permanentf("telegram channel %q has no bot token", c.name)\n\t}\n\n\tpayload := map[string]any{',
     "TestMissingCredentialsArePermanent", 1),

    ("telegram", "description not parsed out of the body", TG, TG_PKG,
     'if err := json.Unmarshal(body, &env); err == nil && env.Description != "" {\n\t\treturn env.Description\n\t}',
     "if false {\n\t\treturn env.Description\n\t}",
     "TestSendSurfacesTelegramDescription", 1),

    # --------------------------------------------------------------- webhook
    ("webhook", "signature omits the timestamp", WH, WH_PKG,
     'mac.Write([]byte(ts))\n\tmac.Write([]byte("."))\n\tmac.Write(body)',
     "mac.Write(body)",
     "TestSignatureCoversTimestamp", 1),

    ("webhook", "signs with an empty secret", WH, WH_PKG,
     'if c.secret != "" {\n\t\tts := strconv.FormatInt(c.now().UTC().Unix(), 10)',
     "if true {\n\t\tts := strconv.FormatInt(c.now().UTC().Unix(), 10)",
     "TestNoSecretSendsNoSignature", 1),

    ("webhook", "timestamp from event time, not send time", WH, WH_PKG,
     "ts := strconv.FormatInt(c.now().UTC().Unix(), 10)",
     "ts := strconv.FormatInt(m.At.UTC().Unix(), 10)",
     "TestTimestampIsFresh", 1),

    ("webhook", "Health probes the endpoint", WH, WH_PKG,
     'func (c *Channel) Health(_ context.Context) error {\n\tif c.url == "" {\n\t\treturn channel.Permanentf("webhook channel %q has no url", c.name)\n\t}\n\treturn nil\n}',
     'func (c *Channel) Health(ctx context.Context) error {\n\tif c.url == "" {\n\t\treturn channel.Permanentf("webhook channel %q has no url", c.name)\n\t}\n\treturn c.Send(ctx, channel.Message{Title: "health probe"})\n}',
     "TestHealthDoesNotProbeTheEndpoint", 1),

    ("webhook", "event-id header dropped", WH, WH_PKG,
     'if m.ID != "" {\n\t\treq.Header.Set(EventIDHeader, m.ID)\n\t}',
     'if false {\n\t\treq.Header.Set(EventIDHeader, m.ID)\n\t}',
     "TestEventIDHeaderAllowsDedupeWithoutParsing", 1),

    ("webhook", "payload version field renamed", WH, WH_PKG,
     'Version  int               `json:"version"`', 'Version  int               `json:"v"`',
     "TestPayloadContract", 1),

    ("webhook", "429 classified as permanent", ERRORS, WH_PKG,
     "if status == 429 {\n\t\treturn true\n\t}", "if status == 429 {\n\t\treturn false\n\t}",
     "TestPermanentVsRetryableStatus", 1),

    ("webhook", "SendTimeout back to 3s", HTTPC, WH_PKG,
     "SendTimeout = 15 * time.Second", "SendTimeout = 3 * time.Second",
     "TestSendTimeoutIsGenerous", 1),

    # ---------------------------------------------------------------- crypto
    ("crypto", "short KEK accepted", SEAL, CRYPTO_PKG,
     "if len(kek) < MinKEKBytes {", "if false {",
     "TestShortKEKIsRefused", 1),

    # The scope binding is two independent layers, so each gets its own
    # mutation and its own test. Breaking either one alone still fails the
    # round trip, which is why the round-trip test cannot prove either.
    ("crypto", "derived key ignores the scope", SEAL, CRYPTO_PKG,
     'mac.Write([]byte("secrets:" + scope))', 'mac.Write([]byte("secrets:"))',
     "TestDEKDependsOnTheScope", 1),

    ("crypto", "Open drops the scope as additional data", SEAL, CRYPTO_PKG,
     "out, err := gcm.Open(nil, nonce, ct, []byte(scope))",
     "out, err := gcm.Open(nil, nonce, ct, nil)",
     "TestOpenPassesTheScopeAsAdditionalData", 1),

    ("crypto", "Seal drops the scope as additional data", SEAL, CRYPTO_PKG,
     "return gcm.Seal(nonce, nonce, []byte(plaintext), []byte(scope)), nil",
     "return gcm.Seal(nonce, nonce, []byte(plaintext), nil), nil",
     "TestSealPassesTheScopeAsAdditionalData", 1),

    # Reads zero bytes instead of the nonce, leaving it all-zeros — a
    # reused GCM nonce. Written as a short read rather than deleting the
    # call so crypto/rand and io stay used and the mutation compiles.
    ("crypto", "nonce never randomised", SEAL, CRYPTO_PKG,
     "if _, err := io.ReadFull(rand.Reader, nonce); err != nil {",
     "if _, err := io.ReadFull(rand.Reader, nonce[:0]); err != nil {",
     "TestNonceIsFresh", 1),

    ("crypto", "empty plaintext sealed to an envelope", SEAL, CRYPTO_PKG,
     'if plaintext == "" {\n\t\treturn nil, nil\n\t}', "if false {\n\t\treturn nil, nil\n\t}",
     "TestEmptyPlaintextSealsToNil", 1),

    ("crypto", "truncation check removed", SEAL, CRYPTO_PKG,
     "if len(sealed) < gcm.NonceSize() {", "if false {",
     "TestTruncatedCiphertextFails", 1),

    ("crypto", "decrypt error echoes the key", SEAL, CRYPTO_PKG,
     'return "", fmt.Errorf("credential could not be decrypted (wrong key or tampered)")',
     'return "", fmt.Errorf("credential could not be decrypted with key %q: %w", string(s.kek), err)',
     "TestOpenErrorDoesNotLeakTheReason", 1),

    # -------------------------------------------------------------- registry
    ("registry", "managed credential written to the store", REG, REG_PKG,
     "\t\t\tManaged:     true,", "\t\t\tCredential:  []byte(m.Credential),\n\t\t\tManaged:     true,",
     "TestManagedCredentialIsNeverPersisted", 1),

    ("registry", "managed credential read from the store", REG, REG_PKG,
     "\tif rec.Managed {\n\t\tm, ok := r.managed[rec.Name]", "\tif false {\n\t\tm, ok := r.managed[rec.Name]",
     "TestManagedCredentialIsNeverPersisted", 1),

    ("registry", "undeclared managed channel left alive", REG, REG_PKG,
     "if _, stillDeclared := r.managed[c.Name]; stillDeclared {",
     "if _, stillDeclared := r.managed[c.Name]; stillDeclared || true {",
     "TestUndeclaredManagedChannelIsRemoved", 1),

    ("registry", "seed deletes SPA channels too", REG, REG_PKG,
     "\t\tif !c.Managed {\n\t\t\tcontinue\n\t\t}", "\t\tif false {\n\t\t\tcontinue\n\t\t}",
     "TestSeedLeavesSPAChannelsAlone", 1),

    ("registry", "name collision reconciled silently", REG, REG_PKG,
     'r.log.Errorf("channel %q is declared in server.yaml and also exists as a UI-created channel; "+\n'
     '\t\t\t\t"the declaration wins and the UI channel\'s stored credential is discarded", name)',
     "_ = name",
     "TestDeclarationTakesOverACollidingSPAName", 1),

    # Routing must come from the store so managed and SPA channels share
    # one gate implementation. Skipping managed rows here would mean every
    # declaratively-configured channel silently stops matching.
    ("registry", "Routes skips managed channels", REG, REG_PKG,
     "\tfor _, c := range recs {\n\t\troutes = append(routes, events.Route{",
     "\tfor _, c := range recs {\n\t\tif c.Managed {\n\t\t\tcontinue\n\t\t}\n\t\troutes = append(routes, events.Route{",
     "TestRoutesCoverBothOrigins", 1),

    ("registry", "disabled channel still sends", REG, REG_PKG,
     "\t\tif !rec.Enabled {\n\t\t\tcontinue\n\t\t}", "\t\tif false {\n\t\t\tcontinue\n\t\t}",
     "TestDisabledChannelIsNotLiveButIsStillListed", 1),

    ("registry", "unusable channel kept in the live set", REG, REG_PKG,
     '\t\t\tr.log.Errorf("channel %q (%s) is configured but unusable: %v", rec.Name, rec.Type, err)\n\t\t\tcontinue\n',
     '\t\t\tr.log.Errorf("channel %q (%s) is configured but unusable: %v", rec.Name, rec.Type, err)\n',
     "TestIncompleteChannelIsOmittedWithAReason", 1),

    ("registry", "telegram without a chat id is usable", REG, REG_PKG,
     '\t\tif rec.ChatID == "" {\n\t\t\treturn nil, fmt.Errorf("no chat id")\n\t\t}',
     "\t\tif false {\n\t\t\treturn nil, fmt.Errorf(\"no chat id\")\n\t\t}",
     "TestIncompleteChannelIsOmittedWithAReason", 1),

    ("registry", "webhook demands a secret", REG, REG_PKG,
     "\t\treturn webhook.New(webhook.Config{Name: rec.Name, URL: rec.URL, Secret: cred}), nil",
     '\t\tif cred == "" {\n\t\t\treturn nil, fmt.Errorf("no secret")\n\t\t}\n'
     "\t\treturn webhook.New(webhook.Config{Name: rec.Name, URL: rec.URL, Secret: cred}), nil",
     "TestWebhookWithoutSecretIsUsable", 1),

    ("registry", "missing sealer treated as no credential", REG, REG_PKG,
     '\tif r.sealer == nil {\n\t\treturn "", fmt.Errorf("a stored credential exists but this process has no encryption key to open it")\n\t}',
     '\tif false {\n\t\treturn "", fmt.Errorf("a stored credential exists but this process has no encryption key to open it")\n\t}',
     "TestSPACredentialIsUnsealed", 1),

    ("registry", "blank minSeverity shown as blank", REG, REG_PKG,
     '\t\tif st.MinSeverity == "" {', "\t\tif false {",
     "TestStatusShowsEffectiveMinSeverity", 1),

    ("registry", "managed hasCredential read from the record", REG, REG_PKG,
     '\t\t\tm, ok := r.managed[rec.Name]\n\t\t\tst.HasCredential = ok && m.Credential != ""',
     "\t\t\t_ = r.managed[rec.Name]",
     "TestStatusReportsManagedCredentialFromMemory", 1),

    # A cached channel set would keep sending with a credential the
    # operator has already revoked, until the Lambda container recycled.
    ("registry", "live channel set cached across ticks", REG, REG_PKG,
     "func (r *Registry) Channels(ctx context.Context) (map[string]channel.Channel, error) {",
     "var channelsCache map[string]channel.Channel\n\n"
     "func (r *Registry) Channels(ctx context.Context) (map[string]channel.Channel, error) {\n"
     "\tif channelsCache == nil {\n"
     "\t\tm, err := r.channelsUncached(ctx)\n"
     "\t\tif err != nil {\n"
     "\t\t\treturn nil, err\n"
     "\t\t}\n"
     "\t\tchannelsCache = m\n"
     "\t}\n"
     "\treturn channelsCache, nil\n"
     "}\n\n"
     "func (r *Registry) channelsUncached(ctx context.Context) (map[string]channel.Channel, error) {",
     "TestChannelsReReadsOnEveryCall", 1),

    # ---------------------------------------------------------------- ingest
    ("ingest", "token op not checked", INGEST, API_PKG,
     'if claims == nil || claims.Op != "event" {', "if claims == nil {",
     "TestEventsIngest_RejectsWrongOp", 1),

    ("ingest", "source taken from the body when present", INGEST, API_PKG,
     "\t\tev.Source = claims.Tenant",
     '\t\tif ev.Source == "" {\n\t\t\tev.Source = claims.Tenant\n\t\t}',
     "TestEventsIngest_SpoofedSourceCannotPassAnAllowlist", 1),

    ("ingest", "tenant-less token accepted", INGEST, API_PKG,
     '\t\tif ev.Source == "" {\n\t\t\thttp.Error(w, "forbidden: token carries no tenant claim", http.StatusForbidden)',
     '\t\tif false {\n\t\t\thttp.Error(w, "forbidden: token carries no tenant claim", http.StatusForbidden)',
     "TestEventsIngest_RejectsTokenWithoutTenant", 1),

    ("ingest", "unknown severity accepted", INGEST, API_PKG,
     "if !channel.KnownSeverity(ev.Severity) {", "if false && !channel.KnownSeverity(ev.Severity) {",
     "TestEventsIngest_RejectsUnknownSeverity", 1),

    ("ingest", "missing id defaulted instead of rejected", INGEST, API_PKG,
     '\t\tcase ev.ID == "":', "\t\tcase false:",
     "TestEventsIngest_RequiresTheFieldsThatCannotBeDefaulted", 1),

    ("ingest", "dedupe failure fails OPEN", INGEST, API_PKG,
     "\t\tdup, err := store.SeenEvent(r.Context(), ev.ID, receipt)\n\t\tif err != nil {",
     "\t\tdup, err := store.SeenEvent(r.Context(), ev.ID, receipt)\n\t\tif err != nil && false {",
     "TestEventsIngest_DedupeFailureIsNotRouted", 1),

    ("ingest", "duplicate routed anyway", INGEST, API_PKG,
     "\t\tif dup {", "\t\tif dup && false {",
     "TestEventsIngest_DuplicateIsAcceptedButNotRouted", 1),

    ("ingest", "unreadable routes treated as an empty table", INGEST, API_PKG,
     "\t\trules, err := routes.Routes(r.Context())\n\t\tif err != nil {",
     "\t\trules, err := routes.Routes(r.Context())\n\t\tif err != nil && false {",
     "TestEventsIngest_UnreadableRoutesIsAnError", 1),

    ("ingest", "event time left zero", INGEST, API_PKG,
     "\t\tif ev.At.IsZero() {", "\t\tif false {",
     "TestEventsIngest_EventTimeDefaultsToReceipt", 1),

    ("ingest", "publisher event time overwritten", INGEST, API_PKG,
     "\t\tif ev.At.IsZero() {", "\t\tif true {",
     "TestEventsIngest_PublisherEventTimeIsPreserved", 1),

    # Lazy registration is what makes push-register safe to fail. Remove it
    # and a publisher whose deploy-time register call did not land stays
    # invisible in the operator's tenant list forever, while delivering
    # alerts perfectly — so nothing anywhere reports the fault.
    ("ingest", "tenant never created from its first event", INGEST, API_PKG,
     "\t\tensureTenant(r.Context(), tenants, ev.Source, receipt)",
     "\t\t_ = ensureTenant",
     "TestEventsIngest_FirstEventRegistersTheTenant", 1),

    ("ingest", "LastSeenAt never stamped", INGEST, API_PKG,
     "\trecord.LastSeenAt = at", "\t_ = at",
     "TestEventsIngest_FirstEventRegistersTheTenant", 1),

    # Stamped on create but never refreshed afterwards — the subtler half,
    # and the one the SPA's list actually depends on: "last seen" silently
    # means "last deployed", so a publisher that has gone quiet is
    # indistinguishable from a healthy one with nothing to say.
    ("ingest", "LastSeenAt stamped on create only", INGEST, API_PKG,
     "\trecord.LastSeenAt = at", "\tif !found {\n\t\trecord.LastSeenAt = at\n\t}",
     "TestEventsIngest_SubsequentEventRefreshesLastSeen", 1),

    ("ingest", "publishing tenant left soft-deleted", INGEST, API_PKG,
     "\t\trecord.SoftDeleted = false", "\t\trecord.SoftDeleted = true",
     "TestEventsIngest_SoftDeletedTenantIsRevivedByPublishing", 1),

    # The two halves of "bookkeeping NEVER fails an ingest" get a mutation
    # each, because a fake that fails the LOOKUP never reaches the upsert:
    # one mutation cannot prove both paths, and the first version of the
    # test proved neither (it passed with a panic in the upsert branch).
    ("ingest", "lookup failure falls through to the upsert", INGEST, API_PKG,
     '\t\tslog.Warn("tenant lookup failed; event unaffected", "stackName", stackName, "err", err)\n\t\treturn',
     '\t\tslog.Warn("tenant lookup failed; event unaffected", "stackName", stackName, "err", err)',
     "TestEventsIngest_TenantStoreFailureDoesNotFailTheEvent", 1),

    # A panic rather than an error return: ensureTenant has no way to
    # affect the response by design, so the only way to express "the
    # failure escaped" is to make escaping fatal.
    ("ingest", "upsert failure reaches the response path", INGEST, API_PKG,
     '\t\tslog.Warn("tenant upsert failed; event unaffected", "stackName", stackName, "err", err)',
     '\t\tpanic("bookkeeping failure reached the response path")',
     "TestEventsIngest_TenantStoreFailureDoesNotFailTheEvent", 1),

    # ---------------------------------------------------------------- tenant
    # The sanitiser-vs-hash choice: replacing the offending characters
    # keeps keys readable but collapses two distinct stack names into one
    # row, which silently merges two publishers' registrations.
    ("tenant", "stack name sanitised instead of hashed", TENANT, TENANT_PKG,
     "\tsum := sha256.Sum256([]byte(s))\n\treturn hex.EncodeToString(sum[:])",
     '\t_ = sha256.Sum256([]byte(s))\n\t_ = hex.EncodeToString\n\t'
     'return strings.ReplaceAll(strings.ReplaceAll(s, "/", "-"), ".", "-")',
     "TestSimilarNamesDoNotCollide", 1),

    ("tenant", "stack name used in the key verbatim", TENANT, TENANT_PKG,
     '\treturn b.prefix + "tenants/" + safeName(stackName) + ".json"',
     '\t_ = safeName\n\treturn b.prefix + "tenants/" + stackName + ".json"',
     "TestHostileStackNameCannotEscapeThePrefix", 1),

    ("tenant", "List order left to the storage layer", TENANT, TENANT_PKG,
     "\tsort.Slice(out, func(i, j int) bool { return out[i].StackName < out[j].StackName })",
     "\t_ = sort.Slice",
     "TestListIsSortedByStackName", 1),

    ("tenant", "nameless tenant accepted", TENANT, TENANT_PKG,
     '\tif record.StackName == "" {\n\t\treturn errors.New("stackName is required")\n\t}',
     '\tif false {\n\t\treturn errors.New("stackName is required")\n\t}',
     "TestUpsertRequiresAStackName", 1),

    ("tenant", "missing tenant is an error", TENANT, TENANT_PKG,
     "\t\tif errors.Is(err, storage.ErrNotFound) {\n\t\t\treturn false, nil\n\t\t}",
     "\t\tif false {\n\t\t\treturn false, nil\n\t\t}",
     "TestGetMissingTenantIsNotAnError", 1),

    ("tenant", "SoftDelete discards the record", TENANT, TENANT_PKG,
     "\trec.SoftDeleted = true\n\treturn b.Upsert(ctx, rec)",
     "\t_ = rec\n\treturn b.Upsert(ctx, Record{StackName: stackName, SoftDeleted: true})",
     "TestSoftDeleteKeepsTheRow", 1),

    # ------------------------------------------------------------------ tick
    ("tick", "any token may drive the drain", TICK, API_PKG,
     'if claims == nil || claims.Op != "scheduler" {', "if claims == nil {",
     "TestDeliveryTick_RequiresSchedulerOp", 1),

    ("tick", "drain error swallowed as 200", TICK, API_PKG,
     "\t\tres, err := drainer.Drain(r.Context())\n\t\tif err != nil {",
     "\t\tres, err := drainer.Drain(r.Context())\n\t\tif err != nil && false {",
     "TestDeliveryTick_DrainErrorIs500", 1),

    # --------------------------------------------------------------- session
    # The operator login flow and the op-gating it makes possible. Both
    # halves were MISSING from the first deploy (2026-11-23): the SPA asked
    # the operator to paste a hand-minted JWT into
    # `localStorage.notifierToken`, while NOTIFIER_OPERATOR_PASSWORD was
    # resolved at boot and then read by nothing.
    #
    # The first entry is the one that matters. GET /api/v1/tenants shipped
    # with no op check and a comment saying "a signed token is enough for
    # the notifier" — but EVERY publisher stack's plugin holds a valid
    # `event` token for this service, and those are scoped to one tenant
    # precisely so a publisher cannot reach past itself. An ungated read
    # route handed that back.
    ("session", "any valid token may enumerate tenants", TENANTS_LIST, API_PKG,
     "\t\tif !requireOperator(w, r) {\n\t\t\treturn\n\t\t}\n", "",
     "TestTenantsList_OnlyAnOperatorTokenMayEnumerateTenants", 1),

    ("session", "requireOperator checks validity, not purpose", JWTMW, API_PKG,
     "if claims == nil || claims.Op != OpOperator {", "if claims == nil {",
     "TestTenantsList_OnlyAnOperatorTokenMayEnumerateTenants", 1),

    ("session", "session probe checks validity, not purpose", SESSION, API_PKG,
     "return err == nil && tok.Valid && claims.Op == OpOperator",
     "return err == nil && tok.Valid",
     "TestSession_AnEventTokenIsNotAnOperatorSession", 1),

    # An unset password is a misconfigured deploy; a wrong one is a typo.
    # Answering both the same way sends an operator hunting for a typo
    # that cannot exist, because `operatorPassword` is optional.
    ("session", "unset password answers like a wrong one", SESSION, API_PKG,
     'writeErr(w, http.StatusForbidden, "NOTIFIER_OPERATOR_PASSWORD not set on the server")',
     'writeErr(w, http.StatusUnauthorized, "wrong password")',
     "TestLogin_NoPasswordConfiguredIs403NotJust401", 1),

    # Compares body.Password against ITSELF rather than deleting the
    # comparison: dropping it entirely leaves `subtle` unused and the
    # mutation reports INVALID instead of proving anything. Live-caught
    # while writing this entry.
    ("session", "password never actually compared", SESSION, API_PKG,
     "[]byte(c.OperatorPassword)) != 1", "[]byte(body.Password)) != 1",
     "TestLogin_WrongPasswordIs401AndSetsNoCookie", 1),

    # HttpOnly is the whole reason the token left localStorage: this one
    # is signed with the key that also seals every tenant's channel
    # credentials, so a page script must not be able to read it.
    ("session", "session cookie readable from JavaScript", SESSION, API_PKG,
     "\t\tHttpOnly: true,\n\t\tSecure:   c.CookieSecure,\n\t\tSameSite: http.SameSiteLaxMode,\n\t\tExpires",
     "\t\tHttpOnly: false,\n\t\tSecure:   c.CookieSecure,\n\t\tSameSite: http.SameSiteLaxMode,\n\t\tExpires",
     "TestLogin_RightPasswordSetsAnHTTPOnlySessionCookie", 1),

    ("session", "logout leaves the cookie alive", SESSION, API_PKG,
     "\t\tMaxAge:   -1,", "\t\tMaxAge:   3600,",
     "TestLogout_ClearsTheCookieAndIsIdempotent", 1),

    # Without the cookie transport the SPA cannot authenticate at all — a
    # browser cannot set an Authorization header on a page load. This is
    # why the first deploy reached for localStorage.
    ("session", "cookie transport removed", JWTMW, API_PKG,
     '\tif c, err := r.Cookie(SessionCookieName); err == nil && c.Value != "" {\n\t\treturn c.Value\n\t}\n',
     "",
     "TestJWTMiddleware_AcceptsTheSessionCookieAndPrefersTheHeader", 1),

    # Precedence matters the other way too: a curl with an explicit Bearer
    # token must not be silently overridden by whatever session cookie the
    # browser happens to hold.
    ("session", "cookie takes precedence over the Bearer header", JWTMW, API_PKG,
     '\tif authz := r.Header.Get("Authorization"); strings.HasPrefix(authz, "Bearer ") {\n'
     '\t\treturn strings.TrimPrefix(authz, "Bearer ")\n\t}\n'
     '\tif c, err := r.Cookie(SessionCookieName); err == nil && c.Value != "" {\n\t\treturn c.Value\n\t}\n',
     '\tif c, err := r.Cookie(SessionCookieName); err == nil && c.Value != "" {\n\t\treturn c.Value\n\t}\n'
     '\tif authz := r.Header.Get("Authorization"); strings.HasPrefix(authz, "Bearer ") {\n'
     '\t\treturn strings.TrimPrefix(authz, "Bearer ")\n\t}\n',
     "TestJWTMiddleware_AcceptsTheSessionCookieAndPrefersTheHeader", 1),

    # ---------------------------------------------------------------- plugin
    # The dead branch, restored as a mutation. Before 2026-11-23
    # lookupDepString asserted the *api.ResourceOutput POINTER against
    # map[string]any — a struct pointer can never satisfy a map type, so
    # the first (most trustworthy) branch of readServiceURL's cascade was
    # unreachable and a live `_endpoint_url` was always ignored in favour
    # of `ingress.hostname`. Nothing went red, because no test built a
    # dependency-output fixture the way the bus builds one. This entry
    # exists so re-introducing the same assertion cannot go unnoticed.
    ("plugin", "dependency outputs read off the pointer, not .Ref", PLG_CONFIG, PLUGIN_PKG,
     "\t\tswitch ref := out.Ref.(type) {\n\t\tcase *api.PluginResourceOutput:",
     "\t\tswitch ref := any(out).(type) {\n\t\tcase *api.PluginResourceOutput:",
     "TestDependencyEndpointURLWinsOverIngressHostname", 1),

    # Trimming slashes BEFORE whitespace is a no-op on a value with a
    # trailing space, so `"https://host/ "` kept its slash and every
    # event POST would go to `//api/v1/events`. This was the shipped
    # order until TestNormalizeURL caught it on its first run.
    ("plugin", "URL trimmed in the wrong order", PLG_CONFIG, PLUGIN_PKG,
     's = strings.TrimRight(strings.TrimSpace(s), "/")',
     's = strings.TrimSpace(strings.TrimRight(s, "/"))',
     "TestNormalizeURL", 1),

    # The worst available outcome: a publisher that deploys green with
    # notifications pointing nowhere. Nobody finds out until the
    # incident, which is the whole reason this component exists.
    ("plugin", "unresolvable service URL defaults instead of failing", PLG_CONFIG, PLUGIN_PKG,
     '\treturn "", errors.New(\n'
     '\t\t"cannot resolve the notifier service URL: no `_endpoint_url`/`_deployed_service` in the dependency outputs '
     'and no `config.ingress.hostname` on the component",\n\t)',
     '\treturn "https://notifier.invalid", nil',
     "TestUnresolvableServiceURLFailsAndNamesBothOptions", 1),

    ("plugin", "blank signingKey accepted", PLG_CONFIG, PLUGIN_PKG,
     '\tif v, ok := configRaw(input)["signingKey"].(string); ok && v != "" {',
     '\tif v, ok := configRaw(input)["signingKey"].(string); ok || v == "" {',
     "TestSigningKeyIsRequired", 1),

    # A single hyphen collides with a resource name that itself ends in
    # `-staging`: the component would then read and write a bucket
    # belonging to something else.
    ("plugin", "bucket separator single hyphen", PLG_CONFIG, PLUGIN_PKG,
     '\treturn resourceName + "--" + env',
     '\treturn resourceName + "-" + env',
     "TestResourceBucketNameUsesTheDoubleHyphenSeparator", 1),

    # `cooldown: 900` arrives as a float64 after the bus's JSON
    # round-trip. Rendering it with %v gives "900" with no unit, and the
    # service's duration parse then exits 2 at boot — the notifier never
    # starts, which is the one outcome worse than a missed alert.
    ("plugin", "bare-number cooldown rendered without a unit", PLG_CONFIG, PLUGIN_PKG,
     '\t\treturn fmt.Sprintf("%ds", int64(v))',
     '\t\treturn fmt.Sprintf("%v", v)',
     "TestCooldownFromConfigRendersADurationStringFromEveryYAMLShape", 1),

    ("plugin", "unresolved placeholder passed through", PLG_CHANNELS, PLUGIN_PKG,
     "\t\tif err := rejectUnresolved(ch); err != nil {\n\t\t\treturn \"\", nil, err\n\t\t}",
     "\t\tif err := rejectUnresolved(ch); err != nil && false {\n\t\t\treturn \"\", nil, err\n\t\t}",
     "TestUnresolvedPlaceholderFailsTheProvision", 1),

    # A SecretsManager ARN contains no placeholder marker, but a marker
    # list that grew an over-broad entry would start rejecting it — and
    # the ARN form is the only one that keeps a bot token out of the
    # function's environment.
    ("plugin", "ARN credentials rejected as placeholders", PLG_CHANNELS, PLUGIN_PKG,
     'var unresolvedMarkers = []string{"${secret:", "${resource:", "$$sc:", "${auth:"}',
     'var unresolvedMarkers = []string{"${secret:", "${resource:", "$$sc:", "${auth:", "arn:"}',
     "TestSecretARNIsAcceptedAndClassified", 1),

    ("plugin", "duplicate channel names allowed", PLG_CHANNELS, PLUGIN_PKG,
     "\t\tif _, dup := seen[ch.Name]; dup {",
     "\t\tif _, dup := seen[ch.Name]; dup && false {",
     "TestInvalidChannelFailsRatherThanDeployingQuietly", 1),

    ("plugin", "invalid channel deployed quietly", PLG_CHANNELS, PLUGIN_PKG,
     "\t\tif err := ch.Validate(); err != nil {\n\t\t\treturn \"\", nil, err\n\t\t}",
     "\t\tif err := ch.Validate(); err != nil && false {\n\t\t\treturn \"\", nil, err\n\t\t}",
     "TestInvalidChannelFailsRatherThanDeployingQuietly", 1),

    # Resolving the default here would put the same rule in two places;
    # the day they disagree, an operator's channel changes behaviour on a
    # plugin upgrade they did not ask for.
    ("plugin", "minSeverity resolved in the plugin, not the service", PLG_CHANNELS, PLUGIN_PKG,
     "\tout, err := json.Marshal(channels)",
     "\tfor i := range channels {\n\t\tchannels[i].MinSeverity = channels[i].ResolvedMinSeverity()\n\t}\n"
     "\tout, err := json.Marshal(channels)",
     "TestBlankMinSeverityIsNotResolvedIntoThePayload", 1),

    # Prefix matching would admit `staging-ru` — a real and separate
    # environment here — and, worse, a workflow-id stack on any operator
    # whose environment name happens to be a prefix of it.
    ("plugin", "environment allowlist matches by prefix", PLG_TENANTENV, PLUGIN_PKG,
     "\t\tif strings.TrimSpace(a) == env {",
     "\t\tif strings.HasPrefix(env, strings.TrimSpace(a)) {",
     "TestAllowlistMatchesExactlyNotByPrefix", 1),

    ("plugin", "empty allowlist locks everything out", PLG_TENANTENV, PLUGIN_PKG,
     "\tif len(allowed) == 0 {\n\t\treturn true\n\t}",
     "\tif len(allowed) == 0 {\n\t\treturn false\n\t}",
     "TestEmptyAllowlistAllowsEveryEnvironment", 1),

    # A typo'd entry must not become a filter that excludes everything:
    # that turns one character into "no publisher anywhere registers",
    # with no error and no alert.
    ("plugin", "non-string allowlist entry becomes a blank entry", PLG_TENANTENV, PLUGIN_PKG,
     '\t\t\tif s, ok := item.(string); ok && strings.TrimSpace(s) != "" {\n'
     "\t\t\t\tout = append(out, strings.TrimSpace(s))\n\t\t\t}",
     "\t\t\ts, _ := item.(string)\n\t\t\tout = append(out, strings.TrimSpace(s))",
     "TestNonStringEntriesAreSkippedNotFatal", 1),

    # The publisher's event token is minted once at deploy time and
    # nothing re-mints it. An `exp` here is the date that publisher's
    # alerting goes quiet — and "add an exp like every other token" is
    # exactly the well-meaning edit that would do it.
    ("plugin", "publisher event token given an expiry", PLG_COMPUTE, PLUGIN_PKG,
     '\t\t"op":     "event",\n\t\t"tenant": tenant,\n\t\t"iat":    now.Unix(),\n\t\t"nbf":    now.Unix(),',
     '\t\t"op":     "event",\n\t\t"tenant": tenant,\n\t\t"iat":    now.Unix(),\n'
     '\t\t"exp":    now.Add(24 * time.Hour).Unix(),\n\t\t"nbf":    now.Unix(),',
     "TestMintEventJWTNeverExpires", 1),

    ("plugin", "event token minted with the register op", PLG_COMPUTE, PLUGIN_PKG,
     '\t\t"op":     "event",', '\t\t"op":     "register",',
     "TestMintEventJWTNeverExpires", 1),

    # A 401/403 on registration means the signing key does not match the
    # deployed service's. Retrying it three times buries the one error
    # message that names the actual fault.
    ("plugin", "4xx retried like a transient failure", PLG_COMPUTE, PLUGIN_PKG,
     "\t\tif status >= 400 && status < 500 && status != 429 {",
     "\t\tif false {",
     "TestPushRegister_NonRetryable4xx", 1),

    # ------------------------------------------------------- plugin: wiring
    # The `SC_` prefix is the property that makes auto-injection safe
    # rather than merely convenient: core refuses an injected name outside
    # it, so no provider plugin can shadow an application's own NOTIFY_URL
    # or DATABASE_URL. Dropping the prefix here is the edit that looks like
    # tidying.
    ("plugin", "injected env name loses the SC_ prefix", PLG_COMPUTE, PLUGIN_PKG,
     '\tnotifyURLVar   = "SC_NOTIFY_URL"   // plain',
     '\tnotifyURLVar   = "NOTIFY_URL"   // plain',
     "TestProcessInjectsPrefixedWiring", 1),

    # The token is a bearer credential for as long as the deploy lives. As
    # a plain env var it sits in the function's configuration in the clear
    # instead of in SecretsManager, and the deploy log reads identically.
    ("plugin", "event token injected as a plain env var", PLG_COMPUTE, PLUGIN_PKG,
     "\tcollector.AddSecretEnvVariableIfNotExist(notifyTokenVar, eventToken, resType, resName, stackName)",
     "\tcollector.AddEnvVariableIfNotExist(notifyTokenVar, eventToken, resType, resName, stackName)",
     "TestProcessInjectsPrefixedWiring", 1),

    # Same mutation, different test: this one proves the secret FLAG
    # survives marshalling into ComputeProcessResponse. The engine-side
    # collector and the plugin-side one are different implementations, so
    # a test against the former says nothing about the live bus.
    ("plugin", "secret flag lost crossing the plugin bus", PLG_COMPUTE, PLUGIN_PKG,
     "\tcollector.AddSecretEnvVariableIfNotExist(notifyTokenVar, eventToken, resType, resName, stackName)",
     "\tcollector.AddEnvVariableIfNotExist(notifyTokenVar, eventToken, resType, resName, stackName)",
     "TestWiringSurvivesThePluginBus", 1),

    # Without the extension, `${resource:my-notifier.token}` resolves to
    # nothing — and ResolvePlaceholders fails OPEN, so the literal string
    # deploys cleanly and surfaces as a 401 at the first incident.
    ("plugin", "template extension never published", PLG_COMPUTE, PLUGIN_PKG,
     "\tcollector.AddResourceTplExtension(resName, map[string]string{\n"
     "\t\ttplKeyURL:   serviceURL,\n\t\ttplKeyToken: eventToken,\n\t})",
     "\t_ = collector",
     "TestProcessPublishesResolvablePlaceholders", 1),

    ("plugin", "placeholder published under the wrong key", PLG_COMPUTE, PLUGIN_PKG,
     "\t\ttplKeyURL:   serviceURL,", '\t\t"self-url":   serviceURL,',
     "TestProcessPublishesResolvablePlaceholders", 1),

    # The whole point of a per-consumer token: the ingest handler takes an
    # event's `source` from the `tenant` claim, so one claim shared across
    # publishers means every alert is attributed to whichever consumer the
    # claim names, and a channel's `sources:` allowlist stops meaning what
    # it says. Invisible to a single-publisher smoke.
    ("plugin", "every consumer's token carries the same tenant", PLG_COMPUTE, PLUGIN_PKG,
     "\teventToken, err := mintEventJWT(signingKey, stackName)",
     "\teventToken, err := mintEventJWT(signingKey, resName)",
     "TestTokensAreScopedPerConsumer", 1),

    # Restores the pre-2026-11-23 behaviour. It is a cold-start deadlock
    # once a sibling COMPONENT can be a consumer: on a first provision the
    # notifier's Lambda does not exist yet, so every publisher in the stack
    # fails — including on the provision that creates the notifier.
    ("plugin", "registration failure fails the deploy again", PLG_COMPUTE, PLUGIN_PKG,
     "\tif err := p.pushRegister(ctx.Context(), serviceURL, registerToken, record); err != nil {\n"
     "\t\tif p.cfg.Progress != nil {\n"
     "\t\t\tp.cfg.Progress.Error(\"compute-process\", fmt.Sprintf(\n"
     "\t\t\t\t\"tenant %q not pre-registered at %s after %d attempts: %v — wiring injected anyway; "
     "the tenant row is created on its first event\",\n"
     "\t\t\t\tstackName, serviceURL, maxAttempts, err,\n"
     "\t\t\t))\n\t\t}\n\t} else if p.cfg.Progress != nil {\n"
     "\t\tp.cfg.Progress.Info(\"compute-process\", fmt.Sprintf(\"registered tenant %q at %s\", stackName, serviceURL))\n\t}",
     "\tif err := p.pushRegister(ctx.Context(), serviceURL, registerToken, record); err != nil {\n"
     "\t\treturn nil, err\n\t}",
     "TestRegistrationFailureStillInjectsWiring", 1),

    # The other side of that line, and the one that must NOT soften:
    # unresolvable wiring hard-fails. No URL means the publisher can never
    # alert anyone, and a notifier that silently does not notify is the
    # worst outcome available.
    ("plugin", "unresolvable service URL warns instead of failing", PLG_COMPUTE, PLUGIN_PKG,
     "\tserviceURL, err := p.readServiceURL(input, params)\n\tif err != nil {\n"
     '\t\treturn nil, errors.Wrapf(err, "component %q unable to resolve service URL", resName)\n\t}',
     "\tserviceURL, _ := p.readServiceURL(input, params)",
     "TestUnresolvableWiringStillFailsTheDeploy", 1),

    # ------------------------------------------------------- secret-env wire
    # Schema() is the component's published API and it is two plain maps
    # with nothing keeping them consistent. Both ways they drift are
    # silent at deploy time.

    # Core dual-uses ComponentAllowedOverrides as the allowed-top-level-
    # keys check, so a bound key that is not on it is refused at PARSE
    # time with "unknown or disallowed top-level key" — the operator
    # cannot write the line, and the binding can never fire.
    ("plugin", "secret bound to a key the operator cannot write", PLG_MAIN, PLG_CMD_PKG,
     '\t\t\t\t"migrationToken",\n', "",
     "TestSecretEnvKeysAreOperatorWritable", 1),

    ("plugin", "two config keys bound to one env var", PLG_MAIN, PLG_CMD_PKG,
     '"migrationToken": "ADMIN_MIGRATION_TOKEN"', '"migrationToken": "SERVICE_JWT_SECRET"',
     "TestSecretEnvVarNamesAreSane", 1),

    # The cross-module one, and the only mutation here that names its
    # test's module explicitly. A bound secret arrives as a
    # SecretsManager ARN; a service that reads it with a bare os.Getenv
    # compares against the ARN string forever, logs nothing, and looks
    # exactly like a working deploy. Live-caught twice already —
    # signingKey on backup-smoke-1d 2026-11-14, then
    # NOTIFIER_OPERATOR_PASSWORD on the first backup.simple-forge.com
    # login 2026-11-18.
    ("plugin", "bound secret read as raw env by the service", SVC_MAIN, PLG_CMD_PKG_XMOD,
     '\t\tadminToken, rErr := resolveAWSSecretARN(ctx, "ADMIN_MIGRATION_TOKEN", rawAdminToken)',
     "\t\tadminToken, rErr := rawAdminToken, error(nil)",
     "TestSecretEnvVarsAreResolvedByTheService", 1),

    # ------------------------------------------------------- goembedbadmin
    # The migration endpoint reads every key out of S3 and upserts it
    # into the live store, and it is deliberately NOT behind the service
    # JWT. The three checks at the top of Migrate are its entire access
    # control.
    ("goembedbadmin", "migration token compare accepts anything", ADMIN, ADMIN_PKG,
     "[]byte(c.AdminToken)) != 1 {", "[]byte(c.AdminToken)) == -1 {",
     "TestMigrate_WrongOrMissingToken_Is401", 1),

    # The other direction: a compare that rejects a correct token would
    # make the cutover impossible, and "my token is wrong" is the first
    # thing an operator would believe.
    ("goembedbadmin", "migration token compare rejects everything", ADMIN, ADMIN_PKG,
     "[]byte(c.AdminToken)) != 1 {", "[]byte(c.AdminToken)) != -1 {",
     "TestMigrate_MatchingToken_PassesAuth", 1),

    # Removing the opt-in gate does not open the route to anyone with a
    # token — it opens it to a request with NO header at all, because an
    # empty configured token and an empty header compare equal. So the
    # gate is not politeness about 404-vs-401; it is the thing standing
    # between an unconfigured deploy and an unauthenticated migration.
    ("goembedbadmin", "opt-in gate removed", ADMIN, ADMIN_PKG,
     '\tif c.AdminToken == "" {\n'
     '\t\twriteErr(w, http.StatusNotFound, "migration endpoint disabled (ADMIN_MIGRATION_TOKEN unset)")\n'
     "\t\treturn\n\t}\n",
     "",
     "TestMigrate_NoTokenConfigured_Is404", 1),

    # The build stamp. Three guards, and the surface that matters is
    # /healthz: it is the only probe a post-deploy smoke can reach with
    # no credential at all, which is the whole reason the field exists.
    ("session", "healthz stops reporting the build", HEALTHZ, API_PKG,
     '\t\t\t"version": version,\n', "",
     "TestHealthz_CarriesTheBuildAndStillSaysOK", 1),

    # The status field is what the Lambda health check and
    # lambda_runtime_test.go match on, and renaming it would leave a
    # 200 with a plausible-looking body — a probe that passes while
    # saying nothing.
    ("session", "healthz stops saying ok", HEALTHZ, API_PKG,
     '"status":  "ok",', '"state":  "ok",',
     "TestHealthz_CarriesTheBuildAndStillSaysOK", 1),

    ("session", "session probe stops reporting the build", SESSION, API_PKG,
     '\t\t"version": c.Version,\n', "",
     "TestSession_ReportsTheBuildVersion", 1),

    # And the two wires. Both are in main.go and NEITHER is visible to a
    # handler test: every test in internal/api constructs its own
    # handler, so all of them pass with main.go never passing `version`
    # at all. That is the exact shape of the bug this repo has shipped
    # twice (`signingKey` as a bare ARN, then
    # NOTIFIER_OPERATOR_PASSWORD) — a value present, correct, and read
    # by nothing, logging identically to a working deploy. Hence a
    # binary test.
    ("session", "healthz wired without the version", SVC_MAIN, SVC_CMD_PKG,
     "mux.Get(\"/healthz\", api.Healthz(version))",
     "mux.Get(\"/healthz\", api.Healthz(\"\"))",
     "TestBinaryReportsItsBuildOnHealthzAndSession", 1),

    ("session", "session wired without the version", SVC_MAIN, SVC_CMD_PKG,
     "\t\tVersion: version,\n", "",
     "TestBinaryReportsItsBuildOnHealthzAndSession", 1),
]

TEST_FILE_PREFIXES = (
    "drain_test.go:", "router_test.go:", "store_blob_test.go:",
    "telegram_test.go:", "webhook_test.go:", "seal_test.go:", "store_blob_test.go:",
    "registry_test.go:", "events_ingest_test.go:", "scheduler_tick_test.go:",
    "config_test.go:", "channels_cfg_test.go:", "tenant_env_test.go:", "compute_test.go:",
    "compute_wiring_test.go:", "schema_test.go:", "migration_auth_test.go:",
    "session_test.go:", "version_wire_test.go:",
)
COMPILE_MARKERS = ("build failed", "undefined:", "declared and not used", "cannot use")


def first_message(out: str) -> tuple[str, bool]:
    """Return (message, is_compile_error) from a go test failure.

    A t.Fatalf message sits on the same line as the file:line prefix, but
    gomega puts nothing there and wraps its "Expected / to equal" report
    across the following indented lines. Both shapes have to read as one
    sentence here, or the api-package mutations report a bare line number
    and the operator has to re-run the test by hand to learn what broke.
    """
    lines = out.splitlines()
    for i, line in enumerate(lines):
        s = line.strip()
        if s.startswith(TEST_FILE_PREFIXES):
            msg = s.split(": ", 1)[-1] if ": " in s else ""
            if msg:
                return msg, False
            tail = [t.strip() for t in lines[i + 1:i + 7]
                    if t.strip() and not t.startswith(("FAIL", "ok", "---", "===", "exit status"))]
            return " ".join(tail) or s, False
        if any(m in s for m in COMPILE_MARKERS):
            return s, True
    return "", False


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    listing = "-l" in sys.argv or "--list" in sys.argv

    muts = [m for m in MUTATIONS if not args or m[0] in args]
    if not muts:
        groups = sorted({m[0] for m in MUTATIONS})
        print(f"no mutations match {args}; groups are: {', '.join(groups)}")
        return 2

    if listing:
        for group, name, _, _, _, _, test, _ in muts:
            print(f"{group:9} {name:48} {test}")
        return 0

    # SIGTERM does not raise, so the restore in `finally` below never runs
    # and the tree is left with exactly one file mutated — which then shows
    # up as an unrelated failure in whatever runs next. Turning it into
    # SystemExit makes the restore happen. Live-caught 2026-11-23: a
    # two-minute wrapper timeout on the full run left
    # plugin/internal/channels_cfg.go mutated, and only `git status` said so.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(130))

    # Back up exactly the files this run will mutate, rather than each
    # module's whole internal/ tree. Guards worth mutating also live
    # OUTSIDE internal/ — Schema's allowlist and secret-env map in
    # plugin/cmd/, the secret resolve in service/cmd/ — and with a
    # tree copy keyed on internal/ the restore below raises
    # FileNotFoundError for those and leaves the file mutated. Backing
    # up by path covers both, and the harness only ever writes files
    # named in MUTATIONS, so it loses no insurance.
    backup = pathlib.Path(tempfile.mkdtemp(prefix="mutation-check-"))
    mutated_files = sorted({m[2] for m in muts})
    for rel in mutated_files:
        (backup / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / rel, backup / rel)

    problems = []
    try:
        for group, name, rel, pkg, find, repl, test, want in muts:
            label = f"{group}/{name}"
            path = ROOT / rel
            # The module the file belongs to is normally also the
            # directory the test has to run in: each module has its own
            # go.mod, so `go test ./internal/` resolves differently per
            # module. The exception is a cross-module contract test (the
            # plugin asserting something about the service's source),
            # which names its module as "<module>:<pkg>".
            if ":" in pkg:
                mod_name, pkg = pkg.split(":", 1)
            else:
                mod_name = rel.split("/", 1)[0]
            module = ROOT / mod_name
            src = path.read_text()
            if src.count(find) != want:
                print(f"STALE       {label}")
                print(f"              pattern occurs {src.count(find)}x, expected {want} — "
                      f"the code moved; update the mutation")
                problems.append(label)
                continue

            path.write_text(src.replace(find, repl))
            try:
                proc = subprocess.run(
                    ["go", "test", pkg, "-count=1", "-run", test],
                    cwd=module, capture_output=True, text=True, timeout=180,
                )
            except subprocess.TimeoutExpired:
                print(f"HUNG        {label}")
                print("              the test deadlocks instead of failing — it would burn the CI timeout")
                problems.append(label)
                continue
            finally:
                shutil.copyfile(backup / rel, path)

            if proc.returncode == 0:
                print(f"NOT CAUGHT  {label}")
                print(f"              {test} PASSES with the guard broken — it proves nothing")
                problems.append(label)
                continue

            msg, compile_err = first_message(proc.stdout + proc.stderr)
            if compile_err:
                print(f"INVALID     {label}")
                print(f"              the mutation does not compile, so nothing was proven: {msg}")
                problems.append(label)
                continue

            print(f"caught      {label}")
            if msg:
                print(f"              → {msg[:160]}")
    finally:
        # Belt and braces: restore every file this run could have
        # touched, so a crash or a Ctrl-C mid-run cannot leave a mutated
        # file behind. By path, for the same reason the backup is — the
        # earlier version rmtree'd each module's internal/ and copied a
        # per-module tree back, which on a filtered run (`mutation-check.py
        # goembedbadmin`, one backed-up file under service/internal)
        # DELETED the rest of that tree and then raised
        # FileNotFoundError on a module with no backup at all. A cleanup
        # path that can destroy source is worse than no cleanup path.
        for rel in mutated_files:
            shutil.copyfile(backup / rel, ROOT / rel)
        shutil.rmtree(backup, ignore_errors=True)

    print()
    if problems:
        print(f"{len(problems)} of {len(muts)} not proven:")
        for p in problems:
            print(f"  - {p}")
        return 1
    print(f"all {len(muts)} mutations caught")
    return 0


if __name__ == "__main__":
    sys.exit(main())
