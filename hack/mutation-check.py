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

Two real test defects were found this way on 2026-11-23, and neither was
visible from a green run:

  * TestMissingCredentialsArePermanent reached the REAL api.telegram.org
    (apiBase was never redirected) and passed only because the live API
    answers 404, which happens to classify as permanent. With the guard
    removed it still passed. Now the base URL points at a closed port.
  * TestSendSurfacesTelegramDescription passed off describeError's
    raw-body fallback rather than its JSON parse, so gutting the parse
    changed nothing. Now it also asserts no raw JSON leaks into the
    message.

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
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent / "service"

EVENTS = "./internal/events/"
TG_PKG = "./internal/channel/telegram/"
WH_PKG = "./internal/channel/webhook/"
CRYPTO_PKG = "./internal/crypto/"
REG_PKG = "./internal/registry/"
API_PKG = "./internal/api/"
TENANT_PKG = "./internal/tenant/"

ROUTER = "internal/events/router.go"
DRAIN = "internal/events/drain.go"
BLOB = "internal/events/store_blob.go"
CHANNEL = "internal/channel/channel.go"
ERRORS = "internal/channel/errors.go"
HTTPC = "internal/channel/http.go"
TG = "internal/channel/telegram/telegram.go"
WH = "internal/channel/webhook/webhook.go"
SEAL = "internal/crypto/seal.go"
REG = "internal/registry/registry.go"
TENANT = "internal/tenant/store_blob.go"
INGEST = "internal/api/events_ingest.go"
TICK = "internal/api/scheduler_tick.go"

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
]

TEST_FILE_PREFIXES = (
    "drain_test.go:", "router_test.go:", "store_blob_test.go:",
    "telegram_test.go:", "webhook_test.go:", "seal_test.go:", "store_blob_test.go:",
    "registry_test.go:", "events_ingest_test.go:", "scheduler_tick_test.go:",
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

    backup = pathlib.Path(tempfile.mkdtemp(prefix="mutation-check-"))
    shutil.copytree(ROOT / "internal", backup / "internal")

    problems = []
    try:
        for group, name, rel, pkg, find, repl, test, want in muts:
            label = f"{group}/{name}"
            path = ROOT / rel
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
                    cwd=ROOT, capture_output=True, text=True, timeout=180,
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
        # Belt and braces: restore everything, so a crash or a Ctrl-C
        # mid-run cannot leave a mutated file behind.
        shutil.rmtree(ROOT / "internal")
        shutil.copytree(backup / "internal", ROOT / "internal")
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
