// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package registry

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/simple-container-com/sc-component-notifier/service/internal/channel"
	"github.com/simple-container-com/sc-component-notifier/service/internal/crypto"
	"github.com/simple-container-com/sc-component-notifier/service/internal/events"
	"github.com/simple-container-com/sc-component-notifier/service/internal/storage"
)

const testKEK = "a-fake-signing-key-that-is-at-least-32-bytes-long"

// botToken is the value the "never persisted" assertions scan for. Kept
// distinctive so a substring search cannot match by accident.
const botToken = "123456:UNMISTAKABLE-BOT-TOKEN"

type capturingLog struct {
	infos  []string
	errors []string
}

func (l *capturingLog) Infof(format string, args ...any) {
	l.infos = append(l.infos, fmt.Sprintf(format, args...))
}

func (l *capturingLog) Errorf(format string, args ...any) {
	l.errors = append(l.errors, fmt.Sprintf(format, args...))
}

func (l *capturingLog) contains(lines []string, substr string) bool {
	for _, s := range lines {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

type fixture struct {
	t       *testing.T
	objects *storage.InMemoryStore
	store   *events.BlobStore
	log     *capturingLog
	reg     *Registry
}

func newFixture(t *testing.T, sealer *crypto.Sealer, managed ...Managed) *fixture {
	t.Helper()
	objects := storage.NewInMemoryStore()
	store := events.NewBlobStore(objects, "")
	log := &capturingLog{}
	return &fixture{
		t:       t,
		objects: objects,
		store:   store,
		log:     log,
		reg: New(Config{
			Store:   store,
			Sealer:  sealer,
			Managed: managed,
			Log:     log,
			Now:     func() time.Time { return time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC) },
		}),
	}
}

func testSealer(t *testing.T) *crypto.Sealer {
	t.Helper()
	s, err := crypto.NewSealer([]byte(testKEK))
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	return s
}

func telegramManaged(name string) Managed {
	return Managed{Name: name, Type: TypeTelegram, Credential: botToken, ChatID: "-100123"}
}

// everyStoredByte concatenates every object in the store, so an
// assertion can ask "does the credential appear ANYWHERE at rest"
// without depending on which key it would have been written under.
func (f *fixture) everyStoredByte() []byte {
	f.t.Helper()
	objs, err := f.objects.List(context.Background(), "")
	if err != nil {
		f.t.Fatalf("list objects: %v", err)
	}
	var all bytes.Buffer
	for _, o := range objs {
		rc, err := f.objects.Get(context.Background(), o.Path)
		if err != nil {
			f.t.Fatalf("get %s: %v", o.Path, err)
		}
		if _, err := all.ReadFrom(rc); err != nil {
			f.t.Fatalf("read %s: %v", o.Path, err)
		}
		_ = rc.Close()
	}
	return all.Bytes()
}

// The headline guard for declarative channels: the credential is
// resolved at boot from the SC registry and held in memory. Persisting
// it would create a second copy of a secret that already has an owner —
// one that goes stale on rotation while still looking valid, and that
// lands in an object store whose access rules are a separate question
// from the secrets registry's.
func TestManagedCredentialIsNeverPersisted(t *testing.T) {
	f := newFixture(t, testSealer(t), telegramManaged("ops-telegram"))
	ctx := context.Background()

	if err := f.reg.Seed(ctx); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	rec, err := f.store.GetChannel(ctx, "ops-telegram")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if rec == nil {
		t.Fatal("Seed did not write the declared channel")
	}
	if !rec.Managed {
		t.Fatal("the seeded record is not marked managed, so the SPA would offer to edit it")
	}
	if rec.HasCredential() {
		t.Fatal("the seeded record carries a credential")
	}
	if stored := f.everyStoredByte(); bytes.Contains(stored, []byte(botToken)) {
		t.Fatal("the bot token appears in the object store")
	}

	// ...and the channel is still usable, because the credential came
	// from memory. Without this half, "not persisted" could be passing
	// for the trivial reason that the token was never loaded.
	chans, err := f.reg.Channels(ctx)
	if err != nil {
		t.Fatalf("Channels: %v", err)
	}
	if _, ok := chans["ops-telegram"]; !ok {
		t.Fatalf("declared channel is not live; log: %v", f.log.errors)
	}
}

// Removing a channel from server.yaml must remove it, not orphan it.
// Otherwise an operator who deletes a channel from their config keeps
// receiving messages from it, with nothing in the config to explain why.
func TestUndeclaredManagedChannelIsRemoved(t *testing.T) {
	ctx := context.Background()

	// Boot once with two channels.
	f := newFixture(t, testSealer(t), telegramManaged("ops-telegram"), telegramManaged("old-telegram"))
	if err := f.reg.Seed(ctx); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if recs, _ := f.store.ListChannels(ctx); len(recs) != 2 {
		t.Fatalf("first boot stored %d channels, want 2", len(recs))
	}

	// Boot again against the same storage with only one declared — the
	// real sequence, since storage outlives the process.
	second := New(Config{Store: f.store, Sealer: testSealer(t), Log: f.log, Managed: []Managed{telegramManaged("ops-telegram")}})
	if err := second.Seed(ctx); err != nil {
		t.Fatalf("Seed (second boot): %v", err)
	}

	recs, err := f.store.ListChannels(ctx)
	if err != nil {
		t.Fatalf("ListChannels: %v", err)
	}
	if len(recs) != 1 || recs[0].Name != "ops-telegram" {
		names := make([]string, len(recs))
		for i, r := range recs {
			names[i] = r.Name
		}
		t.Fatalf("after the second boot the stored channels are %v, want [ops-telegram]", names)
	}

	// And it must not still be routable.
	routes, err := second.Routes(ctx)
	if err != nil {
		t.Fatalf("Routes: %v", err)
	}
	for _, r := range routes {
		if r.Channel == "old-telegram" {
			t.Fatal("the removed channel is still a live route")
		}
	}
}

// The mirror of the above: Seed must not touch SPA-created channels. A
// reconcile that deleted everything it did not declare would wipe the
// operator's UI-created channels on every deploy.
func TestSeedLeavesSPAChannelsAlone(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, testSealer(t), telegramManaged("ops-telegram"))

	sealed, err := testSealer(t).Seal(CredentialScope, "spa-token")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	spa := &events.ChannelRecord{
		Name: "my-own-chat", Type: TypeTelegram, ChatID: "-100999",
		Credential: sealed, Enabled: true, Managed: false,
	}
	if err := f.store.PutChannel(ctx, spa); err != nil {
		t.Fatalf("PutChannel: %v", err)
	}

	if err := f.reg.Seed(ctx); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	got, err := f.store.GetChannel(ctx, "my-own-chat")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if got == nil {
		t.Fatal("Seed deleted a UI-created channel")
	}
	if got.Managed {
		t.Fatal("Seed converted a UI-created channel to managed")
	}
	if !bytes.Equal(got.Credential, sealed) {
		t.Fatal("Seed altered a UI-created channel's credential")
	}
}

// A name declared in server.yaml that already exists as an SPA channel:
// the declaration wins, because an operator must be able to trust their
// config — but it is logged at ERROR, because a stored credential is
// being discarded and a silent takeover would be indistinguishable from
// the channel simply breaking.
func TestDeclarationTakesOverACollidingSPAName(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, testSealer(t), telegramManaged("ops-telegram"))

	sealed, err := testSealer(t).Seal(CredentialScope, "spa-token")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := f.store.PutChannel(ctx, &events.ChannelRecord{
		Name: "ops-telegram", Type: TypeTelegram, ChatID: "-100999",
		Credential: sealed, Enabled: true, Managed: false,
	}); err != nil {
		t.Fatalf("PutChannel: %v", err)
	}

	if err := f.reg.Seed(ctx); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	rec, err := f.store.GetChannel(ctx, "ops-telegram")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if !rec.Managed {
		t.Fatal("the declaration did not win the collision")
	}
	if rec.ChatID != "-100123" {
		t.Fatalf("chat id is %q, want the declared -100123", rec.ChatID)
	}
	if !f.log.contains(f.log.errors, "declared in server.yaml and also exists") {
		t.Fatalf("the collision was not logged as an error; errors: %v", f.log.errors)
	}
}

// A channel whose credential is missing must be OMITTED from the live
// set, with the reason available — not constructed and left to fail at
// send time, which would burn five retries per event and report the
// failure as transient.
func TestIncompleteChannelIsOmittedWithAReason(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name    string
		managed Managed
		want    string
	}{
		{"no token", Managed{Name: "c", Type: TypeTelegram, ChatID: "-1"}, "no bot token"},
		{"no chat id", Managed{Name: "c", Type: TypeTelegram, Credential: botToken}, "no chat id"},
		{"no url", Managed{Name: "c", Type: TypeWebhook}, "no url"},
		{"unknown type", Managed{Name: "c", Type: "carrier-pigeon"}, "unsupported channel type"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, testSealer(t), tc.managed)
			if err := f.reg.Seed(ctx); err != nil {
				t.Fatalf("Seed: %v", err)
			}

			chans, err := f.reg.Channels(ctx)
			if err != nil {
				t.Fatalf("Channels: %v", err)
			}
			if _, ok := chans["c"]; ok {
				t.Fatal("an unusable channel is in the live set")
			}

			statuses, err := f.reg.Statuses(ctx)
			if err != nil {
				t.Fatalf("Statuses: %v", err)
			}
			if len(statuses) != 1 {
				t.Fatalf("got %d statuses, want 1", len(statuses))
			}
			if statuses[0].Usable {
				t.Fatal("an unusable channel reports usable")
			}
			if !strings.Contains(statuses[0].Reason, tc.want) {
				t.Fatalf("reason is %q, want it to mention %q", statuses[0].Reason, tc.want)
			}
			if !f.log.contains(f.log.errors, "unusable") {
				t.Fatalf("nothing was logged about the unusable channel; errors: %v", f.log.errors)
			}
		})
	}
}

// A webhook with no secret is legitimate (a receiver behind its own
// auth), so it must NOT be treated like Telegram's missing token. The
// signature is simply omitted — asserted over in the webhook package.
func TestWebhookWithoutSecretIsUsable(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, testSealer(t), Managed{Name: "hook", Type: TypeWebhook, URL: "https://example.test/hook"})
	if err := f.reg.Seed(ctx); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	chans, err := f.reg.Channels(ctx)
	if err != nil {
		t.Fatalf("Channels: %v", err)
	}
	if _, ok := chans["hook"]; !ok {
		t.Fatalf("a secretless webhook was rejected; errors: %v", f.log.errors)
	}
}

func TestDisabledChannelIsNotLiveButIsStillListed(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, testSealer(t))

	sealed, err := testSealer(t).Seal(CredentialScope, "spa-token")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := f.store.PutChannel(ctx, &events.ChannelRecord{
		Name: "muted", Type: TypeTelegram, ChatID: "-1",
		Credential: sealed, Enabled: false,
	}); err != nil {
		t.Fatalf("PutChannel: %v", err)
	}

	chans, err := f.reg.Channels(ctx)
	if err != nil {
		t.Fatalf("Channels: %v", err)
	}
	if _, ok := chans["muted"]; ok {
		t.Fatal("a disabled channel is in the live set")
	}

	// Still visible, so an operator can see what they muted. A disabled
	// channel that vanished from the UI could not be re-enabled.
	statuses, err := f.reg.Statuses(ctx)
	if err != nil {
		t.Fatalf("Statuses: %v", err)
	}
	if len(statuses) != 1 || statuses[0].Name != "muted" {
		t.Fatalf("a disabled channel is not listed: %+v", statuses)
	}
	if statuses[0].Usable || statuses[0].Reason == "" {
		t.Fatalf("a disabled channel reports usable=%v reason=%q", statuses[0].Usable, statuses[0].Reason)
	}
}

// An SPA credential must be unsealed before use, and a process with no
// key must say so rather than quietly dropping the channel as if it
// were unconfigured.
func TestSPACredentialIsUnsealed(t *testing.T) {
	ctx := context.Background()
	sealed, err := testSealer(t).Seal(CredentialScope, botToken)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	rec := &events.ChannelRecord{
		Name: "my-own-chat", Type: TypeTelegram, ChatID: "-100999",
		Credential: sealed, Enabled: true,
	}

	withKey := newFixture(t, testSealer(t))
	if err := withKey.store.PutChannel(ctx, rec); err != nil {
		t.Fatalf("PutChannel: %v", err)
	}
	chans, err := withKey.reg.Channels(ctx)
	if err != nil {
		t.Fatalf("Channels: %v", err)
	}
	if _, ok := chans["my-own-chat"]; !ok {
		t.Fatalf("a sealed SPA channel was not unsealed; errors: %v", withKey.log.errors)
	}

	noKey := newFixture(t, nil)
	if err := noKey.store.PutChannel(ctx, rec); err != nil {
		t.Fatalf("PutChannel: %v", err)
	}
	statuses, err := noKey.reg.Statuses(ctx)
	if err != nil {
		t.Fatalf("Statuses: %v", err)
	}
	if statuses[0].Usable {
		t.Fatal("a channel whose credential cannot be opened reports usable")
	}
	if !strings.Contains(statuses[0].Reason, "no encryption key") {
		t.Fatalf("reason is %q, want it to name the missing key", statuses[0].Reason)
	}
}

// Routes must come from the store, so managed and SPA channels pass
// through identical gate logic. A Routes() built from the declaration
// alone would make SPA-created channels silently unroutable — they would
// exist, be listed, hold a credential, and never receive anything.
func TestRoutesCoverBothOrigins(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, testSealer(t), Managed{
		Name: "ops-telegram", Type: TypeTelegram, Credential: botToken, ChatID: "-1",
		MinSeverity: channel.SeverityError, Kinds: []string{events.KindBackupFailed},
	})
	if err := f.reg.Seed(ctx); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if err := f.store.PutChannel(ctx, &events.ChannelRecord{
		Name: "my-own-chat", Type: TypeTelegram, ChatID: "-2", Enabled: true,
	}); err != nil {
		t.Fatalf("PutChannel: %v", err)
	}

	routes, err := f.reg.Routes(ctx)
	if err != nil {
		t.Fatalf("Routes: %v", err)
	}
	byName := map[string]events.Route{}
	for _, r := range routes {
		byName[r.Channel] = r
	}
	if len(byName) != 2 {
		t.Fatalf("got %d routes, want 2 (one per origin): %+v", len(byName), routes)
	}
	if got := byName["ops-telegram"]; got.MinSeverity != channel.SeverityError || len(got.Kinds) != 1 {
		t.Fatalf("the declared channel's gates did not survive: %+v", got)
	}
	if !byName["my-own-chat"].Enabled {
		t.Fatal("the SPA channel's route is not enabled")
	}
}

// Statuses must show the EFFECTIVE minimum severity. A blank field
// reads as "everything gets through", and the one thing this component
// cannot be ambiguous about is which severities are not being
// delivered — the forge-notifier lesson, where every subscription was
// warning-or-above and every info notification silently vanished.
func TestStatusShowsEffectiveMinSeverity(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, testSealer(t), Managed{
		Name: "ops-telegram", Type: TypeTelegram, Credential: botToken, ChatID: "-1",
		// MinSeverity deliberately unset.
	})
	if err := f.reg.Seed(ctx); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	statuses, err := f.reg.Statuses(ctx)
	if err != nil {
		t.Fatalf("Statuses: %v", err)
	}
	if statuses[0].MinSeverity != channel.SeverityWarning {
		t.Fatalf("minSeverity renders as %q, want the effective default %q",
			statuses[0].MinSeverity, channel.SeverityWarning)
	}
}

// A managed record holds no credential, so reporting hasCredential from
// the record alone would tell every operator their declared channel has
// no token — the one field they would then go and "fix".
func TestStatusReportsManagedCredentialFromMemory(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, testSealer(t), telegramManaged("ops-telegram"))
	if err := f.reg.Seed(ctx); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	statuses, err := f.reg.Statuses(ctx)
	if err != nil {
		t.Fatalf("Statuses: %v", err)
	}
	if !statuses[0].HasCredential {
		t.Fatal("a managed channel with a resolved token reports hasCredential=false")
	}
	if !statuses[0].Usable {
		t.Fatalf("a fully declared channel is not usable: %q", statuses[0].Reason)
	}
}

// Channels() is called once per drain tick and must re-read, so a
// credential changed in the SPA takes effect on the next tick rather
// than when the Lambda container happens to recycle.
func TestChannelsReReadsOnEveryCall(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, testSealer(t))

	if chans, err := f.reg.Channels(ctx); err != nil || len(chans) != 0 {
		t.Fatalf("expected an empty live set, got %d (err %v)", len(chans), err)
	}

	sealed, err := testSealer(t).Seal(CredentialScope, botToken)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := f.store.PutChannel(ctx, &events.ChannelRecord{
		Name: "added-later", Type: TypeTelegram, ChatID: "-1",
		Credential: sealed, Enabled: true,
	}); err != nil {
		t.Fatalf("PutChannel: %v", err)
	}

	chans, err := f.reg.Channels(ctx)
	if err != nil {
		t.Fatalf("Channels: %v", err)
	}
	if _, ok := chans["added-later"]; !ok {
		t.Fatal("a channel added after construction is not in the live set — the map is cached")
	}
}
