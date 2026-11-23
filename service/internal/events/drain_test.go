// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package events

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/simple-container-com/sc-component-notifier/service/internal/channel"
	"github.com/simple-container-com/sc-component-notifier/service/internal/storage"
)

// fakeChannel records what it was asked to send and fails on demand.
type fakeChannel struct {
	mu       sync.Mutex
	name     string
	sent     []channel.Message
	failWith error
	// failFirst makes the first N attempts fail, then succeed, so the
	// retry path can be tested without faking time.
	failFirst int
	calls     int
}

func (f *fakeChannel) Name() string                       { return f.name }
func (f *fakeChannel) Type() string                       { return "fake" }
func (f *fakeChannel) Capabilities() channel.Capabilities { return channel.Capabilities{} }
func (f *fakeChannel) Health(context.Context) error       { return nil }

func (f *fakeChannel) Send(_ context.Context, m channel.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failFirst {
		return errors.New("transient fake failure")
	}
	if f.failWith != nil {
		return f.failWith
	}
	f.sent = append(f.sent, m)
	return nil
}

func (f *fakeChannel) sentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

func newTestStore() *BlobStore {
	return NewBlobStore(storage.NewInMemoryStore(), "")
}

func testEvent(id, severity string) *Event {
	return &Event{
		ID:       id,
		Source:   "my-backup",
		Kind:     KindBackupFailed,
		Severity: severity,
		Subject:  "forge-conductor",
		Title:    "backup failed",
		At:       time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC),
	}
}

func drainerFor(st Store, chans map[string]channel.Channel, now time.Time) *Drainer {
	return NewDrainer(DrainerConfig{
		Store:    st,
		Channels: func(context.Context) (map[string]channel.Channel, error) { return chans, nil },
		Now:      func() time.Time { return now },
	})
}

// TestSeverityGateDropsInfoByDefault is the forge-notifier scar as a
// test: every Telegram subscription there was `severityAtLeast: warning`
// while plenty of what it sent was `info`, so those notifications were
// silently discarded for months.
//
// The behaviour here is the same — info does NOT pass a default channel
// — but it is now the documented default rather than an accident, which
// is the whole difference.
func TestSeverityGateDropsInfoByDefault(t *testing.T) {
	route := Route{Channel: "ops", Enabled: true} // MinSeverity unset → warning

	for _, tc := range []struct {
		severity string
		want     bool
	}{
		{channel.SeverityInfo, false},
		{channel.SeverityWarning, true},
		{channel.SeverityError, true},
		{channel.SeverityCritical, true},
		// An unknown severity must fail CLOSED. If it ranked as the
		// lowest known value instead, a publisher typo would deliver
		// everything it emitted.
		{"catastrophic", false},
		{"", false},
	} {
		got, reason := route.Matches(testEvent("e1", tc.severity))
		if got != tc.want {
			t.Errorf("severity %q: Matches()=%v (reason %q), want %v", tc.severity, got, reason, tc.want)
		}
		if !got && reason == "" {
			t.Errorf("severity %q: rejected with an empty reason — the SPA would show a blank", tc.severity)
		}
	}
}

// TestGiveUpAfterMaxAttempts covers the 2026-09-19 incident directly:
// forge-notifier had no retry cap and sent 20+ duplicate Telegram
// messages for one notification. Here the 5th failure must stamp
// giveUpAt AND ListDue must stop returning the row — both halves matter,
// because a capped counter that the queue keeps re-reading still
// re-sends.
func TestGiveUpAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	st := newTestStore()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	ch := &fakeChannel{name: "ops", failWith: errors.New("502 bad gateway")}
	chans := map[string]channel.Channel{"ops": ch}

	ev := testEvent("evt-1", channel.SeverityError)
	router := NewRouter(st, -1) // cooldown disabled
	if _, err := router.Route(ctx, ev, []Route{{Channel: "ops", Enabled: true}}, now); err != nil {
		t.Fatalf("Route: %v", err)
	}

	// Each Drain makes exactly one attempt per row.
	for i := 1; i <= MaxDeliveryAttempts; i++ {
		res, err := drainerFor(st, chans, now.Add(time.Duration(i)*time.Minute)).Drain(ctx)
		if err != nil {
			t.Fatalf("drain %d: %v", i, err)
		}
		if res.Examined != 1 {
			t.Fatalf("drain %d: examined=%d, want 1 (the row should still be due)", i, res.Examined)
		}
	}

	// The 6th drain must find nothing: the row is terminal.
	res, err := drainerFor(st, chans, now.Add(time.Hour)).Drain(ctx)
	if err != nil {
		t.Fatalf("final drain: %v", err)
	}
	if res.Examined != 0 {
		t.Fatalf("after %d attempts ListDue still returns %d row(s) — this is the "+
			"duplicate-delivery shape that sent 20+ messages on 2026-09-19", MaxDeliveryAttempts, res.Examined)
	}

	row, err := st.GetDelivery(ctx, DeliveryID("evt-1", "ops"))
	if err != nil {
		t.Fatalf("GetDelivery: %v", err)
	}
	if row.GiveUpAt == nil {
		t.Error("giveUpAt not stamped after exhausting attempts")
	}
	if row.Attempts != MaxDeliveryAttempts {
		t.Errorf("attempts=%d, want %d", row.Attempts, MaxDeliveryAttempts)
	}
	if row.LastError == "" {
		t.Error("lastError empty — the operator has nothing to act on")
	}
	// The row must still be READABLE after give-up. A queue that tidies
	// its own failures away is how a notifier silently stops notifying.
	if row.Permanent {
		t.Error("permanent=true for an exhausted-retry give-up; that flag means a config error")
	}
}

// TestPermanentErrorGivesUpImmediately: a bad chat id must not burn five
// attempts and ~75 seconds of Lambda time.
func TestPermanentErrorGivesUpImmediately(t *testing.T) {
	ctx := context.Background()
	st := newTestStore()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	ch := &fakeChannel{name: "ops", failWith: channel.Permanentf("chat not found")}
	chans := map[string]channel.Channel{"ops": ch}

	router := NewRouter(st, -1)
	if _, err := router.Route(ctx, testEvent("evt-2", channel.SeverityError),
		[]Route{{Channel: "ops", Enabled: true}}, now); err != nil {
		t.Fatalf("Route: %v", err)
	}

	res, err := drainerFor(st, chans, now).Drain(ctx)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if res.GaveUp != 1 {
		t.Fatalf("gaveUp=%d, want 1", res.GaveUp)
	}

	row, err := st.GetDelivery(ctx, DeliveryID("evt-2", "ops"))
	if err != nil {
		t.Fatalf("GetDelivery: %v", err)
	}
	if row.Attempts != 1 {
		t.Errorf("attempts=%d, want 1 — a permanent error must not be retried", row.Attempts)
	}
	if !row.Permanent {
		t.Error("permanent flag not set; the SPA cannot tell a config error from an outage")
	}
}

// TestRetrySucceedsAndStopsBeingDue: a transient failure retries, and
// once delivered the row leaves the due set for good.
func TestRetrySucceedsAndStopsBeingDue(t *testing.T) {
	ctx := context.Background()
	st := newTestStore()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	ch := &fakeChannel{name: "ops", failFirst: 2}
	chans := map[string]channel.Channel{"ops": ch}

	router := NewRouter(st, -1)
	if _, err := router.Route(ctx, testEvent("evt-3", channel.SeverityCritical),
		[]Route{{Channel: "ops", Enabled: true}}, now); err != nil {
		t.Fatalf("Route: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := drainerFor(st, chans, now).Drain(ctx); err != nil {
			t.Fatalf("drain %d: %v", i, err)
		}
	}
	if ch.sentCount() != 1 {
		t.Fatalf("channel received %d messages, want exactly 1", ch.sentCount())
	}

	// A fourth drain must send nothing more.
	if _, err := drainerFor(st, chans, now).Drain(ctx); err != nil {
		t.Fatalf("extra drain: %v", err)
	}
	if ch.sentCount() != 1 {
		t.Fatalf("a delivered row was re-sent: %d messages total", ch.sentCount())
	}

	row, err := st.GetDelivery(ctx, DeliveryID("evt-3", "ops"))
	if err != nil {
		t.Fatalf("GetDelivery: %v", err)
	}
	if row.DeliveredAt == nil {
		t.Error("deliveredAt not stamped")
	}
	if row.Attempts != 3 {
		t.Errorf("attempts=%d, want 3", row.Attempts)
	}
}

// TestOverlappingDrainsDeliverOnce is the single-flight lease.
//
// The drain runs inline on a one-minute schedule, so two ticks CAN
// overlap whenever one runs long. Without the lease both would read the
// same due row and both would send it — the duplicate-delivery vector
// this design closes by construction rather than by hoping drains are
// fast.
func TestOverlappingDrainsDeliverOnce(t *testing.T) {
	ctx := context.Background()
	st := newTestStore()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	// A channel that parks inside Send until released, so the second
	// drain is guaranteed to run while the first still holds the lease.
	release := make(chan struct{})
	blocking := &blockingChannel{
		name:    "ops",
		gate:    release,
		entered: make(chan struct{}, 2),
	}
	chans := map[string]channel.Channel{"ops": blocking}

	router := NewRouter(st, -1)
	if _, err := router.Route(ctx, testEvent("evt-4", channel.SeverityError),
		[]Route{{Channel: "ops", Enabled: true}}, now); err != nil {
		t.Fatalf("Route: %v", err)
	}

	var wg sync.WaitGroup
	results := make([]DrainResult, 2)
	errs := make([]error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		results[0], errs[0] = drainerFor(st, chans, now).Drain(ctx)
	}()

	// Wait until the first drain is parked inside Send (lease held).
	select {
	case <-blocking.entered:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("first drain never reached Send")
	}

	// The second drain runs in its own goroutine, because when the lease
	// is BROKEN it parks in Send too and never returns until the gate
	// opens. Running it inline here would deadlock the test instead of
	// failing it — and a test that hangs burns the whole CI timeout and
	// reports nothing useful, which is strictly worse than no test.
	second := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(second)
		results[1], errs[1] = drainerFor(st, chans, now).Drain(ctx)
	}()

	// Proceed on whichever happens first, with no sleeps: the second
	// drain returns (lease held — it skipped) or it reaches Send (lease
	// broken — it is about to double-send).
	select {
	case <-second:
	case <-blocking.entered:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("second drain neither skipped nor reached Send")
	}

	close(release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("drain %d: %v", i, err)
		}
	}
	if !results[1].Skipped {
		t.Error("the concurrent drain was NOT skipped — the single-flight lease did not hold")
	}
	if n := blocking.enterCount(); n != 1 {
		t.Errorf("Send was entered %d times by overlapping drains, want exactly 1", n)
	}
	if n := blocking.sentCount(); n != 1 {
		t.Fatalf("overlapping drains delivered %d messages, want exactly 1", n)
	}
}

type blockingChannel struct {
	mu      sync.Mutex
	name    string
	gate    chan struct{}
	entered chan struct{}
	entries int
	sent    int
}

func (b *blockingChannel) Name() string                       { return b.name }
func (b *blockingChannel) Type() string                       { return "blocking" }
func (b *blockingChannel) Capabilities() channel.Capabilities { return channel.Capabilities{} }
func (b *blockingChannel) Health(context.Context) error       { return nil }

func (b *blockingChannel) Send(_ context.Context, _ channel.Message) error {
	b.mu.Lock()
	b.entries++
	b.mu.Unlock()
	// Buffered, so signalling never blocks even if nobody is receiving
	// yet. Counting entries separately from the signal is what lets the
	// assertion distinguish "entered Send twice" from "sent twice".
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.gate
	b.mu.Lock()
	b.sent++
	b.mu.Unlock()
	return nil
}

func (b *blockingChannel) enterCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.entries
}

func (b *blockingChannel) sentCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sent
}

// TestDedupeDropsReplayedEventID: a re-fired scheduler tick must not
// produce a second alert. This is why Event.ID is required rather than
// generated.
func TestDedupeDropsReplayedEventID(t *testing.T) {
	ctx := context.Background()
	st := newTestStore()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	seen, err := st.SeenEvent(ctx, "backup.failed:forge-conductor:run-77", now)
	if err != nil {
		t.Fatalf("first SeenEvent: %v", err)
	}
	if seen {
		t.Fatal("a never-before-seen id reported as a duplicate")
	}

	seen, err = st.SeenEvent(ctx, "backup.failed:forge-conductor:run-77", now.Add(time.Minute))
	if err != nil {
		t.Fatalf("replay SeenEvent: %v", err)
	}
	if !seen {
		t.Fatal("a replayed event id was NOT deduped — a re-fired tick would alert twice")
	}

	// Past the window the same id is accepted again: a daily backup
	// that fails on consecutive days must alert on each.
	seen, err = st.SeenEvent(ctx, "backup.failed:forge-conductor:run-77", now.Add(DedupeWindow+time.Minute))
	if err != nil {
		t.Fatalf("post-window SeenEvent: %v", err)
	}
	if seen {
		t.Error("an id past the dedupe window was still treated as a duplicate — " +
			"a recurring failure would alert once and then go quiet")
	}
}

// TestCooldownSuppressesFlapButNotSiblings: the cooldown key is
// per-(source, subject, kind), so a flapping service cannot silence a
// different one.
func TestCooldownSuppressesFlapButNotSiblings(t *testing.T) {
	ctx := context.Background()
	st := newTestStore()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	routes := []Route{{Channel: "ops", Enabled: true}}
	router := NewRouter(st, 15*time.Minute)

	first := testEvent("evt-a", channel.SeverityError)
	decisions, err := router.Route(ctx, first, routes, now)
	if err != nil {
		t.Fatalf("first Route: %v", err)
	}
	if !decisions[0].Delivered {
		t.Fatalf("first event suppressed: %q", decisions[0].Reason)
	}

	// Same source+subject+kind inside the window → suppressed.
	flap := testEvent("evt-b", channel.SeverityError)
	decisions, err = router.Route(ctx, flap, routes, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("flap Route: %v", err)
	}
	if decisions[0].Delivered {
		t.Error("a repeat within the cooldown was delivered — a flapping service would storm the channel")
	}
	if decisions[0].Reason == "" {
		t.Error("suppression recorded with no reason; a muted alert must never be an invisible one")
	}

	// A DIFFERENT subject must still get through.
	sibling := testEvent("evt-c", channel.SeverityError)
	sibling.Subject = "forge-sessions"
	decisions, err = router.Route(ctx, sibling, routes, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("sibling Route: %v", err)
	}
	if !decisions[0].Delivered {
		t.Errorf("a different subject was suppressed by its sibling's cooldown: %q", decisions[0].Reason)
	}
}

// TestFullyFilteredEventDoesNotStartCooldown: an event no channel wants
// must not silence the one that follows it.
func TestFullyFilteredEventDoesNotStartCooldown(t *testing.T) {
	ctx := context.Background()
	st := newTestStore()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	routes := []Route{{Channel: "ops", Enabled: true, MinSeverity: channel.SeverityError}}
	router := NewRouter(st, 15*time.Minute)

	// An info event: filtered by the severity gate.
	info := testEvent("evt-info", channel.SeverityInfo)
	if _, err := router.Route(ctx, info, routes, now); err != nil {
		t.Fatalf("info Route: %v", err)
	}

	// The error that follows must still be delivered.
	bad := testEvent("evt-err", channel.SeverityError)
	decisions, err := router.Route(ctx, bad, routes, now.Add(30*time.Second))
	if err != nil {
		t.Fatalf("error Route: %v", err)
	}
	if !decisions[0].Delivered {
		t.Errorf("a filtered info event started a cooldown and swallowed the following error: %q",
			decisions[0].Reason)
	}
}

// TestMissingChannelIsPermanent: a channel deleted after enqueue must
// not be retried five times.
func TestMissingChannelIsPermanent(t *testing.T) {
	ctx := context.Background()
	st := newTestStore()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	router := NewRouter(st, -1)
	if _, err := router.Route(ctx, testEvent("evt-5", channel.SeverityError),
		[]Route{{Channel: "gone", Enabled: true}}, now); err != nil {
		t.Fatalf("Route: %v", err)
	}

	res, err := drainerFor(st, map[string]channel.Channel{}, now).Drain(ctx)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if res.GaveUp != 1 {
		t.Fatalf("gaveUp=%d, want 1", res.GaveUp)
	}
	row, err := st.GetDelivery(ctx, DeliveryID("evt-5", "gone"))
	if err != nil {
		t.Fatalf("GetDelivery: %v", err)
	}
	if !row.Permanent {
		t.Error("a vanished channel should be a permanent failure")
	}
	if !strings.Contains(row.LastError, "gone") {
		t.Errorf("lastError %q should name the missing channel", row.LastError)
	}
}

// TestExpiredLeaseIsReclaimed: a drain killed mid-flight (Lambda
// timeout, OOM) must not wedge the queue forever.
func TestExpiredLeaseIsReclaimed(t *testing.T) {
	ctx := context.Background()
	st := newTestStore()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	ok, err := st.AcquireTickLease(ctx, LeaseTTL, now)
	if err != nil || !ok {
		t.Fatalf("first acquire: ok=%v err=%v", ok, err)
	}
	// Still held inside the TTL.
	ok, err = st.AcquireTickLease(ctx, LeaseTTL, now.Add(time.Second))
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if ok {
		t.Error("the lease was granted twice inside its TTL")
	}
	// Past the TTL it is reclaimable.
	ok, err = st.AcquireTickLease(ctx, LeaseTTL, now.Add(LeaseTTL+time.Second))
	if err != nil {
		t.Fatalf("reclaim acquire: %v", err)
	}
	if !ok {
		t.Error("an expired lease was not reclaimed — the queue would stall until someone noticed")
	}
}

// TestListDueOrdersOldestFirst: under a batch limit, the oldest alert
// must not be starved by newer ones.
func TestListDueOrdersOldestFirst(t *testing.T) {
	ctx := context.Background()
	st := newTestStore()
	base := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 5; i++ {
		d := &Delivery{
			ID:        DeliveryID(fmt.Sprintf("evt-%d", i), "ops"),
			EventID:   fmt.Sprintf("evt-%d", i),
			Channel:   "ops",
			Event:     *testEvent(fmt.Sprintf("evt-%d", i), channel.SeverityError),
			CreatedAt: base.Add(time.Duration(4-i) * time.Minute),
		}
		if err := st.PutDelivery(ctx, d); err != nil {
			t.Fatalf("PutDelivery: %v", err)
		}
	}

	due, err := st.ListDue(ctx, base.Add(time.Hour), 2)
	if err != nil {
		t.Fatalf("ListDue: %v", err)
	}
	if len(due) != 2 {
		t.Fatalf("len(due)=%d, want 2 (the batch limit)", len(due))
	}
	if !due[0].CreatedAt.Before(due[1].CreatedAt) {
		t.Error("ListDue did not return oldest-first; under a batch limit the oldest alert starves")
	}
}
