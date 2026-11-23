// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	. "github.com/onsi/gomega"

	"github.com/simple-container-com/sc-component-notifier/service/internal/channel"
	"github.com/simple-container-com/sc-component-notifier/service/internal/events"
	"github.com/simple-container-com/sc-component-notifier/service/internal/storage"
	"github.com/simple-container-com/sc-component-notifier/service/internal/tenant"
)

// mintOpJWT mints a token for an arbitrary op, so a test can prove that
// one endpoint's token does not open another's.
func mintOpJWT(t *testing.T, op, tenantName string) string {
	t.Helper()
	now := time.Now().UTC()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"op":     op,
		"tenant": tenantName,
		"iat":    now.Unix(),
		"exp":    now.Add(1 * time.Minute).Unix(),
	})
	s, err := tok.SignedString([]byte(testKey))
	Expect(err).ToNot(HaveOccurred())
	return s
}

// stubRoutes is a fixed routing table, so these tests exercise the
// handler rather than the registry.
type stubRoutes struct {
	routes []events.Route
	err    error
}

func (s stubRoutes) Routes(context.Context) ([]events.Route, error) { return s.routes, s.err }

// oneChannel routes everything at warning or above to "ops".
func oneChannel() stubRoutes {
	return stubRoutes{routes: []events.Route{{Channel: "ops", Enabled: true}}}
}

type ingestFixture struct {
	store   events.Store
	routes  RouteSource
	tenants tenant.Store
	mux     *http.ServeMux
}

func newIngestFixture(t *testing.T, store events.Store, routes RouteSource) *ingestFixture {
	t.Helper()
	if store == nil {
		store = events.NewBlobStore(storage.NewInMemoryStore(), "")
	}
	// A negative cooldown disables suppression: these tests are about
	// the handler's gates, and a 15-minute quiet period would make the
	// second event in any test vanish for an unrelated reason.
	router := events.NewRouter(store, -1)
	tenants := tenant.NewInMemoryStore()
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/events",
		JWTMiddleware([]byte(testKey))(EventsIngest(store, router, routes, tenants, nil)))
	return &ingestFixture{store: store, routes: routes, tenants: tenants, mux: mux}
}

func (f *ingestFixture) post(t *testing.T, op, tenantName string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	buf, err := json.Marshal(body)
	Expect(err).ToNot(HaveOccurred())
	req := httptest.NewRequest("POST", "/api/v1/events", bytes.NewReader(buf))
	req.Header.Set("Authorization", "Bearer "+mintOpJWT(t, op, tenantName))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	f.mux.ServeHTTP(rr, req)
	return rr
}

func validEvent() map[string]any {
	return map[string]any{
		"id":       "backup.failed:forge-conductor:run-7",
		"kind":     events.KindBackupFailed,
		"severity": channel.SeverityError,
		"subject":  "forge-conductor",
		"title":    "Backup failed",
		"body":     "mongodump exited 1",
	}
}

func (f *ingestFixture) deliveries(t *testing.T) []*events.Delivery {
	t.Helper()
	rows, err := f.store.ListDeliveries(context.Background(), 100)
	Expect(err).ToNot(HaveOccurred())
	return rows
}

func TestEventsIngest_HappyPath(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, oneChannel())

	rr := f.post(t, "event", "my-backup", validEvent())

	// 202, not 200: delivery happens on the next scheduler tick, so the
	// work genuinely is not done when this returns.
	Expect(rr.Code).To(Equal(http.StatusAccepted))

	var resp IngestResponse
	Expect(json.Unmarshal(rr.Body.Bytes(), &resp)).To(Succeed())
	Expect(resp.Status).To(Equal("accepted"))
	Expect(resp.Decisions).To(HaveLen(1))
	Expect(resp.Decisions[0].Channel).To(Equal("ops"))
	Expect(resp.Decisions[0].Delivered).To(BeTrue())

	rows := f.deliveries(t)
	Expect(rows).To(HaveLen(1))
	Expect(rows[0].Channel).To(Equal("ops"))
	Expect(rows[0].Event.Title).To(Equal("Backup failed"))
}

// The token's op is the authorisation. A tenant's register token, or
// the scheduler's, must not be able to inject events.
func TestEventsIngest_RejectsWrongOp(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, oneChannel())

	for _, op := range []string{"register", "scheduler", "", "admin"} {
		rr := f.post(t, op, "my-backup", validEvent())
		Expect(rr.Code).To(Equal(http.StatusForbidden), "op %q was accepted", op)
	}
	Expect(f.deliveries(t)).To(BeEmpty())
}

// Source is server-filled from the tenant claim. A publisher must not be
// able to attribute an event to another publisher — which would both
// mislead the operator and bypass a channel's `sources` allowlist.
func TestEventsIngest_SourceComesFromTheToken(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, oneChannel())

	body := validEvent()
	body["source"] = "some-other-component"

	rr := f.post(t, "event", "my-backup", body)
	Expect(rr.Code).To(Equal(http.StatusAccepted))

	rows := f.deliveries(t)
	Expect(rows).To(HaveLen(1))
	Expect(rows[0].Event.Source).To(Equal("my-backup"))
}

// The sources allowlist is only meaningful if the claim wins. This is
// the same guard as above, asserted through its consequence.
func TestEventsIngest_SpoofedSourceCannotPassAnAllowlist(t *testing.T) {
	RegisterTestingT(t)
	routes := stubRoutes{routes: []events.Route{
		{Channel: "ops", Enabled: true, Sources: []string{"trusted-component"}},
	}}
	f := newIngestFixture(t, nil, routes)

	body := validEvent()
	body["source"] = "trusted-component"

	rr := f.post(t, "event", "untrusted-component", body)
	Expect(rr.Code).To(Equal(http.StatusAccepted))

	var resp IngestResponse
	Expect(json.Unmarshal(rr.Body.Bytes(), &resp)).To(Succeed())
	Expect(resp.Decisions).To(HaveLen(1))
	Expect(resp.Decisions[0].Delivered).To(BeFalse())
	Expect(resp.Decisions[0].Reason).To(ContainSubstring("not in channel allowlist"))
	Expect(f.deliveries(t)).To(BeEmpty())
}

// An unknown severity must be a loud 400, not an accepted event that
// the routing gate then ranks 0 and drops. The silent version is
// exactly how every `info` notification in forge-notifier became
// invisible: accepted, gated out, 200, no message, no error anywhere.
func TestEventsIngest_RejectsUnknownSeverity(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, oneChannel())

	for _, sev := range []string{"warn", "ERROR", "", "catastrophic", "0"} {
		body := validEvent()
		body["severity"] = sev
		rr := f.post(t, "event", "my-backup", body)
		Expect(rr.Code).To(Equal(http.StatusBadRequest), "severity %q was accepted", sev)
		// The response must name the acceptable values — a bare 400
		// leaves the publisher's author guessing.
		Expect(rr.Body.String()).To(ContainSubstring("info, warning, error, critical"))
	}
	Expect(f.deliveries(t)).To(BeEmpty())
}

func TestEventsIngest_RequiresTheFieldsThatCannotBeDefaulted(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, oneChannel())

	for _, field := range []string{"id", "kind", "title"} {
		body := validEvent()
		delete(body, field)
		rr := f.post(t, "event", "my-backup", body)
		Expect(rr.Code).To(Equal(http.StatusBadRequest), "a missing %q was accepted", field)
		Expect(rr.Body.String()).To(ContainSubstring(field))
	}
	Expect(f.deliveries(t)).To(BeEmpty())
}

// The id is the idempotency key and the whole defence against duplicate
// alerts: a re-fired scheduler tick or a Lambda retry must produce one
// message, not two. 20+ duplicate Telegram messages for one
// notification is a thing that actually happened in this fleet.
func TestEventsIngest_DuplicateIsAcceptedButNotRouted(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, oneChannel())

	first := f.post(t, "event", "my-backup", validEvent())
	Expect(first.Code).To(Equal(http.StatusAccepted))

	second := f.post(t, "event", "my-backup", validEvent())
	// 200, not 409: a duplicate is the system working, and a publisher
	// that retries on non-2xx would hammer an event already handled.
	Expect(second.Code).To(Equal(http.StatusOK))

	var resp IngestResponse
	Expect(json.Unmarshal(second.Body.Bytes(), &resp)).To(Succeed())
	Expect(resp.Status).To(Equal("duplicate"))
	Expect(resp.Decisions).To(BeEmpty())

	Expect(f.deliveries(t)).To(HaveLen(1))
}

// failingDedupe fails only SeenEvent, so the test isolates the
// fail-closed decision from any other storage error.
type failingDedupe struct {
	events.Store
}

func (failingDedupe) SeenEvent(context.Context, string, time.Time) (bool, error) {
	return false, errors.New("storage unavailable")
}

// A dedupe check that errors must fail CLOSED. The publisher retries —
// cheap, because the id is idempotent. Guessing "not seen" would risk
// the duplicate storm this component is built to avoid.
func TestEventsIngest_DedupeFailureIsNotRouted(t *testing.T) {
	RegisterTestingT(t)
	inner := events.NewBlobStore(storage.NewInMemoryStore(), "")
	f := newIngestFixture(t, failingDedupe{Store: inner}, oneChannel())

	rr := f.post(t, "event", "my-backup", validEvent())
	Expect(rr.Code).To(Equal(http.StatusInternalServerError))

	rows, err := inner.ListDeliveries(context.Background(), 100)
	Expect(err).ToNot(HaveOccurred())
	Expect(rows).To(BeEmpty(), "an event was routed despite the dedupe check failing")
}

// A routing table that cannot be read is a 500, not an empty table. An
// empty table would make the event vanish with a 202 — a notifier
// reporting success while delivering nothing.
func TestEventsIngest_UnreadableRoutesIsAnError(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, stubRoutes{err: errors.New("storage unavailable")})

	rr := f.post(t, "event", "my-backup", validEvent())
	Expect(rr.Code).To(Equal(http.StatusInternalServerError))
}

// `at` is optional, but the row must never carry a zero time: a
// notification rendered from it would read "01 Jan 0001".
func TestEventsIngest_EventTimeDefaultsToReceipt(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, oneChannel())

	body := validEvent()
	delete(body, "at")

	before := time.Now().UTC().Add(-time.Second)
	Expect(f.post(t, "event", "my-backup", body).Code).To(Equal(http.StatusAccepted))

	rows := f.deliveries(t)
	Expect(rows).To(HaveLen(1))
	Expect(rows[0].Event.At).To(BeTemporally(">=", before))
	Expect(rows[0].Event.ReceivedAt).ToNot(BeZero())
}

// A publisher-supplied `at` must survive. Delivery can lag the event by
// a tick, and an operator reading a timestamp wants the failure's time,
// not the notifier's.
func TestEventsIngest_PublisherEventTimeIsPreserved(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, oneChannel())

	at := time.Date(2026, 11, 23, 3, 14, 15, 0, time.UTC)
	body := validEvent()
	body["at"] = at.Format(time.RFC3339)

	Expect(f.post(t, "event", "my-backup", body).Code).To(Equal(http.StatusAccepted))

	rows := f.deliveries(t)
	Expect(rows).To(HaveLen(1))
	Expect(rows[0].Event.At.UTC()).To(BeTemporally("==", at))
}

// A token with no tenant claim cannot have its source filled in, and an
// event with an empty source is unattributable. 403, not a blank
// source — the latter would show up in Telegram as a message from
// nowhere.
func TestEventsIngest_RejectsTokenWithoutTenant(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, oneChannel())

	rr := f.post(t, "event", "", validEvent())
	Expect(rr.Code).To(Equal(http.StatusForbidden))
	Expect(f.deliveries(t)).To(BeEmpty())
}

// --- Lazy tenant registration (hazard 6) ---------------------------------
//
// The publisher's plugin push-registers at deploy time, but that call is
// best-effort by design: on a first provision the notifier's own Lambda
// does not exist yet, so requiring it would deadlock a stack that declares
// both the notifier and a publisher. These tests cover the path that makes
// the tenant row appear anyway, and — more importantly — that nothing on it
// can cost an operator an alert.

func (f *ingestFixture) tenant(t *testing.T, stackName string) (tenant.Record, bool) {
	t.Helper()
	rec, found, err := f.tenants.Get(context.Background(), stackName)
	Expect(err).ToNot(HaveOccurred())
	return rec, found
}

// The reason registration is allowed to fail: a publisher that never
// managed to register is still a real publisher, and its first event is
// proof. The `tenant` claim is already authenticated and is already the
// authority for `source`, so the row grants nothing new.
func TestEventsIngest_FirstEventRegistersTheTenant(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, oneChannel())

	_, found := f.tenant(t, "my-backup")
	Expect(found).To(BeFalse(), "fixture precondition: the tenant must start absent")

	Expect(f.post(t, "event", "my-backup", validEvent()).Code).To(Equal(http.StatusAccepted))

	rec, found := f.tenant(t, "my-backup")
	Expect(found).To(BeTrue(), "a publisher that never push-registered stayed invisible in the tenant list")
	Expect(rec.StackName).To(Equal("my-backup"))
	Expect(rec.CreatedAt).ToNot(BeZero())
	Expect(rec.LastSeenAt).ToNot(BeZero())
}

// LastSeenAt is what makes the tenant list worth reading. Without this
// refresh a publisher's "last seen" is its last DEPLOY, so one that has
// gone silent looks identical to one that is healthy with nothing to
// report — the exact confusion the list exists to resolve.
func TestEventsIngest_SubsequentEventRefreshesLastSeen(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, oneChannel())
	// A controlled clock, because real time makes this assertion vacuous:
	// two posts land in the same millisecond, so `LastSeenAt >= first`
	// holds whether the refresh happened or not, and a mutation that skips
	// the refresh entirely passes.
	clock := time.Date(2026, 11, 23, 9, 0, 0, 0, time.UTC)
	f.rewireNow(func() time.Time { return clock })

	Expect(f.post(t, "event", "my-backup", validEvent()).Code).To(Equal(http.StatusAccepted))
	first, _ := f.tenant(t, "my-backup")

	// A DIFFERENT id: the same one would short-circuit at the dedupe gate,
	// which sits before the bookkeeping on purpose.
	clock = clock.Add(90 * time.Minute)
	second := validEvent()
	second["id"] = "backup.failed:forge-conductor:run-8"
	Expect(f.post(t, "event", "my-backup", second).Code).To(Equal(http.StatusAccepted))

	rec, _ := f.tenant(t, "my-backup")
	Expect(rec.LastSeenAt).To(BeTemporally("==", clock),
		"last seen was not refreshed, so a publisher that has gone quiet reads as healthy")
	Expect(rec.CreatedAt).To(BeTemporally("==", first.CreatedAt),
		"a refresh must not restamp CreatedAt — that is the only record of when the publisher first appeared")
}

// A soft-deleted tenant that is demonstrably alive and publishing is
// revived rather than left invisible. The alternative is an operator
// watching a list that omits the publisher currently paging them.
func TestEventsIngest_SoftDeletedTenantIsRevivedByPublishing(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, oneChannel())

	Expect(f.tenants.Upsert(context.Background(), tenant.Record{
		StackName:   "my-backup",
		CreatedAt:   time.Now().UTC().Add(-time.Hour),
		SoftDeleted: true,
	})).To(Succeed())

	Expect(f.post(t, "event", "my-backup", validEvent()).Code).To(Equal(http.StatusAccepted))

	rec, found := f.tenant(t, "my-backup")
	Expect(found).To(BeTrue())
	Expect(rec.SoftDeleted).To(BeFalse(), "a publishing tenant stayed soft-deleted, i.e. hidden from the operator")
}

// failingTenants fails one of the two store calls. Both are covered
// below: a fake that fails the LOOKUP never reaches the upsert, so it
// cannot prove the upsert path is safe — which is how the first version
// of this test passed while a panic in the upsert branch went unnoticed.
type failingTenants struct {
	tenant.Store
	failGet bool
	gets    int
	upserts int
}

func (f *failingTenants) Get(context.Context, string) (tenant.Record, bool, error) {
	f.gets++
	if f.failGet {
		return tenant.Record{}, false, errors.New("storage unavailable")
	}
	return tenant.Record{}, false, nil
}

func (f *failingTenants) Upsert(context.Context, tenant.Record) error {
	f.upserts++
	return errors.New("storage unavailable")
}

func (f *ingestFixture) rewireTenants(tenants tenant.Store) {
	f.mux = http.NewServeMux()
	f.mux.Handle("POST /api/v1/events",
		JWTMiddleware([]byte(testKey))(EventsIngest(f.store, events.NewRouter(f.store, -1), f.routes, tenants, nil)))
}

// rewireNow rebuilds the handler on a controlled clock, keeping the
// fixture's own stores.
func (f *ingestFixture) rewireNow(now func() time.Time) {
	f.mux = http.NewServeMux()
	f.mux.Handle("POST /api/v1/events",
		JWTMiddleware([]byte(testKey))(EventsIngest(f.store, events.NewRouter(f.store, -1), f.routes, f.tenants, now)))
}

// The trade stated plainly: losing the alert to fix the bookkeeping is
// the wrong way round. Unlike the dedupe check — which fails CLOSED
// because a duplicate storm is a real harm — nothing downstream depends
// on the tenant row, so this is the handler's only deliberate fail-open.
func TestEventsIngest_TenantStoreFailureDoesNotFailTheEvent(t *testing.T) {
	RegisterTestingT(t)
	for _, tc := range []struct {
		name    string
		failGet bool
		// What the fake must record, so a test that silently stops
		// reaching the failing call fails instead of passing.
		wantGets    int
		wantUpserts int
	}{
		{name: "the lookup fails", failGet: true, wantGets: 1, wantUpserts: 0},
		{name: "the lookup succeeds and the upsert fails", failGet: false, wantGets: 1, wantUpserts: 1},
	} {
		f := newIngestFixture(t, nil, oneChannel())
		broken := &failingTenants{failGet: tc.failGet}
		f.rewireTenants(broken)

		Expect(f.post(t, "event", "my-backup", validEvent()).Code).To(Equal(http.StatusAccepted),
			"%s: a tenant-store outage rejected an event it had no business touching", tc.name)
		Expect(f.deliveries(t)).To(HaveLen(1), "%s: the delivery was not enqueued", tc.name)
		Expect(broken.gets).To(Equal(tc.wantGets), "%s: store.Get call count", tc.name)
		Expect(broken.upserts).To(Equal(tc.wantUpserts), "%s: store.Upsert call count", tc.name)
	}
}

// A nil store is the explicitly supported shape (the signature documents
// it), because a caller wiring the handler without a tenant store must
// not panic mid-ingest.
func TestEventsIngest_NilTenantStoreIsInert(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, oneChannel())
	f.rewireTenants(nil)

	Expect(f.post(t, "event", "my-backup", validEvent()).Code).To(Equal(http.StatusAccepted))
	Expect(f.deliveries(t)).To(HaveLen(1))
}

func TestEventsIngest_MalformedBodyIs400(t *testing.T) {
	RegisterTestingT(t)
	f := newIngestFixture(t, nil, oneChannel())

	req := httptest.NewRequest("POST", "/api/v1/events", bytes.NewReader([]byte("{not json")))
	req.Header.Set("Authorization", "Bearer "+mintOpJWT(t, "event", "my-backup"))
	rr := httptest.NewRecorder()
	f.mux.ServeHTTP(rr, req)

	Expect(rr.Code).To(Equal(http.StatusBadRequest))
}
