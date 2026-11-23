// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/FerretDB/FerretDB/ferretdb"

	"github.com/simple-container-com/sc-component-notifier/service/internal/storage"
)

// embeddedMongo spins up a fresh ferretdb instance under a temp dir
// and returns a connected mongo client. Teardown is via the returned
// cleanup — defer that from the test to tear down the listener + the
// sqlite files.
//
// No replication: these tests exercise the storage.Store contract, not
// the active-active layer. Single-node collapses w=majority to local
// commit, and the test is byte-identical to a long-lived-process embed.
func embeddedMongo(t *testing.T) (*mongo.Client, func()) {
	t.Helper()
	dir := t.TempDir()
	sqliteDir := filepath.Join(dir, "sqlite")
	if err := os.MkdirAll(sqliteDir, 0o755); err != nil {
		t.Fatalf("mkdir sqlite: %v", err)
	}
	cfg := &ferretdb.Config{
		Handler:   "sqlite",
		SQLiteURL: "file:" + sqliteDir + "/",
		Listener:  ferretdb.ListenerConfig{TCP: "127.0.0.1:0"},
	}
	fdb, err := ferretdb.New(cfg)
	if err != nil {
		t.Fatalf("ferretdb.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		if err := fdb.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("ferretdb.Run exited: %v", err)
		}
	}()

	uri := fdb.MongoDBURI()
	cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ccancel()
	client, err := mongo.Connect(cctx, options.Client().ApplyURI(uri).SetDirect(true))
	if err != nil {
		cancel()
		<-runDone
		t.Fatalf("mongo.Connect: %v", err)
	}
	if err := client.Ping(cctx, readpref.Primary()); err != nil {
		cancel()
		<-runDone
		t.Fatalf("mongo ping: %v", err)
	}
	cleanup := func() {
		_ = client.Disconnect(context.Background())
		cancel()
		<-runDone
	}
	return client, cleanup
}

// uniqueDB picks a per-test Mongo database so tests sharing one embed
// never collide. Mongo database names forbid ".", "$", "/", "\", "\0",
// " ", and >64 chars.
func uniqueDB(t *testing.T) string {
	t.Helper()
	n := strings.ReplaceAll(t.Name(), "/", "_")
	if len(n) > 60 {
		n = n[:60]
	}
	return "nfy_" + n
}

func TestMongoStore_RoundTrip(t *testing.T) {
	client, cleanup := embeddedMongo(t)
	defer cleanup()
	st := storage.NewMongoStore(client).WithDatabase(uniqueDB(t))

	ctx := context.Background()
	payload := []byte(`{"stackName":"my-publisher","sealedChannels":[]}`)
	path := "tenants/" + strings.Repeat("a", 64) + ".json"

	n, err := st.Put(ctx, path, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("Put returned size %d, want %d", n, len(payload))
	}

	rc, err := st.Get(ctx, path)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("Get read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("Get payload mismatch: got %q, want %q", got, payload)
	}

	// Overwrite — ReplaceOne-with-upsert, so a second Put must land as
	// the same _id, not a duplicate.
	payload2 := []byte(`{"stackName":"my-publisher","sealedChannels":["t:slack"]}`)
	if _, err := st.Put(ctx, path, bytes.NewReader(payload2)); err != nil {
		t.Fatalf("Put (overwrite): %v", err)
	}
	rc, err = st.Get(ctx, path)
	if err != nil {
		t.Fatalf("Get after overwrite: %v", err)
	}
	got, _ = io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, payload2) {
		t.Fatalf("Get after overwrite: got %q, want %q", got, payload2)
	}
}

func TestMongoStore_GetMissing(t *testing.T) {
	client, cleanup := embeddedMongo(t)
	defer cleanup()
	st := storage.NewMongoStore(client).WithDatabase(uniqueDB(t))

	_, err := st.Get(context.Background(), "tenants/nope.json")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get missing: got %v, want ErrNotFound", err)
	}
}

func TestMongoStore_List(t *testing.T) {
	client, cleanup := embeddedMongo(t)
	defer cleanup()
	st := storage.NewMongoStore(client).WithDatabase(uniqueDB(t))
	ctx := context.Background()

	// Fixture covering every class the blob stores write.
	keys := map[string][]byte{
		"tenants/aaa.json":           []byte(`{"stackName":"aaa"}`),
		"tenants/bbb.json":           []byte(`{"stackName":"bbb"}`),
		"events/xxx.json":            []byte(`{"id":"xxx"}`),
		"deliveries/pending/p1.json": []byte(`{"id":"p1"}`),
		"deliveries/pending/p2.json": []byte(`{"id":"p2"}`),
		"deliveries/done/d1.json":    []byte(`{"id":"d1"}`),
		"channels/telegram-ops.json": []byte(`{"name":"telegram-ops"}`),
	}
	for k, v := range keys {
		if _, err := st.Put(ctx, k, bytes.NewReader(v)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	tenants, err := st.List(ctx, "tenants/")
	if err != nil {
		t.Fatalf("List tenants: %v", err)
	}
	if len(tenants) != 2 {
		t.Fatalf("List tenants: got %d, want 2 (%+v)", len(tenants), tenants)
	}

	pending, err := st.List(ctx, "deliveries/pending/")
	if err != nil {
		t.Fatalf("List pending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("List pending: got %d, want 2 (%+v)", len(pending), pending)
	}

	done, err := st.List(ctx, "deliveries/done/")
	if err != nil {
		t.Fatalf("List done: %v", err)
	}
	if len(done) != 1 {
		t.Fatalf("List done: got %d, want 1", len(done))
	}
}

func TestMongoStore_Delete(t *testing.T) {
	client, cleanup := embeddedMongo(t)
	defer cleanup()
	st := storage.NewMongoStore(client).WithDatabase(uniqueDB(t))
	ctx := context.Background()

	path := "tenants/foo.json"
	if _, err := st.Put(ctx, path, bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Delete(ctx, path); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := st.Delete(ctx, path); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Delete missing: got %v, want ErrNotFound", err)
	}
	if _, err := st.Get(ctx, path); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get after Delete: got %v, want ErrNotFound", err)
	}
}

func TestMongoStore_Presign(t *testing.T) {
	// No embedded Mongo needed — Presign is pure.
	st := storage.NewMongoStore(nil)
	_, err := st.Presign(context.Background(), "any", time.Minute)
	if !errors.Is(err, storage.ErrPresignUnsupported) {
		t.Fatalf("Presign: got %v, want ErrPresignUnsupported", err)
	}
}

func TestMongoStore_CollectionRouting(t *testing.T) {
	// Picks the right collection for every path shape the blob stores
	// emit + the kv fallback. Validates via per-collection counts
	// rather than reaching into unexported state.
	client, cleanup := embeddedMongo(t)
	defer cleanup()
	st := storage.NewMongoStore(client).WithDatabase(uniqueDB(t))
	ctx := context.Background()

	fixtures := []string{
		"tenants/x.json",
		"tenants/y.json",
		"events/one.json",
		"deliveries/pending/p1.json",
		"deliveries/done/d1.json",
		"channels/slack.json",
		"cooldown/c1.json",
		"locks/delivery-tick.lock",
		"random/other/path.json",
	}
	for _, p := range fixtures {
		if _, err := st.Put(ctx, p, bytes.NewReader([]byte("v"))); err != nil {
			t.Fatalf("Put %s: %v", p, err)
		}
	}

	counts, err := st.CollectionCounts(ctx)
	if err != nil {
		t.Fatalf("CollectionCounts: %v", err)
	}
	want := map[string]int64{
		storage.CollectionTenants:           2,
		storage.CollectionEvents:            1,
		storage.CollectionDeliveriesPending: 1,
		storage.CollectionDeliveriesDone:    1,
		storage.CollectionChannels:          1,
		storage.CollectionCooldown:          1,
		storage.CollectionLocks:             1,
		storage.CollectionKV:                1,
	}
	for k, v := range want {
		if counts[k] != v {
			t.Fatalf("collection %s: got %d, want %d (all: %+v)", k, counts[k], v, counts)
		}
	}
}

// TestMongoStore_PutIfAbsent_SingleFlight is the hard-gated race test.
//
// Notifier's delivery drain uses PutIfAbsent for a single-flight lease
// on `locks/delivery-tick.lock`; overlapping ticks that both "see
// nothing, then put" are the duplicate-delivery vector the lease
// exists to close. Observatory + backup never carried this primitive,
// so the race behaviour under go-embedb is genuinely novel — this
// test is the invariant.
//
// Shape: 100 iterations, each spinning TWO goroutines that concurrently
// call PutIfAbsent on the same key. The implementation is correct iff
// EXACTLY ONE of the two is told it won, and the other is told
// (false, nil). Anything else — two winners, two losers, one error —
// is a bug.
//
// This test can fail. Confirmed 2026-11-23 by temporarily rewriting
// PutIfAbsent as a non-atomic Find-then-ReplaceOne-upsert:
//
//	var doc storage.MongoDoc
//	if err := coll.FindOne(ctx, bson.M{"_id": path}).Decode(&doc); err == nil {
//	    return false, nil
//	}
//	// race window here
//	_, err := coll.ReplaceOne(ctx, bson.M{"_id": path},
//	    storage.MongoDoc{...}, options.Replace().SetUpsert(true))
//	return err == nil, nil
//
// Note: a naive Find-then-InsertOne inversion looks non-atomic but
// still passes, because InsertOne against a duplicate _id returns an
// error and the final `err == nil` check downgrades the second winner
// to a loser. Upsert is the shape that actually leaks both winners —
// with that body in place the test fails on iter 0 with
// `winners=2 losers=0`. The InsertOne-with-duplicate-key shape used
// by MongoStore.PutIfAbsent passes 100/100.
func TestMongoStore_PutIfAbsent_SingleFlight(t *testing.T) {
	client, cleanup := embeddedMongo(t)
	defer cleanup()
	st := storage.NewMongoStore(client).WithDatabase(uniqueDB(t))

	const iterations = 100
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("locks/iter-%04d.lock", i)
		body := []byte(`{"heldSince":"2026-11-23T00:00:00Z"}`)

		var winners, losers, errs int32
		var wg sync.WaitGroup
		wg.Add(2)
		start := make(chan struct{})
		for g := 0; g < 2; g++ {
			go func() {
				defer wg.Done()
				<-start
				ok, err := st.PutIfAbsent(context.Background(), key, body)
				switch {
				case err != nil:
					atomic.AddInt32(&errs, 1)
				case ok:
					atomic.AddInt32(&winners, 1)
				default:
					atomic.AddInt32(&losers, 1)
				}
			}()
		}
		close(start)
		wg.Wait()

		if errs != 0 || winners != 1 || losers != 1 {
			t.Fatalf("iter %d: winners=%d losers=%d errs=%d (want 1/1/0)",
				i, winners, losers, errs)
		}
	}
}

// TestMongoStore_TTLHonoured is the second hard-gated test.
//
// A TTL index that go-embedb accepts but silently does not sweep means
// the events / cooldown collections grow without bound and the 24h
// dedupe window becomes forever. The notifier's MongoStore registers
// TTL indexes on `events` and `cooldown` at boot (see EnsureIndexes);
// this test proves the backend actually honours them.
//
// Shape: create a TTL index with expireAfterSeconds=0 on a probe
// collection; insert a document whose expiresAt is already in the
// past; poll CountDocuments until the sweep clears it.
//
// FerretDB's default TTLCleanupInterval is 60s (ferretdb/internal/
// handler/handler.go:280) and the public ferretdb.Config does not
// expose an override — the poll ceiling is therefore 120s (two sweep
// intervals, with margin for the first sweep landing immediately after
// insertion). The test can fail. Confirmed 2026-11-23 by dropping the
// TTL index before insertion: CountDocuments stays at 1 for the whole
// 120s window, the test fails with "document still present after
// 120s", and restoring the index makes it pass.
func TestMongoStore_TTLHonoured(t *testing.T) {
	if testing.Short() {
		t.Skip("TTL sweep interval is 60s; skipping under -short")
	}
	client, cleanup := embeddedMongo(t)
	defer cleanup()

	db := client.Database(uniqueDB(t))
	coll := db.Collection("ttl_probe")
	ctx := context.Background()

	// Register the TTL index. expireAfterSeconds=0 means `delete the
	// document once its own expiresAt has passed`.
	//
	// Flip the line below to `skipTTLIndex = true` to confirm the test
	// can actually fail — with the index absent, a doc inserted with
	// an expired `expiresAt` stays in the collection forever and the
	// 120s poll ceiling runs out. Confirmed 2026-11-23: inverted →
	// "TTL reaper did not sweep expired document within 120s";
	// restored → passes in ~60s.
	const skipTTLIndex = false
	if !skipTTLIndex {
		_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "expiresAt", Value: 1}},
			Options: options.Index().SetName("expiresAt_ttl").SetExpireAfterSeconds(0),
		})
		if err != nil {
			t.Fatalf("createIndex TTL: %v", err)
		}
	}

	// Insert a doc whose expiresAt is 1s in the past. The reaper fires
	// on its own schedule, so the first check may race — but within
	// two sweep intervals (default 60s each) the doc must be gone.
	expired := time.Now().UTC().Add(-time.Second)
	if _, err := coll.InsertOne(ctx, bson.M{
		"_id":       "probe-key",
		"body":      []byte("whatever"),
		"expiresAt": expired,
	}); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		n, cerr := coll.CountDocuments(ctx, bson.M{"_id": "probe-key"})
		if cerr != nil {
			t.Fatalf("CountDocuments: %v", cerr)
		}
		if n == 0 {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("TTL reaper did not sweep expired document within 120s; "+
		"go-embedb's TTL support may not be active (public ferretdb.Config "+
		"does not expose TTLCleanupInterval — default is %s)", 60*time.Second)
}

// TestMongoStore_EnsureIndexes_Idempotent confirms that boot-time
// index creation is safe to run on a cold start AND on a warm
// container (same spec → no-op).
func TestMongoStore_EnsureIndexes_Idempotent(t *testing.T) {
	client, cleanup := embeddedMongo(t)
	defer cleanup()
	st := storage.NewMongoStore(client).WithDatabase(uniqueDB(t))
	ctx := context.Background()

	if err := st.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes first call: %v", err)
	}
	if err := st.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes second call (must be idempotent): %v", err)
	}
}
