// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// MongoStore backs the storage.Store interface with an embedded
// go-embedb (ferretdb + sqlite + active-active S3 replication) talking
// Mongo over a loopback TCP listener.
//
// This is the NEW backend introduced behind USE_GOEMBEDB=1
// (sc-component-notifier-goembedb-migration). The existing S3Store
// stays the default; the operator flips the env var once per-stack
// after a `/admin/migrate-s3-to-goembedb` dry-run verifies count
// parity. Shape mirrors sc-component-observatory's adoption — same
// package layout, same collection-routing split — because the two
// Components persist the same classes of state (small JSON blobs under
// prefix-shaped paths).
//
// ## Storage shape
//
// Every storage key `<prefix>/<class>/...` becomes one document
// `{_id: "<full-path>", body: <binary>, expiresAt?: <t>, updatedAt: <t>}`
// in a Mongo collection picked per logical class:
//
//	events/*              → collection "events"           (TTL-swept)
//	deliveries/pending/*  → collection "deliveries_pending"
//	deliveries/done/*     → collection "deliveries_done"
//	channels/*            → collection "channels"
//	cooldown/*            → collection "cooldown"         (TTL-swept)
//	tenants/*             → collection "tenants"
//	locks/*               → collection "locks"            (used by PutIfAbsent)
//	anything else         → collection "kv"
//
// One collection per class keeps List cheap: the drain's hot path
// (ListDue over pending/) walks one collection with one anchored regex,
// instead of filtering an undifferentiated "objects" collection. The
// alternative — one collection with path-prefix filtering — would make
// every drain pay for every terminal delivery the service has ever
// produced.
//
// ## TTL
//
// The `events` and `cooldown` collections carry a TTL index on
// `expiresAt` with `expireAfterSeconds: 0`, so ferretdb's background
// reaper deletes a record once its own ExpiresAt has passed. Put
// inspects a JSON body for a top-level `expiresAt` (RFC3339) and
// promotes it to a BSON time.Time so the index can see it; a body
// without that field (channels, deliveries, tenants) writes no
// expiresAt and is never swept.
//
// ## Presign
//
// NOT implemented. `grep -rn 'Presign(' service` on this repo shows
// only the Store interface definition + the S3/FS/mem implementations;
// no notifier code path hands a signed URL out. The FS/S3/mem stores
// keep Presign because the Store contract requires it; MongoStore
// returns ErrPresignUnsupported so a future caller fails loudly instead
// of silently receiving an empty string.
package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ErrPresignUnsupported is returned by MongoStore.Presign.
var ErrPresignUnsupported = errors.New("storage.MongoStore: Presign is not supported on this backend")

// MongoDatabase is the Mongo database name the store writes under. A
// single database keeps bootstrap trivial (`createCollection` fires on
// first write via driver autoCreate); the collection split lives
// ONE level down.
const MongoDatabase = "notifier"

// MongoStore implements Store on top of a mongo.Client.
//
// Holds the Mongo database name + a package-selected collection router
// (see collectionFor). No background goroutines of its own — connection
// lifecycle is the caller's (see cmd/notifier/boot_goembedb.go).
type MongoStore struct {
	client *mongo.Client
	dbName string
}

// MongoDoc is the on-disk document shape. Kept small on purpose: the
// storage.Store contract is bytes-in/bytes-out, not structured docs,
// so Body is an opaque BSON binary. `ExpiresAt` is promoted from the
// JSON body (when present) so the TTL sweep on events/cooldown can
// see it; an omitted field disables the sweep for that record.
type MongoDoc struct {
	ID        string           `bson:"_id"`
	Body      primitive.Binary `bson:"body"`
	ExpiresAt *time.Time       `bson:"expiresAt,omitempty"`
	UpdatedAt time.Time        `bson:"updatedAt"`
}

// NewMongoStore builds a store around an already-connected client.
//
// The caller owns the client's lifetime; MongoStore has no Close. The
// boot helper that holds this store also holds the client for other
// uses (the migration endpoint), and splitting ownership would mean
// double-close semantics.
func NewMongoStore(client *mongo.Client) *MongoStore {
	return &MongoStore{client: client, dbName: MongoDatabase}
}

// WithDatabase overrides the Mongo database name; useful in tests.
func (s *MongoStore) WithDatabase(name string) *MongoStore {
	cp := *s
	cp.dbName = name
	return &cp
}

// EnsureIndexes creates the TTL indexes on events + cooldown and the
// _id uniqueness is implicit. Idempotent — createIndexes against an
// index that already exists is a no-op (same spec) or a loud error
// (different spec), which is the behaviour we want.
//
// Called at boot by cmd/notifier/boot_goembedb.go. Separated from
// NewMongoStore so unit tests can opt into (or out of) TTL depending
// on what they are exercising.
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	for _, name := range []string{CollectionEvents, CollectionCooldown} {
		coll := s.client.Database(s.dbName).Collection(name)
		_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "expiresAt", Value: 1}},
			Options: options.Index().SetName("expiresAt_ttl").SetExpireAfterSeconds(0),
		})
		if err != nil {
			return fmt.Errorf("storage.MongoStore: ensure TTL index on %s: %w", name, err)
		}
	}
	return nil
}

// Put writes body under `path`, upserting by _id.
//
// Returns the size written. Mirrors InMemoryStore: the body is fully
// read before the upsert so a broken Reader surfaces as an error
// rather than a half-written document (every production caller marshals
// a small JSON struct into a *bytes.Reader before calling Put).
//
// When the body parses as a JSON object with a top-level RFC3339
// `expiresAt`, the timestamp is promoted to a top-level BSON field so
// the TTL index on events/cooldown can see it. A non-JSON body, or one
// with no expiresAt, writes no such field.
func (s *MongoStore) Put(ctx context.Context, path string, body io.Reader) (int64, error) {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return 0, errors.New("storage.MongoStore.Put: empty path")
	}
	buf, err := io.ReadAll(body)
	if err != nil {
		return 0, fmt.Errorf("storage.MongoStore.Put: read body: %w", err)
	}
	coll := s.collectionFor(path)
	doc := MongoDoc{
		ID:        path,
		Body:      primitive.Binary{Subtype: 0x00, Data: buf},
		UpdatedAt: time.Now().UTC(),
	}
	if t := extractExpiresAt(buf); t != nil {
		doc.ExpiresAt = t
	}
	_, err = coll.ReplaceOne(
		ctx,
		bson.M{"_id": path},
		doc,
		options.Replace().SetUpsert(true),
	)
	if err != nil {
		return 0, fmt.Errorf("storage.MongoStore.Put %q: %w", path, err)
	}
	return int64(len(buf)), nil
}

// Get returns a ReadCloser over the stored body. Missing key →
// ErrNotFound so the colocated stores' "absent vs backend-broken"
// splits still work.
func (s *MongoStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	path = strings.TrimPrefix(path, "/")
	coll := s.collectionFor(path)
	var doc MongoDoc
	err := coll.FindOne(ctx, bson.M{"_id": path}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("storage.MongoStore.Get %q: %w", path, err)
	}
	return io.NopCloser(bytes.NewReader(doc.Body.Data)), nil
}

// List returns every key whose _id starts with `prefix`, within the
// collection that `prefix` routes to.
//
// Uses an anchored `^<quoted-prefix>` regex rather than a `{$gte, $lt}`
// range trick: notifier's List calls are all against stable
// dir-shaped prefixes (`deliveries/pending/`, `tenants/`, …), so the
// regex is fast enough and the semantics match S3's prefix listing
// byte-for-byte.
func (s *MongoStore) List(ctx context.Context, prefix string) ([]StoredObject, error) {
	prefix = strings.TrimPrefix(prefix, "/")
	coll := s.collectionFor(prefix)
	filter := bson.M{}
	if prefix != "" {
		filter = bson.M{"_id": bson.M{"$regex": "^" + regexp.QuoteMeta(prefix)}}
	}
	opts := options.Find().SetProjection(bson.M{"_id": 1, "body": 1, "updatedAt": 1})
	cur, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("storage.MongoStore.List %q: %w", prefix, err)
	}
	defer cur.Close(ctx)
	var out []StoredObject
	for cur.Next(ctx) {
		var doc MongoDoc
		if err := cur.Decode(&doc); err != nil {
			return nil, fmt.Errorf("storage.MongoStore.List %q: decode: %w", prefix, err)
		}
		out = append(out, StoredObject{
			Path:       doc.ID,
			Size:       int64(len(doc.Body.Data)),
			ModifiedAt: doc.UpdatedAt,
		})
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("storage.MongoStore.List %q: cursor: %w", prefix, err)
	}
	return out, nil
}

// Delete removes one key. Missing → ErrNotFound, matching S3Store /
// FSStore so idempotent-delete paths work the same way on both
// backends.
func (s *MongoStore) Delete(ctx context.Context, path string) error {
	path = strings.TrimPrefix(path, "/")
	coll := s.collectionFor(path)
	res, err := coll.DeleteOne(ctx, bson.M{"_id": path})
	if err != nil {
		return fmt.Errorf("storage.MongoStore.Delete %q: %w", path, err)
	}
	if res.DeletedCount == 0 {
		return ErrNotFound
	}
	return nil
}

// Presign is not implemented — see file header. Returns a typed error
// so a future caller surfaces loudly rather than seeing an empty URL.
func (s *MongoStore) Presign(_ context.Context, _ string, _ time.Duration) (string, error) {
	return "", ErrPresignUnsupported
}

// PutIfAbsent is the ConditionalPutter contract.
//
// Implemented as InsertOne against the target collection. _id is unique
// by construction in Mongo, so a concurrent writer hitting the same
// path gets a duplicate-key error — translated to (false, nil), because
// contention is a normal outcome of the single-flight lease and not an
// error worth logging as one. Any other error is returned.
//
// This is the FIRST caller of PutIfAbsent that lands on go-embedb
// (observatory + backup never had it), and overlapping drains are
// precisely the duplicate-delivery vector the lease exists to close —
// see goembedb_test.go's TestMongoStore_PutIfAbsent_SingleFlight for
// the 100-iteration race assertion.
func (s *MongoStore) PutIfAbsent(ctx context.Context, path string, body []byte) (bool, error) {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return false, errors.New("storage.MongoStore.PutIfAbsent: empty path")
	}
	coll := s.collectionFor(path)
	doc := MongoDoc{
		ID:        path,
		Body:      primitive.Binary{Subtype: 0x00, Data: append([]byte(nil), body...)},
		UpdatedAt: time.Now().UTC(),
	}
	if t := extractExpiresAt(body); t != nil {
		doc.ExpiresAt = t
	}
	_, err := coll.InsertOne(ctx, doc)
	if err == nil {
		return true, nil
	}
	if mongo.IsDuplicateKeyError(err) {
		return false, nil
	}
	return false, fmt.Errorf("storage.MongoStore.PutIfAbsent %q: %w", path, err)
}

// Collection names. Lower-case, underscore-separated — the Mongo
// convention and the one go-embedb's own metadata uses.
const (
	CollectionEvents            = "events"
	CollectionDeliveriesPending = "deliveries_pending"
	CollectionDeliveriesDone    = "deliveries_done"
	CollectionChannels          = "channels"
	CollectionCooldown          = "cooldown"
	CollectionTenants           = "tenants"
	CollectionLocks             = "locks"
	CollectionKV                = "kv"
)

// Collections is the full set of collection names MongoStore may write
// to. Enumerated so the migration endpoint can produce a count
// breakdown that the operator can diff against the S3 LIST count.
var Collections = []string{
	CollectionEvents,
	CollectionDeliveriesPending,
	CollectionDeliveriesDone,
	CollectionChannels,
	CollectionCooldown,
	CollectionTenants,
	CollectionLocks,
	CollectionKV,
}

// collectionFor routes a path to its collection.
//
// Matches on the class segment the events / tenant blob stores write:
//
//	[<prefix>/]events/*              → events
//	[<prefix>/]deliveries/pending/*  → deliveries_pending
//	[<prefix>/]deliveries/done/*     → deliveries_done
//	[<prefix>/]channels/*            → channels
//	[<prefix>/]cooldown/*            → cooldown
//	[<prefix>/]tenants/*             → tenants
//	[<prefix>/]locks/*               → locks
//
// Anything else falls back to `kv` so a future caller that writes a
// path outside this tree is still served, just slowly on List.
func (s *MongoStore) collectionFor(path string) *mongo.Collection {
	return s.client.Database(s.dbName).Collection(collectionNameFor(path))
}

func collectionNameFor(path string) string {
	// Order: deliveries/{pending,done} checked before plain "deliveries/".
	switch {
	case containsSegment(path, "deliveries/pending/"):
		return CollectionDeliveriesPending
	case containsSegment(path, "deliveries/done/"):
		return CollectionDeliveriesDone
	case containsSegment(path, "events/"):
		return CollectionEvents
	case containsSegment(path, "channels/"):
		return CollectionChannels
	case containsSegment(path, "cooldown/"):
		return CollectionCooldown
	case containsSegment(path, "tenants/"):
		return CollectionTenants
	case containsSegment(path, "locks/"):
		return CollectionLocks
	}
	return CollectionKV
}

// containsSegment: strings.Contains anchored on a `/` boundary on the
// left so `myevents/` would NOT route to `events`.
func containsSegment(haystack, needle string) bool {
	i := strings.Index(haystack, needle)
	if i < 0 {
		return false
	}
	return i == 0 || haystack[i-1] == '/'
}

// extractExpiresAt pulls a top-level RFC3339 `expiresAt` out of a JSON
// object body, if any. Returns nil on non-JSON, non-object, absent
// field, or an unparseable value — Put then stores no expiresAt and
// the TTL index skips the record (safe: a record with no TTL is simply
// not swept, same behaviour the blob store had on S3).
func extractExpiresAt(buf []byte) *time.Time {
	if len(buf) == 0 || buf[0] != '{' {
		return nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(buf, &m); err != nil {
		return nil
	}
	raw, ok := m["expiresAt"]
	if !ok {
		return nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
		if err != nil {
			return nil
		}
	}
	tt := t.UTC()
	return &tt
}

// CollectionCounts returns a per-collection document count. Used by
// the migration endpoint to produce a verification report the operator
// can diff against the S3 LIST count.
func (s *MongoStore) CollectionCounts(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	for _, name := range Collections {
		n, err := s.client.Database(s.dbName).Collection(name).CountDocuments(ctx, bson.M{})
		if err != nil {
			return nil, fmt.Errorf("count %s: %w", name, err)
		}
		out[name] = n
	}
	return out, nil
}
