// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package tenant

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/simple-container-com/sc-component-notifier/service/internal/storage"
)

func newBlobFixture(t *testing.T) (*BlobStore, *storage.InMemoryStore) {
	t.Helper()
	st := storage.NewInMemoryStore()
	return NewBlobStore(st, ""), st
}

func rec(name string) Record {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	return Record{
		StackName:  name,
		Version:    "2026.11.23-abc1234",
		CreatedAt:  now,
		LastSeenAt: now,
		Resources: []Resource{
			{Type: "mongodb", Name: "main-db", Connection: map[string]string{"host": "db.internal"}},
		},
	}
}

func TestUpsertAndGetRoundTrip(t *testing.T) {
	b, _ := newBlobFixture(t)
	ctx := context.Background()

	if err := b.Upsert(ctx, rec("forge-conductor")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	got, found, err := b.Get(ctx, "forge-conductor")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("the tenant just written was not found")
	}
	if got.Version != "2026.11.23-abc1234" {
		t.Fatalf("version round-tripped as %q", got.Version)
	}
	// The Resources slice is a pass-through and must survive intact —
	// a publisher sends it once at register time and nothing re-sends it.
	if len(got.Resources) != 1 || got.Resources[0].Connection["host"] != "db.internal" {
		t.Fatalf("resources did not round-trip: %+v", got.Resources)
	}
}

// A missing tenant is a normal read result, not an error. Returning an
// error here would make the register handler's "have I seen this stack
// before" check indistinguishable from a broken bucket.
func TestGetMissingTenantIsNotAnError(t *testing.T) {
	b, _ := newBlobFixture(t)

	_, found, err := b.Get(context.Background(), "never-registered")
	if err != nil {
		t.Fatalf("Get on a missing tenant returned an error: %v", err)
	}
	if found {
		t.Fatal("a tenant that was never written was found")
	}
}

func TestUpsertOverwrites(t *testing.T) {
	b, st := newBlobFixture(t)
	ctx := context.Background()

	first := rec("forge-conductor")
	if err := b.Upsert(ctx, first); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	second := first
	second.Version = "2026.11.24-def5678"
	second.LastSeenAt = first.LastSeenAt.Add(time.Hour)
	if err := b.Upsert(ctx, second); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// One object, not two: the stack name is the identity, so a
	// re-registering publisher must not accumulate a row per deploy.
	objs, err := st.List(ctx, "tenants/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(objs) != 1 {
		t.Fatalf("got %d objects after two upserts of one stack, want 1", len(objs))
	}

	got, _, err := b.Get(ctx, "forge-conductor")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Version != "2026.11.24-def5678" {
		t.Fatalf("the second upsert did not win: %q", got.Version)
	}
}

func TestUpsertRequiresAStackName(t *testing.T) {
	b, st := newBlobFixture(t)
	ctx := context.Background()

	if err := b.Upsert(ctx, Record{Version: "1"}); err == nil {
		t.Fatal("a nameless tenant was accepted")
	}
	objs, err := st.List(ctx, "tenants/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(objs) != 0 {
		t.Fatalf("a nameless tenant wrote %d objects", len(objs))
	}
}

func TestListIsSortedByStackName(t *testing.T) {
	b, _ := newBlobFixture(t)
	ctx := context.Background()

	for _, n := range []string{"forge-storage", "forge-aigateway", "forge-conductor"} {
		if err := b.Upsert(ctx, rec(n)); err != nil {
			t.Fatalf("Upsert %s: %v", n, err)
		}
	}

	rows, err := b.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// Names are HASHED into keys, so the storage layer's own ordering is
	// arbitrary. Without the explicit sort the SPA's tenant list would
	// reshuffle on every page load.
	want := []string{"forge-aigateway", "forge-conductor", "forge-storage"}
	if len(rows) != len(want) {
		t.Fatalf("got %d tenants, want %d", len(rows), len(want))
	}
	for i, w := range want {
		if rows[i].StackName != w {
			t.Fatalf("row %d is %q, want %q (order: %v)", i, rows[i].StackName, w, rows)
		}
	}
}

// Soft, because a publisher that stops registering is the normal result
// of its stack being destroyed, and "this used to send events and
// stopped" is a thing an operator wants to be able to read.
func TestSoftDeleteKeepsTheRow(t *testing.T) {
	b, _ := newBlobFixture(t)
	ctx := context.Background()

	if err := b.Upsert(ctx, rec("forge-conductor")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := b.SoftDelete(ctx, "forge-conductor"); err != nil {
		t.Fatalf("SoftDelete: %v", err)
	}

	got, found, err := b.Get(ctx, "forge-conductor")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("SoftDelete removed the row")
	}
	if !got.SoftDeleted {
		t.Fatal("the row is present but not marked deleted")
	}
	if len(got.Resources) != 1 {
		t.Fatal("SoftDelete discarded the rest of the record")
	}
}

func TestSoftDeleteOfAMissingTenantIsANoOp(t *testing.T) {
	b, _ := newBlobFixture(t)

	if err := b.SoftDelete(context.Background(), "never-registered"); err != nil {
		t.Fatalf("SoftDelete on a missing tenant errored: %v", err)
	}
}

// Stack names arrive over HTTP and are concatenated into a storage path.
// Hashing removes the whole traversal-and-weird-character class rather
// than enumerating it, so a name with a slash or a `..` must land in one
// flat key under tenants/ and read back as itself.
func TestHostileStackNameCannotEscapeThePrefix(t *testing.T) {
	b, st := newBlobFixture(t)
	ctx := context.Background()

	hostile := "../../etc/passwd"
	if err := b.Upsert(ctx, rec(hostile)); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	objs, err := st.List(ctx, "tenants/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(objs) != 1 {
		t.Fatalf("got %d objects, want 1", len(objs))
	}
	if strings.Contains(objs[0].Path, "..") || strings.Count(objs[0].Path, "/") != 1 {
		t.Fatalf("the name leaked into the key: %q", objs[0].Path)
	}

	// And the readable name survives, in the BODY — which is what keeps a
	// hashed key legible when an operator lists tenants.
	got, found, err := b.Get(ctx, hostile)
	if err != nil || !found {
		t.Fatalf("Get(%q): found=%v err=%v", hostile, found, err)
	}
	if got.StackName != hostile {
		t.Fatalf("stack name round-tripped as %q", got.StackName)
	}
}

// Two names that differ only in a character the key cannot carry must
// still be two tenants. A sanitiser that replaced "/" with "-" would
// collapse these into one row; a hash does not.
func TestSimilarNamesDoNotCollide(t *testing.T) {
	b, _ := newBlobFixture(t)
	ctx := context.Background()

	for _, n := range []string{"forge/conductor", "forge-conductor"} {
		if err := b.Upsert(ctx, rec(n)); err != nil {
			t.Fatalf("Upsert %s: %v", n, err)
		}
	}

	rows, err := b.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d tenants, want 2: %v", len(rows), rows)
	}
}
