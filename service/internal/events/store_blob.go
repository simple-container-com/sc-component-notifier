// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package events

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/simple-container-com/sc-component-notifier/service/internal/storage"
)

// Key layout inside the referenced storage resource.
//
// Deliveries are split pending/ vs done/ rather than kept in one prefix
// with a status field, because ListDue runs on EVERY tick: a single
// prefix would make the drain's cost grow with total history forever,
// and the drain is the one code path that must stay cheap. A terminal
// row is MOVED, not deleted — the SPA reads both prefixes.
const (
	prefixEvents          = "events/"
	prefixDeliveryPending = "deliveries/pending/"
	prefixDeliveryDone    = "deliveries/done/"
	prefixChannels        = "channels/"
	prefixCooldown        = "cooldown/"
	keyTickLease          = "locks/delivery-tick.lock"
)

// Compile-time conformance. Cheap, and it means adding a method to
// Store fails here rather than at the one call site that happened to be
// wired up last.
var _ Store = (*BlobStore)(nil)

// BlobStore implements Store over a storage.Store.
type BlobStore struct {
	st     storage.Store
	prefix string
}

// NewBlobStore wraps a byte store. prefix namespaces every key, so one
// bucket can host several components.
func NewBlobStore(st storage.Store, prefix string) *BlobStore {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &BlobStore{st: st, prefix: prefix}
}

func (b *BlobStore) key(parts ...string) string { return b.prefix + strings.Join(parts, "") }

func (b *BlobStore) putJSON(ctx context.Context, key string, v any) error {
	buf, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	if _, err := b.st.Put(ctx, key, bytes.NewReader(buf)); err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

// getJSON returns (false, nil) when the key is absent — missing is a
// normal outcome for most callers here, and forcing each to match on
// ErrNotFound invites one of them to treat a backend failure as absence.
func (b *BlobStore) getJSON(ctx context.Context, key string, v any) (bool, error) {
	rc, err := b.st.Get(ctx, key)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("get %s: %w", key, err)
	}
	defer rc.Close()
	buf, err := io.ReadAll(rc)
	if err != nil {
		return false, fmt.Errorf("read %s: %w", key, err)
	}
	if err := json.Unmarshal(buf, v); err != nil {
		return false, fmt.Errorf("unmarshal %s: %w", key, err)
	}
	return true, nil
}

// safeName makes an arbitrary id safe as a single path segment.
//
// Event ids are publisher-supplied and the recommended shape contains
// colons ("backup.failed:<tenant>:<run-id>"); a delivery id adds a pipe.
// Hashing sidesteps every path-traversal and
// character-the-backend-dislikes question at once. The id is retained
// INSIDE the object, so nothing is lost for the SPA.
func safeName(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

type eventSeenRecord struct {
	ID        string    `json:"id"`
	FirstSeen time.Time `json:"firstSeen"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// SeenEvent is check-and-record in one call.
func (b *BlobStore) SeenEvent(ctx context.Context, id string, now time.Time) (bool, error) {
	key := b.key(prefixEvents, safeName(id), ".json")
	var rec eventSeenRecord
	found, err := b.getJSON(ctx, key, &rec)
	if err != nil {
		// An unreadable dedupe record must NOT be treated as "not
		// seen". Failing the ingest with a 500 makes the publisher
		// retry, which is recoverable; guessing "not seen" risks a
		// duplicate alert, which is the thing this whole file exists
		// to prevent.
		return false, err
	}
	if found && now.Before(rec.ExpiresAt) {
		return true, nil
	}
	// Absent or expired → record afresh.
	return false, b.putJSON(ctx, key, eventSeenRecord{
		ID:        id,
		FirstSeen: now,
		ExpiresAt: now.Add(DedupeWindow),
	})
}

func (b *BlobStore) pendingKey(id string) string {
	return b.key(prefixDeliveryPending, safeName(id), ".json")
}

func (b *BlobStore) doneKey(id string) string {
	return b.key(prefixDeliveryDone, safeName(id), ".json")
}

// PutDelivery writes the row to the prefix matching its state, and
// cleans up the other one so a row can never be listed twice.
func (b *BlobStore) PutDelivery(ctx context.Context, d *Delivery) error {
	if d.Done() {
		if err := b.putJSON(ctx, b.doneKey(d.ID), d); err != nil {
			return err
		}
		// Best-effort, and deliberately not an error: if this delete
		// fails the row stays in pending/ and the next drain re-reads
		// it — which is SAFE, because the drain re-checks Done() on
		// every row. Ordering is what matters: done/ is written FIRST,
		// so a crash between the two leaves a duplicate listing rather
		// than a lost row.
		_ = b.st.Delete(ctx, b.pendingKey(d.ID))
		return nil
	}
	return b.putJSON(ctx, b.pendingKey(d.ID), d)
}

func (b *BlobStore) GetDelivery(ctx context.Context, id string) (*Delivery, error) {
	var d Delivery
	found, err := b.getJSON(ctx, b.pendingKey(id), &d)
	if err != nil {
		return nil, err
	}
	if found {
		return &d, nil
	}
	found, err = b.getJSON(ctx, b.doneKey(id), &d)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, storage.ErrNotFound
	}
	return &d, nil
}

// ListDue returns rows still needing an attempt, oldest first.
func (b *BlobStore) ListDue(ctx context.Context, _ time.Time, limit int) ([]*Delivery, error) {
	objs, err := b.st.List(ctx, b.key(prefixDeliveryPending))
	if err != nil {
		return nil, fmt.Errorf("list pending deliveries: %w", err)
	}
	out := make([]*Delivery, 0, len(objs))
	for _, o := range objs {
		var d Delivery
		found, err := b.getJSON(ctx, o.Path, &d)
		if err != nil || !found {
			// One corrupt row must not stall the whole queue — the
			// other alerts in it are the point. Skipped silently here
			// and surfaced by the SPA, which lists the same prefix.
			continue
		}
		if d.Done() {
			continue
		}
		out = append(out, &d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ListDeliveries returns recent rows from both prefixes, newest first.
func (b *BlobStore) ListDeliveries(ctx context.Context, limit int) ([]*Delivery, error) {
	out := make([]*Delivery, 0, limit)
	for _, p := range []string{prefixDeliveryPending, prefixDeliveryDone} {
		objs, err := b.st.List(ctx, b.key(p))
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", p, err)
		}
		for _, o := range objs {
			var d Delivery
			found, err := b.getJSON(ctx, o.Path, &d)
			if err != nil || !found {
				continue
			}
			out = append(out, &d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (b *BlobStore) channelKey(name string) string {
	return b.key(prefixChannels, safeName(name), ".json")
}

func (b *BlobStore) PutChannel(ctx context.Context, c *ChannelRecord) error {
	return b.putJSON(ctx, b.channelKey(c.Name), c)
}

func (b *BlobStore) GetChannel(ctx context.Context, name string) (*ChannelRecord, error) {
	var c ChannelRecord
	found, err := b.getJSON(ctx, b.channelKey(name), &c)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, storage.ErrNotFound
	}
	return &c, nil
}

func (b *BlobStore) ListChannels(ctx context.Context) ([]*ChannelRecord, error) {
	objs, err := b.st.List(ctx, b.key(prefixChannels))
	if err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}
	out := make([]*ChannelRecord, 0, len(objs))
	for _, o := range objs {
		var c ChannelRecord
		found, err := b.getJSON(ctx, o.Path, &c)
		if err != nil || !found {
			continue
		}
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (b *BlobStore) DeleteChannel(ctx context.Context, name string) error {
	if err := b.st.Delete(ctx, b.channelKey(name)); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return storage.ErrNotFound
		}
		return fmt.Errorf("delete channel %q: %w", name, err)
	}
	return nil
}

type cooldownRecord struct {
	Key       string    `json:"key"`
	StartedAt time.Time `json:"startedAt"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// CooldownActive reports whether key is suppressed, starting a new quiet
// period when it is not.
func (b *BlobStore) CooldownActive(ctx context.Context, key string, window time.Duration, now time.Time) (bool, error) {
	if window <= 0 {
		return false, nil
	}
	k := b.key(prefixCooldown, safeName(key), ".json")
	var rec cooldownRecord
	found, err := b.getJSON(ctx, k, &rec)
	if err != nil {
		// Unlike SeenEvent, an unreadable cooldown record fails OPEN —
		// the two failures are not symmetric. A missed dedupe sends a
		// duplicate; a missed cooldown also sends a duplicate, but a
		// cooldown that errors CLOSED would suppress a real alert, and
		// silence is the worse failure for a notifier. So: log and
		// deliver.
		return false, nil
	}
	if found && now.Before(rec.ExpiresAt) {
		return true, nil
	}
	return false, b.putJSON(ctx, k, cooldownRecord{
		Key:       key,
		StartedAt: now,
		ExpiresAt: now.Add(window),
	})
}

type tickLease struct {
	HeldSince time.Time `json:"heldSince"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// AcquireTickLease takes the drain's single-flight lease.
//
// On a ConditionalPutter backend (S3, and the in-memory store used by
// the overlapping-drain test) this is atomic. On others it degrades to
// read-then-write, which is sound only because those backends run
// single-process — and that caveat is why the type assertion is explicit
// rather than hidden behind a capability flag.
func (b *BlobStore) AcquireTickLease(ctx context.Context, ttl time.Duration, now time.Time) (bool, error) {
	key := b.key(keyTickLease)

	var existing tickLease
	found, err := b.getJSON(ctx, key, &existing)
	if err != nil {
		return false, err
	}
	if found {
		if now.Before(existing.ExpiresAt) {
			return false, nil
		}
		// Expired: a previous drain died mid-flight (a Lambda timeout,
		// an OOM). Clear it so the conditional put below can win —
		// without this the queue would stall until someone noticed.
		if err := b.st.Delete(ctx, key); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return false, fmt.Errorf("clear expired tick lease: %w", err)
		}
	}

	body, err := json.Marshal(tickLease{HeldSince: now, ExpiresAt: now.Add(ttl)})
	if err != nil {
		return false, err
	}
	if cp, ok := b.st.(storage.ConditionalPutter); ok {
		return cp.PutIfAbsent(ctx, key, body)
	}
	if _, err := b.st.Put(ctx, key, bytes.NewReader(body)); err != nil {
		return false, fmt.Errorf("take tick lease: %w", err)
	}
	return true, nil
}

func (b *BlobStore) ReleaseTickLease(ctx context.Context) error {
	if err := b.st.Delete(ctx, b.key(keyTickLease)); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("release tick lease: %w", err)
	}
	return nil
}
