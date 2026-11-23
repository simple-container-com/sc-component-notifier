// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package tenant

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

	"github.com/simple-container-com/sc-component-notifier/service/internal/storage"
)

// BlobStore is the production Store: one JSON object per tenant in
// whatever `storage: <resource-name>` points at.
//
// Same backend as the events store, under a sibling prefix — so a single
// bucket holds the whole component's state and there is exactly one
// thing for an operator to back up, point at, or delete.
type BlobStore struct {
	st     storage.Store
	prefix string
}

// NewBlobStore wraps an object store. prefix is optional and is
// normalised to end in "/".
func NewBlobStore(st storage.Store, prefix string) *BlobStore {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &BlobStore{st: st, prefix: prefix}
}

func (b *BlobStore) key(stackName string) string {
	return b.prefix + "tenants/" + safeName(stackName) + ".json"
}

// safeName hashes an operator- or publisher-supplied name into a key
// segment, the same way the events store does.
//
// A stack name is in practice `[a-z0-9-]`, but it arrives over HTTP and
// is concatenated into a storage path — "in practice" is not access
// control. Hashing removes the whole traversal-and-weird-character class
// rather than enumerating it. The readable name is
// in the object BODY, so a List is still legible.
func safeName(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func (b *BlobStore) Upsert(ctx context.Context, record Record) error {
	if record.StackName == "" {
		return errors.New("stackName is required")
	}
	buf, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal tenant %s: %w", record.StackName, err)
	}
	if _, err := b.st.Put(ctx, b.key(record.StackName), bytes.NewReader(buf)); err != nil {
		return fmt.Errorf("put tenant %s: %w", record.StackName, err)
	}
	return nil
}

func (b *BlobStore) List(ctx context.Context) ([]Record, error) {
	objs, err := b.st.List(ctx, b.prefix+"tenants/")
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	out := make([]Record, 0, len(objs))
	for _, o := range objs {
		var rec Record
		found, err := b.getJSON(ctx, o.Path, &rec)
		if err != nil {
			return nil, err
		}
		// A row that vanished between the List and the Get is skipped,
		// not an error: the listing is a snapshot and a concurrent
		// delete is legitimate.
		if !found {
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StackName < out[j].StackName })
	return out, nil
}

func (b *BlobStore) Get(ctx context.Context, stackName string) (Record, bool, error) {
	var rec Record
	found, err := b.getJSON(ctx, b.key(stackName), &rec)
	return rec, found, err
}

// SoftDelete marks a tenant gone without removing the row.
//
// Soft, because a tenant that stops registering is the normal result of
// a consumer stack being destroyed, and the operator still wants to see
// that it existed — "this publisher used to send events and stopped" is
// a thing worth being able to read.
func (b *BlobStore) SoftDelete(ctx context.Context, stackName string) error {
	rec, found, err := b.Get(ctx, stackName)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	rec.SoftDeleted = true
	return b.Upsert(ctx, rec)
}

// Close is a no-op: an object store holds no connection to drain.
func (b *BlobStore) Close(context.Context) error { return nil }

// getJSON returns (false, nil) when the key is absent — a missing tenant
// is a normal read result, not an error.
func (b *BlobStore) getJSON(ctx context.Context, key string, v any) (bool, error) {
	rc, err := b.st.Get(ctx, key)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("get %s: %w", key, err)
	}
	defer func() { _ = rc.Close() }()

	buf, err := io.ReadAll(rc)
	if err != nil {
		return false, fmt.Errorf("read %s: %w", key, err)
	}
	if err := json.Unmarshal(buf, v); err != nil {
		return false, fmt.Errorf("unmarshal %s: %w", key, err)
	}
	return true, nil
}
