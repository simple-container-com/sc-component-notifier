// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// Package mongox holds the small Mongo error classifications shared by
// the go-embedb-backed stores. It exists for one reason: on go-embedb,
// a boot that is not the lease owner must still boot.
//
// Deliberately duplicated from sc-component-backup's package of the
// same name rather than shared. The fleet has no live shared-contracts
// module (forge-contracts is dormant and imported by nothing), and the
// payload here is the doc comment on TolerateNotWritablePrimary, which
// is worth having in front of whoever is reading the boot path of
// whichever component just crash-looped.
package mongox

import (
	"errors"
	"log/slog"

	"go.mongodb.org/mongo-driver/mongo"
)

// codeNotWritablePrimary is the Mongo wire-spec code for
// NotWritablePrimary, raised by go-embedb's per-tenant lease gate when
// an RMW/DDL op reaches a replica that does not hold the write lease.
// Mirrors `handlererrors.ErrNotWritablePrimary` in go-embedb.
const codeNotWritablePrimary = 10107

// IsNotWritablePrimary reports whether err is go-embedb's (or Mongo's)
// NotWritablePrimary(10107) refusal.
func IsNotWritablePrimary(err error) bool {
	if err == nil {
		return false
	}

	var se mongo.ServerError
	if errors.As(err, &se) {
		return se.HasErrorCode(codeNotWritablePrimary)
	}

	return false
}

// TolerateNotWritablePrimary swallows a NotWritablePrimary refusal and
// returns every other error unchanged.
//
// Why this is correct rather than a papered-over write failure, and why
// it is specifically scoped to index creation on the BOOT path:
//
// go-embedb grants one write lease per tenant. Every DDL op — including
// `createIndexes` — is classified RMW and refused on a non-owner with
// NotWritablePrimary(10107) plus a `primary` hint naming the owner, so a
// multi-host Mongo client can retry there. That contract holds when the
// replicas are mutually routable.
//
// On Lambda they are not. Each execution environment is its own VM with
// no inbound path, so every slot advertises the loopback placeholder
// `127.0.0.1:0` (see `slots.json`) and the `primary` hint can never be
// dialled. The refusal is therefore not "retry at the owner" — it is a
// dead end.
//
// Treating it as fatal took sc-component-backup down for over an hour on
// 2026-11-24: its `meta store init` created six indexes at boot and
// `os.Exit(1)`d on any error, so every execution environment that was
// not the lease owner died ~880ms into init. Lambda surfaces that as a
// 502 and answers the retry by spawning ANOTHER environment, which
// claims another slot and dies the same way — a self-amplifying loop
// that reached five slots (`r-01`..`r-05`) and epoch 43 on one of them,
// and did not self-heal.
//
// The notifier's own `MongoStore.EnsureIndexes` had the identical shape
// (two TTL indexes, `os.Exit(2)` on failure) and was green only because
// its traffic has never driven concurrency above one. That is not a
// property worth depending on.
//
// Swallowing it is safe because the index is not lost:
//   - the lease owner creates it, and that write replicates;
//   - a replica bootstraps from a snapshot that already carries it;
//   - `createIndexes` is idempotent, so the next boot that does own the
//     lease converges.
//
// A non-owner that skipped them can still serve reads, which is the
// whole value of a replica, and it cannot silently diverge on writes:
// the same lease gate refuses its RMW ops at request time, by design.
//
// The one thing this does cost on the notifier specifically: until the
// lease owner has created them, the TTL sweep on events/ and cooldown/
// is not armed, so records written in that window age out late rather
// than never (the index is retroactive once created). Weighed against a
// boot loop that serves nothing at all, late is the better failure.
//
// Deliberately NOT applied to `mongo.Connect` or `Ping` — a replica
// that cannot reach its own embedded store has nothing to serve and
// SHOULD fail the boot.
//
// The `index` label names the index being skipped so a WARN identifies
// which one, rather than several indistinguishable lines.
func TolerateNotWritablePrimary(index string, err error) error {
	if IsNotWritablePrimary(err) {
		slog.Warn("index create skipped: this replica does not hold the go-embedb write lease; "+
			"the lease owner creates it and the write replicates",
			"index", index, "err", err)

		return nil
	}

	return err
}
