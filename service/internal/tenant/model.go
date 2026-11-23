// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package tenant

import "time"

// Record is one tenant row — one `(consumer-stack, version)` pair.
//
// The publisher plugin's push-register call produces the StackName /
// Version / Resources fields; the service fills in CreatedAt /
// LastSeenAt.
//
// A tenant here is a PUBLISHER: the backup or observatory component that
// sends events. Its stack name becomes the `source` on every event it
// publishes (filled from the JWT's tenant claim, never from the body),
// so this row is what a channel's `sources:` allowlist is checked
// against.
//
// Tags are JSON only. The scaffold carried `bson:` tags alongside them
// from its Mongo original; they were removed with the Mongo store,
// because a tag for a driver that is no longer a dependency reads like
// evidence of a database path that does not exist.
type Record struct {
	StackName   string     `json:"stackName"`
	Version     string     `json:"version"`
	Resources   []Resource `json:"resources"`
	OrgID       string     `json:"orgId,omitempty"`
	CreatedAt   time.Time  `json:"createdAt"`
	LastSeenAt  time.Time  `json:"lastSeenAt"`
	SoftDeleted bool       `json:"softDeleted,omitempty"`
}

// Resource describes one sibling resource the publisher has (DB, S3, …).
// A pass-through for the notifier — it does not act on these, but an
// operator reading the SPA wants to know what the publisher manages, and
// a future channel could route on it.
type Resource struct {
	Type       string            `json:"type"`
	Name       string            `json:"name"`
	Connection map[string]string `json:"connection,omitempty"`
}
