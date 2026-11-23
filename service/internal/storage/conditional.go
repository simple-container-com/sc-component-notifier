// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// ConditionalPutter is an OPTIONAL extension to Store: a put that
// succeeds only if the key does not already exist.
//
// It is a separate interface rather than a Store method so the three
// existing backends (and observatory's copy of this package, which this
// one is derived from) do not all have to grow a method most of them
// cannot implement atomically. Callers type-assert and degrade.
//
// The one caller is the delivery drain's single-flight lease. That is
// also why atomicity matters: two overlapping ticks that both "check
// then put" would both win, and overlapping drains are precisely the
// duplicate-delivery vector the lease exists to close.
type ConditionalPutter interface {
	// PutIfAbsent writes body at path only if nothing is there.
	// Returns false (with a nil error) when the key already existed.
	PutIfAbsent(ctx context.Context, path string, body []byte) (bool, error)
}

// ErrPreconditionFailed is returned by backends that can detect the
// race but cannot report it as a clean boolean.
var ErrPreconditionFailed = errors.New("storage: precondition failed")

// PutIfAbsent implements ConditionalPutter using S3's If-None-Match.
//
// `If-None-Match: *` on PutObject has been supported since late 2024 and
// is the documented way to do a compare-and-swap create on S3. A loser
// gets HTTP 412 PreconditionFailed, which is translated to (false, nil)
// — contention is a normal outcome here, not an error worth logging as
// one.
func (s *S3Store) PutIfAbsent(ctx context.Context, path string, body []byte) (bool, error) {
	path = strings.TrimPrefix(path, "/")
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.cfg.Bucket),
		Key:         aws.String(path),
		Body:        bytes.NewReader(body),
		IfNoneMatch: aws.String("*"),
	})
	if err == nil {
		return true, nil
	}
	if isPreconditionFailed(err) {
		return false, nil
	}
	return false, fmt.Errorf("s3 conditional put %q: %w", path, err)
}

// isPreconditionFailed matches S3's 412. Checked by API error code
// rather than by string, so a reworded message cannot turn "someone
// else holds the lease" into "the storage backend is broken".
func isPreconditionFailed(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return true
		}
	}
	return false
}

// PutIfAbsent implements ConditionalPutter for the in-memory store,
// atomically under its own mutex. Used by tests, including the
// overlapping-drain regression test.
func (s *InMemoryStore) PutIfAbsent(_ context.Context, path string, body []byte) (bool, error) {
	path = strings.TrimPrefix(path, "/")
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.objects[path]; exists {
		return false, nil
	}
	s.objects[path] = memObject{body: append([]byte(nil), body...), modifiedAt: time.Now().UTC()}
	return true, nil
}
