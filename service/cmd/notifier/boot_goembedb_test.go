// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package main

import (
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestBootGoEmbedOnce_LambdaWithoutBucketHardFails pins the invariant that
// a Lambda deploy with GOEMBEDB_S3_BUCKET unset refuses to boot rather
// than warning and falling back to /tmp/goembedb. Lambda's /tmp is per-
// container and dies with every container recycle; silent fallback means
// each warm container runs its own ephemeral store, which independently
// defeats:
//
//   - `AcquireTickLease` single-flight: every container wins its own
//     lease; the delivery drain stops being exclusive and double-delivers
//     to real recipients. Storage amnesia between containers silently
//     invalidates the exclusivity that storage.goembedb_test.go's
//     adapter-level race test proves in-process.
//   - idempotency keys and cooldown records: per-container means replay
//     dedupe + per-recipient cooldown reset on every cold start.
//
// The test is called from a fresh process via `t.Run` so the once.Do in
// bootGoEmbed does not interfere; bootGoEmbedOnce is called directly to
// bypass the outer sync.Once.
//
// Inversion to confirm the test can fail: unset AWS_LAMBDA_FUNCTION_NAME
// (the sibling test TestBootGoEmbedOnce_NonLambdaWithoutBucketWarns);
// bootGoEmbedOnce must NOT return the GOEMBEDB_S3_BUCKET-named error in
// that mode. Without the inversion, this test would also pass against
// the pre-fix code that unconditionally warned.
func TestBootGoEmbedOnce_LambdaWithoutBucketHardFails(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("GOEMBEDB_SQLITE_DIR", tmp)
	t.Setenv("AWS_LAMBDA_FUNCTION_NAME", "notifier-staging-test")
	// Belt-and-braces: explicitly unset so the test is deterministic
	// regardless of ambient env.
	t.Setenv("GOEMBEDB_S3_BUCKET", "")

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	_, err := bootGoEmbedOnce(logger)
	if err == nil {
		t.Fatal("bootGoEmbedOnce must refuse to boot on Lambda with no bucket; got nil error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "GOEMBEDB_S3_BUCKET") {
		t.Errorf("error must name GOEMBEDB_S3_BUCKET so the operator can find the fix; got: %s", msg)
	}
	if !strings.Contains(strings.ToLower(msg), "lambda") {
		t.Errorf("error must name Lambda so the Lambda-specific reason is visible; got: %s", msg)
	}
}

// TestBootGoEmbedOnce_NonLambdaWithoutBucketWarns inverts the assertion:
// outside Lambda (ECS / VM / local smoke) a single-node sqlite embed is
// a valid "just put it in sqlite" deploy, so no-bucket must proceed with
// a warning rather than return the Lambda error.
//
// Without this inversion, a pre-fix implementation that unconditionally
// returned the GOEMBEDB_S3_BUCKET error would pass the Lambda-hard-fail
// test and silently break every non-Lambda deploy.
//
// We only assert that bootGoEmbedOnce does NOT return the specific
// GOEMBEDB_S3_BUCKET error; later-stage I/O errors (sqlite path, ferretdb
// Listen, etc.) are fine — they fail for unrelated reasons and are not
// this test's subject.
func TestBootGoEmbedOnce_NonLambdaWithoutBucketWarns(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("GOEMBEDB_SQLITE_DIR", tmp)
	// `AWS_LAMBDA_FUNCTION_NAME` empty forces isLambda() = false.
	t.Setenv("AWS_LAMBDA_FUNCTION_NAME", "")
	t.Setenv("GOEMBEDB_S3_BUCKET", "")

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	_, err := bootGoEmbedOnce(logger)
	if err != nil && strings.Contains(err.Error(), "GOEMBEDB_S3_BUCKET") &&
		strings.Contains(strings.ToLower(err.Error()), "lambda") {
		t.Errorf("non-Lambda no-bucket must NOT return the Lambda error; got: %s", err)
	}
	// Any other error (listener bind, etc.) is fine for this test's
	// subject — we are not asserting bootGoEmbedOnce succeeds in a unit
	// test env, only that the Lambda guard does not fire.
}
