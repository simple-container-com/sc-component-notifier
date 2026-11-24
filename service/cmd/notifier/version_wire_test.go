// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// The build stamp, end to end: ldflags -> main.version -> the JSON on
// /healthz (what a smoke reads) and on /api/v1/session (what the SPA
// header renders).
//
// Why a binary test and not a handler test. The handler tests in
// internal/api build their own SessionConfig, so every one of them
// passes with main.go never setting Version at all. That is the exact
// shape of the bug this repo has now shipped twice — a value that is
// present, correct and READ BY NOTHING, logging identically to a working
// deploy (`signingKey` as a bare ARN on backup-smoke-1d, then
// NOTIFIER_OPERATOR_PASSWORD on the first backup.simple-forge.com
// login). The only honest assertion is to ask the real binary what
// build it is.
//
// Why the field exists at all: a pin bump was unverifiable from
// outside. The version was logged once at boot and exposed nowhere, and
// reading that log needs AWS credentials the person smoking a deploy
// usually does not have — so "the provision went green" was the only
// evidence that a new image was serving, which is evidence that the
// apply ran, not that this process is the build you merged.
//
// Both surfaces are asserted here because they are wired SEPARATELY in
// main.go (api.Healthz(version) and SessionConfig.Version) and the
// /healthz one is the one a smoke reaches with no credentials.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A value no default could produce: `version` falls back to "dev" and a
// real build stamps CalVer, so neither can pass this by accident.
const wantVersion = "v2026.11.24-wiretest"

func TestBinaryReportsItsBuildOnHealthzAndSession(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary; skipped under -short")
	}

	bin := filepath.Join(t.TempDir(), "notifier")
	build := exec.Command("go", "build",
		"-ldflags", "-X main.version="+wantVersion, "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	port := freePort(t)
	const password = "correct-horse-battery-staple"

	cmd := exec.Command(bin)
	cmd.Env = append(
		os.Environ(),
		// LOCAL_DEBUG=true is what makes the SDK serve ordinary HTTP
		// instead of waiting for the Lambda Runtime API. The deployed
		// shape is covered by lambda_runtime_test.go; what matters here
		// is the handler wiring, which is identical on both branches.
		"LOCAL_DEBUG=true",
		fmt.Sprintf("PORT=%d", port),
		"SERVICE_JWT_SECRET=0123456789abcdef0123456789abcdef",
		"NOTIFIER_OPERATOR_PASSWORD="+password,
		"STORAGE_TYPE=filesystem",
		"STORAGE_PATH="+t.TempDir(),
		"NOTIFIER_CHANNELS=",
		// Explicitly off: this test must not depend on the go-embedb
		// branch, which needs a bucket on Lambda and is not what is
		// under test.
		"USE_GOEMBEDB=0",
	)
	var logs strings.Builder
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start binary: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForHealthz(t, base, &logs)

	client := &http.Client{Timeout: 10 * time.Second}

	// /healthz first, with no cookie at all: this is the exact call a
	// post-deploy smoke makes, and it is the one that must not need a
	// credential.
	health := getJSON(t, client, base+"/healthz")
	if got, _ := health["version"].(string); got != wantVersion {
		t.Fatalf("GET /healthz reported version %q, want %q.\n"+
			"This is the only probe a smoke can reach without AWS credentials or an operator "+
			"password, so losing the build here is what makes a pin bump unverifiable from "+
			"outside — the state this test exists to prevent returning to.", got, wantVersion)
	}
	if got, _ := health["status"].(string); got != "ok" {
		t.Fatalf("GET /healthz status is %q, want \"ok\" — the Lambda health check and "+
			"lambda_runtime_test.go both match on that substring", got)
	}

	// Log in exactly the way the operator smoke does.
	body, err := json.Marshal(map[string]string{"password": password})
	if err != nil {
		t.Fatalf("marshal login: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, base+"/api/v1/login", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build login request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST /api/v1/login: %v\nlogs:\n%s", err, logs.String())
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("POST /api/v1/login returned %d, want 204\nlogs:\n%s", resp.StatusCode, logs.String())
	}
	var cookie *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == "sc_notifier_session" {
			cookie = c
		}
	}
	if cookie == nil {
		t.Fatalf("login set no session cookie\nlogs:\n%s", logs.String())
	}

	authed := getJSONWithCookie(t, client, base+"/api/v1/session", cookie)
	got, _ := authed["version"].(string)
	if got != wantVersion {
		t.Fatalf("the authenticated session probe reported version %q, want %q.\n"+
			"The binary was built with -X main.version=%s, so either main.go stopped passing "+
			"`version` into api.SessionConfig or the handler stopped reporting it. /healthz is "+
			"wired separately, so it can still be right while this is wrong.\nlogs:\n%s",
			got, wantVersion, wantVersion, logs.String())
	}
}

func getJSON(t *testing.T, client *http.Client, url string) map[string]any {
	t.Helper()
	return getJSONWithCookie(t, client, url, nil)
}

func getJSONWithCookie(t *testing.T, client *http.Client, url string, cookie *http.Cookie) map[string]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build request for %s: %v", url, err)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s returned %d, want 200", url, resp.StatusCode)
	}
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode %s body: %v", url, err)
	}
	return got
}

// freePort asks the kernel for one and closes it, rather than hardcoding
// 8080 — these tests run alongside whatever else is on the box, and a
// port clash would fail as a connection error that reads like a broken
// binary.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("release the reserved port: %v", err)
	}
	return port
}

func waitForHealthz(t *testing.T, base string, logs *strings.Builder) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("the binary never answered /healthz in 30s\nlogs:\n%s", logs.String())
}
