// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// This file holds the one test that would have caught the first deploy's
// failure, and it is deliberately heavier than everything else in the
// repo: it builds the real binary and runs it against a stub Lambda
// Runtime API.
//
// The defect it guards (live, 2026-11-23): main() served net/http on
// :8080 and blocked on ctx.Done(). Nothing joined the Runtime API, so
// every invoke ended in `platform.initReport status:timeout` after the
// full 30s and the Function URL answered 502 — while the boot log read
// perfectly ("channel ready", "listening addr=:8080"). No handler-level
// test can see this: every handler was correct and every unit test was
// green. What was broken is whether anything ever hands a request TO a
// handler, and the only honest way to assert that is to make the binary
// answer one.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// invocationResponse is what the stub captures off
// POST /2018-06-01/runtime/invocation/{id}/response.
type invocationResponse struct {
	StatusCode int               `json:"statusCode"`
	Body       string            `json:"body"`
	Headers    map[string]string `json:"headers"`
}

func TestBinaryAnswersAFunctionURLInvocation(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary; skipped under -short")
	}

	bin := filepath.Join(t.TempDir(), "notifier")
	build := exec.Command("go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	// The event the its-felix adapter expects for a function-url
	// routing type: an API Gateway v2 / Function URL request. Minimal
	// but complete enough that the adapter can synthesise an
	// *http.Request for GET /healthz.
	event := map[string]any{
		"version":         "2.0",
		"rawPath":         "/healthz",
		"rawQueryString":  "",
		"headers":         map[string]string{"host": "example.invalid"},
		"isBase64Encoded": false,
		"requestContext": map[string]any{
			"http": map[string]any{
				"method":    http.MethodGet,
				"path":      "/healthz",
				"protocol":  "HTTP/1.1",
				"sourceIp":  "127.0.0.1",
				"userAgent": "runtime-api-stub",
			},
			"requestId": "stub-request-1",
			"timeEpoch": time.Now().UnixMilli(),
		},
	}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	var (
		served    atomic.Int32
		responses = make(chan invocationResponse, 1)
		failures  = make(chan string, 1)
	)

	// A stub Runtime API: the three endpoints aws-lambda-go talks to.
	// It hands out exactly ONE invocation and then blocks the /next
	// long-poll, which is what the real API does between invokes.
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/invocation/next"):
			if served.Add(1) > 1 {
				// Long-poll, as the real Runtime API does. The test is
				// over by now; holding here keeps the binary from
				// spinning on an empty response.
				<-r.Context().Done()
				return
			}
			w.Header().Set("Lambda-Runtime-Aws-Request-Id", "stub-request-1")
			w.Header().Set("Lambda-Runtime-Deadline-Ms",
				fmt.Sprint(time.Now().Add(30*time.Second).UnixMilli()))
			w.Header().Set("Lambda-Runtime-Invoked-Function-Arn",
				"arn:aws:lambda:eu-central-1:000000000000:function:notifier-test")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(eventJSON)

		case strings.HasSuffix(r.URL.Path, "/response"):
			var got invocationResponse
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				select {
				case failures <- "undecodable invocation response: " + err.Error():
				default:
				}
			} else {
				select {
				case responses <- got:
				default:
				}
			}
			w.WriteHeader(http.StatusAccepted)

		case strings.Contains(r.URL.Path, "/error"):
			body := make([]byte, 2048)
			n, _ := r.Body.Read(body)
			select {
			case failures <- "runtime reported an error: " + string(body[:n]):
			default:
			}
			w.WriteHeader(http.StatusAccepted)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer stub.Close()

	cmd := exec.Command(bin)
	cmd.Env = append(
		os.Environ(),
		// AWS_LAMBDA_RUNTIME_API is host:port with NO scheme — that is
		// the contract aws-lambda-go reads, and a URL here fails with a
		// dial error that looks nothing like the real cause.
		"AWS_LAMBDA_RUNTIME_API="+strings.TrimPrefix(stub.URL, "http://"),
		"AWS_LAMBDA_FUNCTION_NAME=notifier-test",
		"SIMPLE_CONTAINER_AWS_LAMBDA_ROUTING_TYPE=function-url",
		// Explicitly NOT LOCAL_DEBUG=true: this test exists to exercise
		// the lambda.Start branch, which is the one that was broken.
		"LOCAL_DEBUG=",
		"SERVICE_JWT_SECRET=0123456789abcdef0123456789abcdef",
		"STORAGE_TYPE=filesystem",
		"STORAGE_PATH="+t.TempDir(),
		"NOTIFIER_CHANNELS=",
		"PORT=0",
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

	select {
	case got := <-responses:
		if got.StatusCode != http.StatusOK {
			t.Fatalf("GET /healthz through the Runtime API returned %d, want 200\nlogs:\n%s",
				got.StatusCode, logs.String())
		}
		if !strings.Contains(got.Body, "ok") {
			t.Fatalf("body %q does not contain \"ok\"\nlogs:\n%s", got.Body, logs.String())
		}
	case why := <-failures:
		t.Fatalf("%s\nlogs:\n%s", why, logs.String())
	case <-time.After(45 * time.Second):
		// This is the shape the live failure takes. The message names
		// the cause rather than the symptom, because "timed out" on its
		// own sent the first investigation to the Function URL, the
		// Cloudflare worker and the DNS record before the entrypoint.
		t.Fatalf("the binary never answered an invocation in 45s — nothing registered with the "+
			"Lambda Runtime API. On Lambda this presents as initReport status:timeout and a 502, "+
			"with a clean-looking boot log. Check that main() starts go-aws-lambda-sdk's "+
			"service.Start() rather than http.Server.ListenAndServe.\nlogs:\n%s", logs.String())
	}
}
