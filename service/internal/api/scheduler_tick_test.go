// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/onsi/gomega"

	"github.com/simple-container-com/sc-component-notifier/service/internal/events"
)

type stubDrainer struct {
	res    events.DrainResult
	err    error
	called int
}

func (s *stubDrainer) Drain(context.Context) (events.DrainResult, error) {
	s.called++
	return s.res, s.err
}

func tick(t *testing.T, drainer Draining, op string) *httptest.ResponseRecorder {
	t.Helper()
	return tickRep(t, drainer, nil, op)
}

func tickRep(t *testing.T, drainer Draining, rep func(context.Context) error, op string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/scheduler/delivery/tick",
		JWTMiddleware([]byte(testKey))(DeliveryTick(drainer, rep)))

	req := httptest.NewRequest("POST", "/api/v1/scheduler/delivery/tick", nil)
	req.Header.Set("Authorization", "Bearer "+mintOpJWT(t, op, ""))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

func TestDeliveryTick_RunsTheDrain(t *testing.T) {
	RegisterTestingT(t)
	d := &stubDrainer{res: events.DrainResult{Examined: 3, Delivered: 2, Failed: 1}}

	rr := tick(t, d, "scheduler")

	Expect(rr.Code).To(Equal(http.StatusOK))
	Expect(d.called).To(Equal(1))

	var res events.DrainResult
	Expect(json.Unmarshal(rr.Body.Bytes(), &res)).To(Succeed())
	Expect(res.Examined).To(Equal(3))
	Expect(res.Delivered).To(Equal(2))
	Expect(res.Failed).To(Equal(1))
}

// The drain is a separate op from "event" and "register". A publisher
// holding a valid ingest token must not be able to drive the delivery
// loop — overlapping drains are the duplicate-delivery vector this
// component exists to avoid, and a caller who can fire ticks at will is
// the cheapest way to produce them.
func TestDeliveryTick_RequiresSchedulerOp(t *testing.T) {
	RegisterTestingT(t)

	for _, op := range []string{"event", "register", "", "admin"} {
		d := &stubDrainer{}
		rr := tick(t, d, op)
		Expect(rr.Code).To(Equal(http.StatusForbidden), "op %q was accepted", op)
		Expect(d.called).To(BeZero(), "op %q reached the drain", op)
	}
}

// A failing drain must be a 500 so it lands in the schedule's own error
// metric. A 200 here is the worst available outcome: a notifier that
// has stopped delivering while every dashboard reads green.
func TestDeliveryTick_DrainErrorIs500(t *testing.T) {
	RegisterTestingT(t)
	d := &stubDrainer{err: errors.New("storage unavailable")}

	rr := tick(t, d, "scheduler")

	Expect(rr.Code).To(Equal(http.StatusInternalServerError))
	Expect(rr.Body.String()).To(ContainSubstring("storage unavailable"))
}

// A held lease is the normal answer, not a fault — ticks fire every
// minute and a slow drain will still be running. It must be a 200 with
// `skipped: true` so an operator can tell it apart from an error, and
// so the schedule's error metric stays meaningful.
func TestDeliveryTick_SkippedIsSuccess(t *testing.T) {
	RegisterTestingT(t)
	d := &stubDrainer{res: events.DrainResult{Skipped: true}}

	rr := tick(t, d, "scheduler")

	Expect(rr.Code).To(Equal(http.StatusOK))

	var res events.DrainResult
	Expect(json.Unmarshal(rr.Body.Bytes(), &res)).To(Succeed())
	Expect(res.Skipped).To(BeTrue())
}

// An unauthenticated tick must not reach the drain. The endpoint is
// reachable from the public internet — it is driven by an EventBridge
// schedule over HTTPS, not from inside a VPC.
func TestDeliveryTick_RejectsMissingToken(t *testing.T) {
	RegisterTestingT(t)
	d := &stubDrainer{}

	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/scheduler/delivery/tick",
		JWTMiddleware([]byte(testKey))(DeliveryTick(d, nil)))

	req := httptest.NewRequest("POST", "/api/v1/scheduler/delivery/tick", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	Expect(rr.Code).To(Equal(http.StatusUnauthorized))
	Expect(d.called).To(BeZero())
}

// TestDeliveryTick_DrivesReplicationTick is the point of the wiring: this
// endpoint is already the notifier's clock (there is no ticker in this
// service and must never be one), and since 2026-11-25 it is also
// go-embedb's. The engine starts five wall-clock-ticker goroutines at
// handler construction and Lambda freezes them when the response flushes,
// so a loop fires only if one invocation outlives its period.
//
// "Called once" is not the whole assertion — the call must be BOUNDED, or
// a snapshot plus a GC sweep on a cold bucket starves the drain that this
// endpoint exists for.
func TestDeliveryTick_DrivesReplicationTick(t *testing.T) {
	RegisterTestingT(t)
	d := &stubDrainer{res: events.DrainResult{Examined: 1}}

	calls := 0
	hadDeadline := false
	rr := tickRep(t, d, func(ctx context.Context) error {
		calls++
		_, hadDeadline = ctx.Deadline()
		return nil
	}, "scheduler")

	Expect(rr.Code).To(Equal(http.StatusOK))
	Expect(calls).To(Equal(1), "replication tick must run exactly once per delivery tick")
	Expect(hadDeadline).To(BeTrue(), "the replication tick must be bounded so it cannot consume the whole invocation and starve the drain")
	Expect(d.called).To(Equal(1), "the drain must still run after the replication tick")
}

// A replication failure must not stop DELIVERY. It is retried on the next
// tick (one minute later), whereas 500-ing here would halt the thing this
// endpoint exists for over a transient S3 error — and the drain result is
// what the schedule's error metric is supposed to mean.
func TestDeliveryTick_ReplicationFailureIsNotFatal(t *testing.T) {
	RegisterTestingT(t)
	d := &stubDrainer{res: events.DrainResult{Examined: 2, Delivered: 2}}

	rr := tickRep(t, d, func(context.Context) error {
		return errors.New("put body: operation error S3: PutObject, StatusCode: 0")
	}, "scheduler")

	Expect(rr.Code).To(Equal(http.StatusOK))
	Expect(d.called).To(Equal(1), "a replication failure must not skip the drain")

	var res events.DrainResult
	Expect(json.Unmarshal(rr.Body.Bytes(), &res)).To(Succeed())
	Expect(res.Delivered).To(Equal(2))
}

// The replication tick sits BEHIND the auth gate. It issues S3 writes, so
// an unauthenticated caller who could drive it would have a free amplifier
// — the same reasoning that keeps the drain itself behind the gate.
func TestDeliveryTick_ReplicationNotDrivenByAWrongOp(t *testing.T) {
	RegisterTestingT(t)

	for _, op := range []string{"event", "register", "", "admin"} {
		d := &stubDrainer{}
		calls := 0
		rr := tickRep(t, d, func(context.Context) error {
			calls++
			return nil
		}, op)

		Expect(rr.Code).To(Equal(http.StatusForbidden), "op %q was accepted", op)
		Expect(calls).To(BeZero(), "op %q drove the replication tick", op)
	}
}

// Nil is the steady state on any deploy not using the embed (S3 or
// filesystem storage), and must stay a no-op rather than a nil-call panic.
func TestDeliveryTick_NilReplicationTickIsSkipped(t *testing.T) {
	RegisterTestingT(t)
	d := &stubDrainer{res: events.DrainResult{Examined: 1}}

	rr := tickRep(t, d, nil, "scheduler")

	Expect(rr.Code).To(Equal(http.StatusOK))
	Expect(d.called).To(Equal(1))
}
