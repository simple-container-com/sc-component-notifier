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
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/scheduler/delivery/tick",
		JWTMiddleware([]byte(testKey))(DeliveryTick(drainer)))

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
		JWTMiddleware([]byte(testKey))(DeliveryTick(d)))

	req := httptest.NewRequest("POST", "/api/v1/scheduler/delivery/tick", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	Expect(rr.Code).To(Equal(http.StatusUnauthorized))
	Expect(d.called).To(BeZero())
}
