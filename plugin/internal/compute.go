// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package notifier

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/pkg/errors"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"

	"github.com/simple-container-com/api/pkg/api"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
	scPlugin "github.com/simple-container-com/api/pkg/plugin"
)

// Retry policy constants — per sc-components-platform design doc v1.3 §P3
// push-model operational contract. DO NOT lower the attempt count or the
// timeout without updating the design doc and reasoning about how
// consumer deploys behave when the Backup service is cold / rolling /
// 5xx-ing.
const (
	maxAttempts        = 3
	attemptTimeout     = 10 * time.Second
	registerJWTTTL     = 60 * time.Second
	userAgent          = "sc-component-notifier/0.0.1 (+https://github.com/simple-container-com/sc-component-notifier)"
	notifyURLVar       = "NOTIFY_URL"   // injected into the publisher's env (plain)
	notifyTokenVar     = "NOTIFY_TOKEN" // injected into the publisher's env (secret)
	tenantsRegisterURL = "/api/v1/tenants/register"
)

// Backoff schedule, in seconds. Jitter ±25% applied per attempt.
var backoffSeconds = []int{1, 2, 4}

// ProcessorConfig carries per-plugin wiring into the ComputeProcessor fn.
// Progress is required; HTTPClient defaults to a sensible one with
// attemptTimeout per attempt (not the whole chain).
type ProcessorConfig struct {
	Progress   *scPlugin.Publisher
	HTTPClient *http.Client // nil → use default
}

// Processor is the ComputeProcessor for a `uses: [my-notifier]` consumer.
type Processor struct {
	cfg ProcessorConfig
}

func NewProcessor(cfg ProcessorConfig) *Processor {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: attemptTimeout}
	}
	return &Processor{cfg: cfg}
}

// Process is the push-register call.
//
//   - Reads the deployed service URL from the dependency outputs (the
//     `_deployed_service` key SC core's sidecar wrapper populates in
//     slice 1a; or a `<my-notifier>-service-url` export from Provision).
//   - Mints a short-TTL HS256 JWT with the consumer stack as `tenant`.
//   - POSTs a tenant record to {service-url}/api/v1/tenants/register,
//     with the retry policy from design doc §P3.
//   - Injects NOTIFY_URL + NOTIFY_TOKEN env into the consumer.
//
// The signing key is read from the plugin's own Provision inputs
// (`signingKey`); for the notifier's purposes we expect the test harness to
// pass it via the DependencyOutputsJson or an env var in the plugin
// binary's process.
func (p *Processor) Process(ctx *sdk.Context, _ api.Stack, input api.ResourceInput, collector pApi.ComputeContextCollector, params pApi.ProvisionParams) (*api.ResourceOutput, error) {
	resName := input.Descriptor.Name
	resType := input.Descriptor.Type
	stackName := input.StackParams.StackName

	serviceURL, err := p.readServiceURL(input, params)
	if err != nil {
		return nil, errors.Wrapf(err, "component %q unable to resolve service URL", resName)
	}
	signingKey, err := p.readSigningKey(input)
	if err != nil {
		return nil, errors.Wrapf(err, "component %q unable to resolve signingKey", resName)
	}

	registerToken, err := mintRegisterJWT(signingKey, stackName, registerJWTTTL)
	if err != nil {
		return nil, errors.Wrap(err, "mint the registration JWT")
	}
	eventToken, err := mintEventJWT(signingKey, stackName)
	if err != nil {
		return nil, errors.Wrap(err, "mint the publisher's event JWT")
	}

	record := TenantRecord{
		StackName: stackName,
		Version:   deriveConsumerVersion(input),
		Resources: readConsumerResources(input),
	}

	if err := p.pushRegister(ctx.Context(), serviceURL, registerToken, record); err != nil {
		// The consumer's deploy fails, with a diagnosable error naming
		// the component as the blocker (per §P3 non-silent-skip rule).
		return nil, errors.Wrapf(err,
			"component %q unreachable after %d attempts (check the component service is live at %s; or remove the component from client.yaml uses: if intentionally torn down)",
			resName, maxAttempts, serviceURL)
	}

	// On success, inject the publisher's runtime wiring: where to send
	// (plain) and what to send it with (secret).
	//
	// NOTE the two tokens are NOT the same credential. The registration
	// token above lives for 60 seconds and is spent by this plugin
	// within one of them. The token injected here is the one the
	// PUBLISHER'S OWN CODE presents on every POST /api/v1/events for as
	// long as that deploy is live — see mintEventJWT for why it carries
	// no expiry.
	collector.AddEnvVariableIfNotExist(notifyURLVar, serviceURL, resType, resName, stackName)
	collector.AddSecretEnvVariableIfNotExist(notifyTokenVar, eventToken, resType, resName, stackName)

	if p.cfg.Progress != nil {
		p.cfg.Progress.Info("compute-process", fmt.Sprintf("registered tenant %q at %s", stackName, serviceURL))
	}

	return &api.ResourceOutput{Ref: sdk.String(fmt.Sprintf("notifier-compute:%s:%s", resType, resName)).ToStringOutput()}, nil
}

// pushRegister executes the retry loop. Returns nil on first successful
// attempt (2xx); wrapped error on all-retries-exhausted. Each attempt uses
// its own context.WithTimeout so the overall wall time is bounded.
func (p *Processor) pushRegister(ctx context.Context, serviceURL, token string, record TenantRecord) error {
	body, err := json.Marshal(record)
	if err != nil {
		return errors.Wrap(err, "marshal tenant record")
	}

	target := strings.TrimRight(serviceURL, "/") + tenantsRegisterURL
	var lastErr error
	var lastStatus int
	var lastBody string

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			delay := jitteredBackoff(backoffSeconds[attempt-2])
			if p.cfg.Progress != nil {
				p.cfg.Progress.Info("compute-process", fmt.Sprintf("register attempt %d/%d in %s", attempt, maxAttempts, delay))
			}
			select {
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "retry loop cancelled")
			case <-time.After(delay):
			}
		}

		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		status, respBody, err := doPost(attemptCtx, p.cfg.HTTPClient, target, token, body)
		cancel()

		if err == nil && status >= 200 && status < 300 {
			if p.cfg.Progress != nil {
				p.cfg.Progress.Info("compute-process", fmt.Sprintf("register attempt %d/%d status=%d (ok)", attempt, maxAttempts, status))
			}
			return nil
		}
		lastStatus = status
		lastBody = truncate(respBody, 256)
		if err != nil {
			lastErr = err
		} else {
			lastErr = errors.Errorf("http %d: %s", status, lastBody)
		}
		if p.cfg.Progress != nil {
			p.cfg.Progress.Error("compute-process", fmt.Sprintf("register attempt %d/%d status=%d err=%v", attempt, maxAttempts, status, lastErr))
		}

		// Non-retryable: 4xx except 429. See design doc §P3.
		if status >= 400 && status < 500 && status != 429 {
			return errors.Wrapf(lastErr, "non-retryable http %d from %s", status, target)
		}
	}

	return errors.Wrapf(lastErr, "all %d attempts failed (last status=%d, last body=%q)", maxAttempts, lastStatus, lastBody)
}

func doPost(ctx context.Context, client *http.Client, url, token string, body []byte) (int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(respBody), nil
}

// jitteredBackoff returns `base ± 25%` as a time.Duration.
func jitteredBackoff(baseSeconds int) time.Duration {
	// jitter in [-250ms, +250ms] per second of base, i.e. ±25%.
	base := time.Duration(baseSeconds) * time.Second
	jitterRange := base / 2 // ±25% = range of 50%
	// crypto/rand.Int PANICS on a non-positive bound, so a zero base must not
	// reach it. A zero base means "retry immediately", which has no jitter to
	// apply. Tests zero backoffSeconds out to keep the retry loop instant, and
	// before this guard that panicked inside the plugin's ComputeProcess — i.e.
	// mid-Pulumi-provision, had any real config ever used a zero delay.
	if jitterRange <= 0 {
		return base
	}
	jitter, err := rand.Int(rand.Reader, big.NewInt(int64(jitterRange)))
	if err != nil {
		return base
	}
	return base - (jitterRange / 2) + time.Duration(jitter.Int64())
}

func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "…"
}

// deriveConsumerVersion picks a stable version string for idempotency.
// In production, this is the consumer's deployed image version;
// pre-slice-1a-wired, we use the resource name as the version marker.
func deriveConsumerVersion(input api.ResourceInput) string {
	return input.Descriptor.Name
}

// readConsumerResources enumerates the consumer's sibling resources
// relevant to a real component (DBs, S3, etc.). For the notifier, we just
// return an empty list — real components (Backup, Monitoring, …) will
// populate this from DependencyOutputsJson.
func readConsumerResources(_ api.ResourceInput) []TenantResource {
	return []TenantResource{}
}

// mintRegisterJWT creates the short-TTL HS256 token that authorises the
// push-register call below. Signing key is the operator's
// `config.signingKey` secret value, the same key the service was
// deployed with (as SERVICE_JWT_SECRET env).
//
// 60 seconds is generous: this plugin spends the token itself, in the
// next few hundred milliseconds, and the retry loop's whole budget is
// bounded well inside it.
func mintRegisterJWT(signingKey, tenant string, lifetime time.Duration) (string, error) {
	now := time.Now().UTC()
	claims := jwt.MapClaims{
		"op":     "register",
		"tenant": tenant,
		"iat":    now.Unix(),
		"exp":    now.Add(lifetime).Unix(),
		"nbf":    now.Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return tok.SignedString([]byte(signingKey))
}

// mintEventJWT creates the token the PUBLISHER keeps, and presents on
// every POST /api/v1/events.
//
// It carries NO `exp`, deliberately, and that is the one place this
// component departs from "every token is short-lived".
//
// The token is minted at the publisher's DEPLOY time and injected into
// its environment. Nothing re-mints it afterwards — there is no refresh
// endpoint and no sidecar — so any finite expiry becomes the date on
// which that publisher silently stops being able to alert anyone. A
// service that was deployed once and has run happily since is the
// NORMAL case in this fleet, and it is also exactly the case where an
// alert matters most. "Your alerting went quiet eleven months after the
// last deploy" is the single worst failure this component could ship.
//
// What makes it acceptable is that the token is narrow, not that it is
// short-lived:
//
//   - `op: "event"` is checked by the ingest handler and NOTHING else
//     accepts it. It cannot register a publisher, cannot drive the
//     delivery drain, cannot read or write channels.
//   - `tenant` is the publisher's own stack name, and the ingest
//     handler takes the event's `source` from this claim rather than
//     from the request body — so holding one publisher's token lets you
//     forge events as that publisher and no other, which is also what
//     a channel's `sources:` allowlist is checked against.
//   - Rotating `signingKey` invalidates every one of these at once,
//     which is the revocation story for all five token shapes here.
//
// The exposure, stated plainly: whoever can read the publisher's
// environment can post events as that publisher until the key rotates.
// They cannot read anything, and the blast radius is noise on a channel
// an operator is already watching.
func mintEventJWT(signingKey, tenant string) (string, error) {
	now := time.Now().UTC()
	claims := jwt.MapClaims{
		"op":     "event",
		"tenant": tenant,
		"iat":    now.Unix(),
		"nbf":    now.Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return tok.SignedString([]byte(signingKey))
}

// TenantRecord is the request body shape for POST /api/v1/tenants/register.
type TenantRecord struct {
	StackName string           `json:"stackName"`
	Version   string           `json:"version"`
	Resources []TenantResource `json:"resources"`
}

// TenantResource describes one sibling resource the consumer has
// (DB, S3, etc.). Notifier v1 has no resources; real components populate
// from DependencyOutputsJson via the §P3 canonical `connection.*` family.
type TenantResource struct {
	Type       string            `json:"type"`
	Name       string            `json:"name"`
	Connection map[string]string `json:"connection,omitempty"`
}

// The pre-slice-1a `ReadDeployedServiceURL(depOutputsJSON []byte)`
// helper that used to live here is gone. It parsed the raw
// DependencyOutputsJson looking for `_deployed_service`, which was the
// scaffold's placeholder for a URL the plugin had no other way to
// learn. readServiceURL in config.go supersedes it: SC core hands the
// decoded sibling outputs to the processor through
// ProvisionParams.ResourceOutputs, and the `ingress.hostname` fallback
// covers the consumer-side ComputeProcess path, where core populates no
// dependency outputs at all.
