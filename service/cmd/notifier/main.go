// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// Command notifier is the microservice half of sc-component-notifier: it
// ingests events from sibling components, routes them against the
// configured channels, and delivers them.
//
// Environment:
//
//	SERVICE_JWT_SECRET               HS256 key, >= 32 bytes. Operator-set via
//	                                 the plugin's config.signingKey. Also the
//	                                 KEK for sealing SPA-written credentials.
//	NOTIFIER_OPERATOR_PASSWORD       shared SPA login secret. Optional: unset
//	                                 means POST /api/v1/login answers 403 and
//	                                 the dashboard reads nothing. The boot log
//	                                 says which, because an unset password is
//	                                 otherwise indistinguishable from a typo.
//	NOTIFIER_CHANNELS                JSON array of managed channel configs
//	NOTIFIER_SELF_URL                this service's own public URL, for logs
//	NOTIFIER_COOLDOWN                per-(source,subject,kind) quiet period
//	                                 as a Go duration; "0" disables
//	STORAGE_TYPE                     s3 | filesystem        (default s3)
//	STORAGE_BUCKET                   bucket for s3
//	STORAGE_REGION                   region for s3          (default eu-central-1)
//	STORAGE_PREFIX                   optional key namespace
//	STORAGE_ENDPOINT_URL             S3-compatible gateway   (optional)
//	STORAGE_USE_PATH_STYLE           non-empty => path-style (optional)
//	STORAGE_PATH                     mount point for filesystem
//	STORAGE_ACCESS_KEY / _SECRET_KEY optional; prefer the execution role
//	USE_GOEMBEDB                     "1" to serve the live store from go-embedb
//	                                 (ferretdb + sqlite + S3 replication) instead
//	                                 of S3Store. DEFAULT OFF — step 1 of the
//	                                 go-embedb adoption just ships the code path
//	                                 behind this flag so a deploy is bit-identical
//	                                 to pre-adoption until the operator flips it.
//	                                 See README.md §Storage + the
//	                                 sc-component-notifier-goembedb-migration
//	                                 roadmap item.
//	GOEMBEDB_S3_BUCKET               replication substrate bucket. HARD-REQUIRED
//	                                 on Lambda: with USE_GOEMBEDB=1 and no bucket
//	                                 the service REFUSES TO BOOT
//	                                 ("GOEMBEDB_S3_BUCKET is required on Lambda
//	                                 …"), because `/tmp` is per-container and
//	                                 ephemeral, which defeats the single-flight
//	                                 tick lease and idempotency expiry. Optional
//	                                 only off Lambda (ECS / VM / local smoke),
//	                                 where it warns and runs single-node.
//	GOEMBEDB_S3_PREFIX               key namespace under the replication bucket
//	                                 (default sc-component-notifier/).
//	GOEMBEDB_S3_REGION               region for the replication bucket (default
//	                                 $AWS_REGION).
//	GOEMBEDB_SQLITE_DIR              local sqlite dir (default /tmp/goembedb).
//	GOEMBEDB_REPLSET_NAME            replica-set name (default notifier).
//	GOEMBEDB_MAX_REPLICAS            cap on slot-table entries.
//	GOEMBEDB_HEARTBEAT_TTL           slot heartbeat (default 3m on Lambda).
//	GOEMBEDB_ACK_WAIT_LIVE_ONLY      truthy => ack ignores dead replicas (Bug 13
//	                                 fix; auto-on when AWS_LAMBDA_FUNCTION_NAME
//	                                 is set).
//	ADMIN_MIGRATION_TOKEN            opt-in bearer that gates
//	                                 POST /admin/migrate-s3-to-goembedb; empty
//	                                 disables the endpoint. Bound from the
//	                                 component's `migrationToken:` through the
//	                                 SC secret-env wire, so it arrives as a
//	                                 SecretsManager ARN and is resolved below.
//	PORT                             listen port            (default 8080)
//	LOCAL_DEBUG                      "true" => serve ordinary HTTP on $PORT.
//	                                 Anything else (including unset) means the
//	                                 SDK calls lambda.Start and waits for the
//	                                 Runtime API — so this must be set to run
//	                                 the binary outside Lambda.
//
// Any of the secret-bearing values may arrive as a SecretsManager ARN
// rather than a literal — see secret_resolver.go for why that is not
// optional to handle.
//
// There is NO Mongo and no MONGO_URI. The scaffold this repo was copied
// from had one; a notifier's volume does not justify a paid cluster, and
// `storage: <resource-name>` stays the operator's only storage knob.
//
// There is also NO ticker. The delivery drain runs inline on
// POST /api/v1/scheduler/delivery/tick, driven by an external schedule,
// because a Lambda freezes background goroutines the instant the handler
// returns — see internal/api/scheduler_tick.go.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	sdkservice "github.com/simple-container-com/go-aws-lambda-sdk/pkg/service"

	"github.com/simple-container-com/sc-component-notifier/service/internal/api"
	"github.com/simple-container-com/sc-component-notifier/service/internal/crypto"
	"github.com/simple-container-com/sc-component-notifier/service/internal/events"
	"github.com/simple-container-com/sc-component-notifier/service/internal/goembedbadmin"
	"github.com/simple-container-com/sc-component-notifier/service/internal/registry"
	"github.com/simple-container-com/sc-component-notifier/service/internal/storage"
	"github.com/simple-container-com/sc-component-notifier/service/internal/tenant"
	"github.com/simple-container-com/sc-component-notifier/service/internal/ui"
)

// Set at build time via ldflags: -X main.version=... etc.
//
// SC always injects VERSION as a --build-arg, so a deployed binary
// reporting "dev" means the Dockerfile lost its ARG, not that this is
// the normal case.
var (
	version  = "dev"
	commit   = "dev"
	revision = "dev"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	jwtSecret, err := resolveAWSSecretARN(ctx, "SERVICE_JWT_SECRET", os.Getenv("SERVICE_JWT_SECRET"))
	if err != nil {
		slog.Error("could not resolve SERVICE_JWT_SECRET", "err", err)
		os.Exit(2)
	}
	if len(jwtSecret) < crypto.MinKEKBytes {
		// Exit 2 rather than warn. Below 32 bytes the JWT validator
		// rejects every token, so the service would answer 401 to every
		// publisher while looking healthy on /healthz.
		slog.Error("SERVICE_JWT_SECRET must be at least 32 bytes",
			"floor", crypto.MinKEKBytes, "got", len(jwtSecret))
		os.Exit(2)
	}

	// The SPA's only credential. Optional by design — a deployment with
	// no password set is read-nothing rather than read-everything — but
	// "optional" used to mean "resolved and then read by nothing": the
	// first deploy shipped a dashboard that asked the operator to paste a
	// hand-minted JWT into localStorage, which is both unusable and the
	// wrong secret to hand a browser (SERVICE_JWT_SECRET is also the KEK
	// sealing every tenant's channel credentials). GET /api/v1/session
	// reports whether this is set, so the SPA can tell a typo apart from
	// a deployment that never configured a password.
	operatorPassword, err := resolveAWSSecretARN(ctx, "NOTIFIER_OPERATOR_PASSWORD", os.Getenv("NOTIFIER_OPERATOR_PASSWORD"))
	if err != nil {
		slog.Error("could not resolve NOTIFIER_OPERATOR_PASSWORD", "err", err)
		os.Exit(2)
	}

	sealer, err := crypto.NewSealer([]byte(jwtSecret))
	if err != nil {
		slog.Error("credential sealer unavailable", "err", err)
		os.Exit(2)
	}

	// Storage backend selection:
	//
	//   USE_GOEMBEDB unset or "0" (DEFAULT): S3Store. Current production.
	//   USE_GOEMBEDB=1                     : MongoStore on top of an
	//                                        embedded go-embedb (ferretdb +
	//                                        sqlite + S3 replication).
	//
	// migrationSource stays non-nil whenever we built an S3Store, so the
	// `/admin/migrate-s3-to-goembedb` endpoint has a source regardless of
	// which backend serves the live traffic. See forge/docs/roadmap/items/
	// sc-component-notifier-goembedb-migration.md.
	var (
		objects         storage.Store
		target          string
		migrationSource storage.Store
		migrationSink   *storage.MongoStore

		// replicationTick drives go-embedb's five background loops from
		// the delivery tick. Nil unless the embed is in use — see
		// api.DeliveryTick for why a ticker goroutine cannot do this
		// job on Lambda. (It is the same reason this service has no
		// ticker of its own.)
		replicationTick func(context.Context) error
	)
	useGoEmbedb := strings.TrimSpace(os.Getenv("USE_GOEMBEDB")) == "1"

	// Always build the S3 source when STORAGE_TYPE=s3 (default), both
	// as the live backend and as the migration source. A deploy with
	// USE_GOEMBEDB=1 but no S3 wiring is still valid — the migration
	// just has no source to pull from.
	if envOr("STORAGE_TYPE", "s3") == "s3" {
		s3Store, loc, serr := buildObjectStore(ctx)
		if serr != nil && !useGoEmbedb {
			slog.Error("object storage unavailable", "err", serr)
			os.Exit(2)
		}
		if serr == nil {
			// buildObjectStore returns storage.Store; cast back to the
			// S3Store concrete type so the migration source carries the
			// If-None-Match ConditionalPutter path. Any non-S3 shape
			// returned here is a programming error, not a runtime one.
			migrationSource = s3Store
			objects = s3Store
			target = loc
		}
	} else if !useGoEmbedb {
		// Non-S3 storage types (filesystem) go through the regular builder.
		st, loc, serr := buildObjectStore(ctx)
		if serr != nil {
			slog.Error("object storage unavailable", "err", serr)
			os.Exit(2)
		}
		objects = st
		target = loc
	}

	if useGoEmbedb {
		embed, berr := bootGoEmbed(slog.Default())
		if berr != nil {
			slog.Error("goembedb boot failed", "err", berr)
			os.Exit(2)
		}
		mongoStore := storage.NewMongoStore(embed.Client)
		// Register the TTL + unique indexes before the store serves any
		// request — a cold Lambda hitting events/ or cooldown/ before
		// the TTL sweep is wired would still work (writes succeed) but
		// records would never age out, which is exactly the failure
		// mode TestMongoStore_TTLHonoured exists to catch.
		ictx, icancel := context.WithTimeout(ctx, 30*time.Second)
		if err := mongoStore.EnsureIndexes(ictx); err != nil {
			icancel()
			slog.Error("goembedb ensure indexes failed", "err", err)
			os.Exit(2)
		}
		icancel()
		migrationSink = mongoStore
		objects = mongoStore
		target = "goembedb://" + embed.ReplSet
		replicationTick = embed.FDB.TickNow
	}

	if objects == nil {
		slog.Error("no storage backend configured (USE_GOEMBEDB unset and buildObjectStore returned nil)")
		os.Exit(2)
	}
	goembedbadmin.SetStoreBackend(backendLabel(useGoEmbedb))
	prefix := os.Getenv("STORAGE_PREFIX")

	managed, err := loadManagedChannels(ctx)
	if err != nil {
		// A channel declared in server.yaml that cannot be parsed is
		// fatal, not skippable: the operator believes they configured
		// an alerting path. Booting without it would mean the one
		// failure mode this component exists to prevent — silence.
		slog.Error("NOTIFIER_CHANNELS is invalid", "err", err)
		os.Exit(2)
	}

	eventStore := events.NewBlobStore(objects, prefix)
	tenantStore := tenant.NewBlobStore(objects, prefix)

	reg := registry.New(registry.Config{
		Store:   eventStore,
		Sealer:  sealer,
		Managed: managed,
		Log:     slogLogger{},
	})
	if err := reg.Seed(ctx); err != nil {
		slog.Error("could not seed declarative channels", "err", err)
		os.Exit(1)
	}

	cooldown, err := resolveCooldown()
	if err != nil {
		slog.Error("NOTIFIER_COOLDOWN is invalid", "err", err)
		os.Exit(2)
	}

	router := events.NewRouter(eventStore, cooldown)
	drainer := events.NewDrainer(events.DrainerConfig{
		Store:    eventStore,
		Channels: reg.Channels,
		Log:      slogLogger{},
	})

	slog.Info("starting sc-component-notifier",
		"version", version, "commit", commit, "revision", revision,
		"port", envOr("PORT", "8080"), "storage", target,
		"backend", backendLabel(useGoEmbedb),
		"managed_channels", len(managed), "cooldown", cooldown.String(),
		"self_url", os.Getenv("NOTIFIER_SELF_URL"),
		// Not the value — just whether the dashboard can be logged into
		// at all. An operator staring at a 403 should find the answer
		// here rather than by guessing at the secret registry.
		"operator_login", operatorPassword != "")

	// Report the channels at boot, usable or not. An operator reading
	// the cold-start log should be able to see that a channel exists and
	// why it cannot send, without opening the SPA.
	if statuses, err := reg.Statuses(ctx); err != nil {
		slog.Error("could not describe channels", "err", err)
	} else {
		for _, st := range statuses {
			if st.Usable {
				slog.Info("channel ready", "name", st.Name, "type", st.Type,
					"managed", st.Managed, "minSeverity", st.MinSeverity)
				continue
			}
			slog.Error("channel NOT usable", "name", st.Name, "type", st.Type,
				"managed", st.Managed, "reason", st.Reason)
		}
		if len(statuses) == 0 {
			slog.Warn("no channels configured — events will be ingested and routed nowhere")
		}
	}

	mux := chi.NewRouter()
	mux.Use(middleware.RequestID)
	mux.Use(middleware.RealIP)
	mux.Use(middleware.Recoverer)
	mux.Use(requestLogger)

	// The build stamp goes on /healthz because that is the only probe a
	// post-deploy smoke can reach with no credentials at all — see
	// api.Healthz for the pin bump that was unverifiable without it.
	mux.Get("/healthz", api.Healthz(version))

	// Prometheus text-exposition. Unauthenticated: exposes process-level
	// counters + the notifier_store_backend{backend="s3"|"goembedb"}
	// gauge an operator alerts on after flipping USE_GOEMBEDB.
	mux.Method(http.MethodGet, "/metrics", goembedbadmin.MetricsHandler())

	// Migration endpoint. Disabled unless ADMIN_MIGRATION_TOKEN is set —
	// the endpoint is a one-off operator tool for the S3 → go-embedb
	// cutover, NOT a steady-state route. Source may be nil if
	// USE_GOEMBEDB=1 was deployed without STORAGE_BUCKET, in which case
	// the endpoint returns a 500 that names what is missing.
	if rawAdminToken := os.Getenv("ADMIN_MIGRATION_TOKEN"); rawAdminToken != "" {
		// Resolved here rather than beside the other two secrets at
		// startup: an operator who never opted in pays no
		// SecretsManager call, and a value that IS an ARN proves the
		// opt-in already happened. Exit 2 on failure for the reason
		// secret_resolver.go gives — comparing `X-Admin-Token` against
		// an unresolved ARN would 401 every operator call with nothing
		// in the log to say why.
		adminToken, rErr := resolveAWSSecretARN(ctx, "ADMIN_MIGRATION_TOKEN", rawAdminToken)
		if rErr != nil {
			slog.Error("could not resolve ADMIN_MIGRATION_TOKEN", "err", rErr)
			os.Exit(2)
		}
		migration := &goembedbadmin.MigrationConfig{
			Source:     migrationSource,
			Sink:       migrationSink,
			AdminToken: adminToken,
		}
		mux.Post("/admin/migrate-s3-to-goembedb", migration.Migrate)
	}

	// Unauthenticated on purpose: the SPA has to be able to ask what kind
	// of login it is facing before it holds a credential, and a login
	// endpoint that required a session would be a closed loop. Login
	// itself is rate-limited only by the shared password's entropy and a
	// constant-time compare — no accounts, no lockout, same as the other
	// two Components (D5).
	session := &api.SessionConfig{
		SigningKey:       []byte(jwtSecret),
		OperatorPassword: operatorPassword,
		// Secure cookies except in local HTTP development. Lambda is
		// always behind TLS, so this is on in every deployed shape.
		CookieSecure: os.Getenv("LOCAL_DEBUG") != "true",
		// So the SPA header can show which build is serving the page.
		// The smoke reads the same value off /healthz above.
		Version: version,
	}
	mux.Get("/api/v1/session", session.Session)
	mux.Post("/api/v1/login", session.Login)
	mux.Post("/api/v1/logout", session.Logout)

	mux.Group(func(r chi.Router) {
		r.Use(api.JWTMiddleware([]byte(jwtSecret)))
		r.Post("/api/v1/tenants/register", api.TenantsRegister(tenantStore))
		r.Get("/api/v1/tenants", api.TenantsList(tenantStore))
		r.Post("/api/v1/events", api.EventsIngest(eventStore, router, reg, tenantStore, nil))
		r.Post("/api/v1/scheduler/delivery/tick", api.DeliveryTick(drainer, replicationTick))
	})

	spaFS, err := fs.Sub(ui.BuildFS, "dist")
	if err != nil {
		slog.Warn("SPA dist not embedded (build not run?) — serving fallback", "err", err)
	} else {
		mux.Handle("/*", http.FileServer(http.FS(spaFS)))
	}

	// Bridge through go-aws-lambda-sdk, the fleet pattern, rather than
	// serving net/http directly. On Lambda the SDK calls lambda.Start,
	// which registers with the Runtime API and adapts Function URL
	// events onto this vanilla handler; with LOCAL_DEBUG=true it serves
	// ordinary HTTP on $PORT instead, so one binary covers local
	// development and the deployed artifact and the two cannot drift.
	//
	// This is not a refactor — it is the fix for a live failure. The
	// first deploy of this service (2026-11-23, infra-my-notifier-
	// callback-556b576) ran `srv.ListenAndServe()` in a goroutine and
	// blocked on ctx.Done(). Nothing ever joined the Runtime API, so
	// every invoke hit `platform.initReport status:timeout` after the
	// full 30s and the Function URL answered 502 — while the boot log
	// read perfectly: secret resolved, store built, "channel ready",
	// "listening addr=:8080". A service that logs a clean start and
	// cannot be invoked is the exact failure this component exists to
	// make loud, so the lesson is in TRAPS.md too.
	//
	// context.Background(), NOT the signal ctx: SIGTERM cancellation is
	// the runtime's business here, and handing a cancellable ctx to the
	// SDK would end the process on the first signal Lambda sends during
	// a freeze.
	svc, err := sdkservice.New(context.Background(), sdkservice.WithVanillaHandler(mux))
	if err != nil {
		slog.Error("sdkservice init failed", "err", err)
		os.Exit(1)
	}
	slog.Info("serving", "mode", serveMode(), "port", envOr("PORT", "8080"))
	if err := svc.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("sdkservice exited", "err", err)
		os.Exit(1)
	}
	slog.Info("exited cleanly")
}

// managedChannelJSON is the wire shape of one entry in NOTIFIER_CHANNELS.
// Field names match api.ChannelConfig's JSON tags, because that is the
// struct the plugin marshals.
type managedChannelJSON struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	MinSeverity string   `json:"minSeverity"`
	Sources     []string `json:"sources"`
	Kinds       []string `json:"kinds"`
	Token       string   `json:"token"`
	ChatID      string   `json:"chatId"`
	URL         string   `json:"url"`
}

// loadManagedChannels parses NOTIFIER_CHANNELS and resolves each
// credential, which may be a SecretsManager ARN.
//
// Credentials are resolved HERE and held in memory for the process
// lifetime. They are never written to the object store: the secret
// already has an owner in the SC registry, and a second copy in a
// bucket would go stale on rotation while continuing to look valid.
func loadManagedChannels(ctx context.Context) ([]registry.Managed, error) {
	raw := strings.TrimSpace(os.Getenv("NOTIFIER_CHANNELS"))
	if raw == "" {
		return nil, nil
	}

	var decoded []managedChannelJSON
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, fmt.Errorf("parse NOTIFIER_CHANNELS as a JSON array: %w", err)
	}

	out := make([]registry.Managed, 0, len(decoded))
	seen := make(map[string]bool, len(decoded))
	for i, c := range decoded {
		if c.Name == "" {
			return nil, fmt.Errorf("channel #%d has no name", i)
		}
		if seen[c.Name] {
			// Two declarations for one name would make "which won" a
			// coin flip decided by map iteration order.
			return nil, fmt.Errorf("channel %q is declared twice", c.Name)
		}
		seen[c.Name] = true

		// The label names the CHANNEL, not the env var, so the
		// "resolving from ARN" log line identifies which credential is
		// being fetched when several are.
		token, err := resolveAWSSecretARN(ctx, "channel "+c.Name+" token", c.Token)
		if err != nil {
			return nil, fmt.Errorf("resolve credential for channel %q: %w", c.Name, err)
		}

		out = append(out, registry.Managed{
			Name:        c.Name,
			Type:        c.Type,
			MinSeverity: c.MinSeverity,
			Sources:     c.Sources,
			Kinds:       c.Kinds,
			Credential:  token,
			ChatID:      c.ChatID,
			URL:         c.URL,
		})
	}
	return out, nil
}

// resolveCooldown reads NOTIFIER_COOLDOWN.
//
// An explicit "0" disables suppression, which is why this does not just
// pass the parsed value through: events.NewRouter treats a zero
// duration as "use the default", so disabling has to be mapped to a
// negative. An operator who writes `cooldown: 0` means off.
func resolveCooldown() (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv("NOTIFIER_COOLDOWN"))
	if raw == "" {
		return events.DefaultCooldown, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%q is not a Go duration (e.g. \"15m\", \"0\"): %w", raw, err)
	}
	if d <= 0 {
		return -1, nil
	}
	return d, nil
}

func buildObjectStore(ctx context.Context) (storage.Store, string, error) {
	switch t := envOr("STORAGE_TYPE", "s3"); t {
	case "s3":
		bucket := os.Getenv("STORAGE_BUCKET")
		if bucket == "" {
			return nil, "", errors.New("STORAGE_BUCKET is required when STORAGE_TYPE=s3")
		}
		region := envOr("STORAGE_REGION", "eu-central-1")
		st, err := storage.NewS3Store(ctx, storage.S3Config{
			Bucket:       bucket,
			Region:       region,
			EndpointURL:  os.Getenv("STORAGE_ENDPOINT_URL"),
			AccessKey:    os.Getenv("STORAGE_ACCESS_KEY"),
			SecretKey:    os.Getenv("STORAGE_SECRET_KEY"),
			UsePathStyle: os.Getenv("STORAGE_USE_PATH_STYLE") != "",
		})
		if err != nil {
			return nil, "", err
		}
		return st, fmt.Sprintf("s3://%s/%s", bucket, strings.Trim(os.Getenv("STORAGE_PREFIX"), "/")), nil

	case "filesystem":
		path := os.Getenv("STORAGE_PATH")
		if path == "" {
			return nil, "", errors.New("STORAGE_PATH is required when STORAGE_TYPE=filesystem")
		}
		st, err := storage.NewFSStore(ctx, storage.FSConfig{Root: path})
		if err != nil {
			return nil, "", err
		}
		return st, "file://" + path, nil

	default:
		return nil, "", fmt.Errorf("unsupported STORAGE_TYPE %q — this build ships %q and %q", t, "s3", "filesystem")
	}
}

func envOr(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}

// backendLabel maps the USE_GOEMBEDB flag to the Prometheus label
// value the gauge records. Centralised so the boot log and the gauge
// agree.
func backendLabel(useGoEmbedb bool) string {
	if useGoEmbedb {
		return "goembedb"
	}
	return "s3"
}

// serveMode names the branch go-aws-lambda-sdk's Start() will take, so
// the boot log says which one happened.
//
// The switch is LOCAL_DEBUG, not the presence of AWS_LAMBDA_FUNCTION_NAME
// (pkg/service/service.go:101) — worth stating, because the Lambda
// runtime sets the latter and it would be the intuitive guess. Outside
// Lambda without LOCAL_DEBUG=true the SDK still calls lambda.Start and
// the process hangs with no listener, which is what makes this log line
// worth its three lines of code.
func serveMode() string {
	if os.Getenv("LOCAL_DEBUG") == "true" {
		return "http (LOCAL_DEBUG=true)"
	}
	return "lambda runtime api"
}

// slogLogger adapts slog to the events.Logger / registry.Logger seam,
// which exists so those packages depend on no logging library.
type slogLogger struct{}

func (slogLogger) Infof(format string, args ...any)  { slog.Info(fmt.Sprintf(format, args...)) }
func (slogLogger) Warnf(format string, args ...any)  { slog.Warn(fmt.Sprintf(format, args...)) }
func (slogLogger) Errorf(format string, args ...any) { slog.Error(fmt.Sprintf(format, args...)) }

// requestLogger is a minimal structured-log middleware. Not
// production-grade (no latency bucketing, no sampling) — fits the
// notifier's scale.
func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		slog.Info(
			"http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", ww.Status(),
			"bytes", ww.BytesWritten(),
			"latency_ms", fmt.Sprintf("%.2f", float64(time.Since(start).Microseconds())/1000.0),
		)
	})
}
