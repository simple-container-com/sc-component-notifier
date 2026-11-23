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
//	NOTIFIER_OPERATOR_PASSWORD       SPA login secret (optional)
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
//	PORT                             listen port            (default 8080)
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

	"github.com/simple-container-com/sc-component-notifier/service/internal/api"
	"github.com/simple-container-com/sc-component-notifier/service/internal/crypto"
	"github.com/simple-container-com/sc-component-notifier/service/internal/events"
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

	sealer, err := crypto.NewSealer([]byte(jwtSecret))
	if err != nil {
		slog.Error("credential sealer unavailable", "err", err)
		os.Exit(2)
	}

	objects, target, err := buildObjectStore(ctx)
	if err != nil {
		slog.Error("object storage unavailable", "err", err)
		os.Exit(2)
	}
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
		"managed_channels", len(managed), "cooldown", cooldown.String(),
		"self_url", os.Getenv("NOTIFIER_SELF_URL"))

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

	mux.Get("/healthz", api.Healthz)

	mux.Group(func(r chi.Router) {
		r.Use(api.JWTMiddleware([]byte(jwtSecret)))
		r.Post("/api/v1/tenants/register", api.TenantsRegister(tenantStore))
		r.Get("/api/v1/tenants", api.TenantsList(tenantStore))
		r.Post("/api/v1/events", api.EventsIngest(eventStore, router, reg, nil))
		r.Post("/api/v1/scheduler/delivery/tick", api.DeliveryTick(drainer))
	})

	spaFS, err := fs.Sub(ui.BuildFS, "dist")
	if err != nil {
		slog.Warn("SPA dist not embedded (build not run?) — serving fallback", "err", err)
	} else {
		mux.Handle("/*", http.FileServer(http.FS(spaFS)))
	}

	srv := &http.Server{
		Addr:              ":" + envOr("PORT", "8080"),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		slog.Info("listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("listen failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutdown requested")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "err", err)
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

// slogLogger adapts slog to the events.Logger / registry.Logger seam,
// which exists so those packages depend on no logging library.
type slogLogger struct{}

func (slogLogger) Infof(format string, args ...any)  { slog.Info(fmt.Sprintf(format, args...)) }
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
