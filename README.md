# sc-component-notifier

The template every SC **component** clones from, AND the throwaway smoke
subject of slice 1a of the [sc-components-platform](https://github.com/simple-container-com/forge/blob/main/docs/roadmap/items/sc-components-platform.md)
milestone.

An **SC component** is a new class of SC plugin whose resource provisions a
cloud resource AND a full forge-fleet-style microservice (go backend +
React SPA via `go:embed`) into the operator's chosen template (lambda /
cloud-compose / k8s). When a deploying service declares `uses:
[my-component]` in its `client.yaml`, the plugin's `ComputeProcess` sidecar
pushes a tenant registration to the deployed component service via a
JWT-authed HTTP call — tenants persist in the component service's own DB,
not in Pulumi state.

## Publishing to it — the consumer contract

Declaring the dependency is the whole of the wiring. No hand-minted token,
no `sc secrets add`, no hostname in config:

```yaml
my-backup:
  type: sc-backup
  uses: [my-notifier]        # ← this line is the wiring
```

SC mints a per-consumer `op:"event"` JWT and injects two env vars into the
consumer:

| Var | Kind | Value |
|---|---|---|
| `SC_NOTIFY_URL` | plain | the notifier's base URL |
| `SC_NOTIFY_TOKEN` | **secret** | that consumer's own event token |

The `SC_` prefix is reserved: SC core refuses an injected name outside it,
so a provider plugin can never shadow an application's own `NOTIFY_URL`,
`DATABASE_URL`, or anything else the consumer owns.

The same two values are also reachable as template placeholders, for a
consumer whose config needs them somewhere other than its env:

```yaml
    endpoint: "${resource:my-notifier.url}/api/v1/events"
    token:    "${resource:my-notifier.token}"
```

**`url` and `token` are not equally available, and the asymmetry is
structural.** `url` is one value for everyone, so PreProvision publishes it
and it resolves for *any* resource. `token` carries a `tenant` claim scoped
to one consumer, so only `ComputeProcess` can publish it — and therefore it
resolves **only for a resource that declared `uses: [my-notifier]`**.
Elsewhere SC's placeholder resolution fails *open* and hands on the literal
string `${resource:my-notifier.token}`, which deploys cleanly and surfaces
as a 401 at the first incident. SC core refuses a `uses:`-derived value
that still contains `${resource:` for exactly this reason; if you reference
`.token` anywhere else, you own that failure.

Each consumer's token is distinct because its `tenant` claim is, and the
ingest handler takes an event's `source` from that claim rather than from
the request body. Two publishers sharing a token would mean every alert
from one is attributed to the other, and a channel's `sources:` allowlist
would silently stop meaning what it says.

The design doc is at [`forge/docs/design/2026-11-10/sc-components-platform/`](https://github.com/simple-container-com/forge/tree/main/docs/design/2026-11-10/sc-components-platform/).
Read it before working on this repo. §P1 explains the Path A sidecar
wrapper (Schema-advertised component types + `.Raw`-reading, NOT Go
interface assertion — see `CLAUDE.md` and `TRAPS.md` for why). §P3 is the
push-register model this repo's plugin implements.

Five real components will clone this repo: **Backup → Monitoring →
Notifier → Credentials rotation → Incident Management**. `sc-component-notifier`
is the forcing function that proves the primitives work end-to-end, with
the smallest possible payload (a tenants list in Mongo, no real work).

## Three modules

```
sc-component-notifier/
├── api/         ← config tier. Published as its own Go module, referenced
│                  by the plugin binary (NOT by SC core — see CLAUDE.md on
│                  why Schema-advertised types + `.Raw` reader, not Go
│                  interface assertion).
├── plugin/      ← provisioning tier. The hashicorp/go-plugin subprocess
│                  binary SC core spawns via the `plugins:` block.
│                  Advertises `sc-notifier` under `component_resource_types`
│                  in its Schema. Provision is a no-op aside from
│                  republishing the service URL dispatched by Path A.
│                  ComputeProcess is where the push-register call lives.
└── service/     ← the microservice. go-aws-lambda-sdk + MongoDB + embedded
                   React SPA. Published to Dockerhub by the operator
                   (NOT by this repo's CI — see CLAUDE.md on the Dockerhub
                   publish gate).
```

## Storage (go-embedb adoption, step 1)

The service persists its own state (tenants, events, deliveries, channels,
cooldown, the single-flight drain lease) in an **object store**. Two
backends are built into the binary, picked at boot by `USE_GOEMBEDB`:

| `USE_GOEMBEDB` | Backend | Shape |
|---|---|---|
| unset or `0` (**default**) | `S3Store` (or `FSStore` for `STORAGE_TYPE=filesystem`) | one JSON object per key, under `STORAGE_BUCKET` + `STORAGE_PREFIX` |
| `1` | `MongoStore` on top of embedded **go-embedb** (ferretdb + sqlite + active-active S3 replication) | one Mongo document per key, routed to a collection by path prefix |

**This release ships the go-embedb code path with `USE_GOEMBEDB`
defaulted off**, so a `main` deploy is bit-identical to the pre-adoption
service. An operator flips the flag per stack only after
`POST /admin/migrate-s3-to-goembedb?dry_run=1` reports count parity.

Full design: [`forge/docs/design/2026-11-22/go-embedb-active-active-s3/`](https://github.com/simple-container-com/forge/tree/main/docs/design/2026-11-22/go-embedb-active-active-s3/).

### Why a `replace` to a private pseudo-version

`go-embedb` is a private fork of FerretDB. `service/go.mod` carries:

```
replace github.com/FerretDB/FerretDB =>
    github.com/simple-container-com/go-embedb v0.0.0-20261123133627-155d9eb7d9e0
```

A module-path replace is NOT optional — a filesystem replace
(`=> ../../go-embedb`) resolves only on an operator laptop that
happens to have the fork checked out next door, and it fails in `go mod
download` *before* any build tag or env var is read. That turns a flag
defaulted-off into main-red the day it merges.

Because the fork is private, CI needs a read credential. It lives in
this repo's own SC registry — same as the fleet's `SC_CONFIG` pattern,
nothing extra in GitHub Actions secrets:

```sh
# one-time bootstrap (operator)
ssh-keygen -t ed25519 -N '' -f .sc/deploy-keys/go-embedb.pem
# install the PUBLIC half as a deploy key on simple-container-com/go-embedb
yes Y | sc secrets add .sc/deploy-keys/go-embedb.pem
sc secrets hide --force
echo '.sc/deploy-keys/go-embedb.pem' >> .gitignore
```

`.github/workflows/pr.yml` + `release.yml` then run `sc secrets reveal
--force`, point `GIT_SSH_COMMAND` at the revealed key, and `go mod
download` resolves. The Dockerfile mounts the same file as a buildx
secret so the module layer gets cached without the key ever landing in
an image layer.

**Operator gate:** if `.sc/secrets.yaml` + `.sc/deploy-keys/go-embedb.pem`
are not yet set up in this repo, CI's `service-test` will fail at the
`sc secrets reveal` step until they are. That is the SAME blocker
sc-component-observatory and sc-component-backup hit before their
adoptions landed; the fix is the bootstrap recipe above, not a
workaround in this PR.

### Migration + rollback

- **Dry-run first.** `POST /admin/migrate-s3-to-goembedb?dry_run=1`
  with header `X-Admin-Token: <ADMIN_MIGRATION_TOKEN>` lists every
  S3 key the migration would touch and the per-collection row count
  on the destination, without writing. Operator diffs the two.
- **Wet run.** Same endpoint without `dry_run=1`. Per-key errors are
  non-fatal and surface in the response body; the migration is
  idempotent (ReplaceOne-with-upsert), so a partial run can be
  retried.
- **Set the replication bucket BEFORE the flag flip on Lambda.**
  `GOEMBEDB_S3_BUCKET` is a separate env var; on Lambda it is
  **hard-required** when `USE_GOEMBEDB=1`. Boot refuses with
  `GOEMBEDB_S3_BUCKET is required on Lambda …` otherwise. The reason
  is not optimisation: Lambda's `/tmp` is per-container and dies with
  every container recycle, so without a shared bucket each warm
  container runs its own ephemeral store. That silently defeats
  `AcquireTickLease` single-flight (every container wins its own
  lease → **double delivery to real recipients**), replay dedupe,
  and cooldown tracking.
- **Flip the flag.** Set `USE_GOEMBEDB=1` on the service and redeploy.
  `notifier_store_backend{backend="goembedb"}` on `/metrics` goes to
  `1`, `backend="s3"` to `0`.
- **Rollback.** Unset `USE_GOEMBEDB` (or set `0`) and redeploy. S3Store
  still holds the pre-cutover data, the two stores never cross-write,
  and recent writes made while on go-embedb are lost on rollback — the
  migration is forward-only within a cutover window. (A reverse
  migration endpoint is a PR B follow-up, not shipped here.)

### Metrics

`GET /metrics` exposes Prometheus text-exposition, unauthenticated —
process-level counters + `notifier_store_backend{backend="s3"|"goembedb"}`.
Alerting pattern:

```
# A deploy that was supposed to flip to go-embedb but hasn't
notifier_store_backend{backend="s3"} == 1 AND <expected-cutover-time>
```

### Admin token

`ADMIN_MIGRATION_TOKEN` gates the migration endpoint. **Empty disables
the endpoint entirely** — that is the safe default: an operator who
did not opt in never opens an unauthenticated migration route. In this
PR the value is read as a plain env; PR B binds it through the SC
secret-env wire so an operator writes a one-line `migrationToken:` in
`server.yaml` and the token arrives in the Lambda as a SecretsManager
ARN.

## Running tests

```
make test       # fans out to all three modules
```

Both `api/` and `plugin/` tests must stay green on `main`. `service/`
tests cover the HTTP handlers + JWT middleware; they need Go 1.25+ and
no external services.

## Status

- Repo scaffolded 2026-11-11.
- Slice 1a (Schema+Raw dispatch + the `ComponentConfig` reader) landed in
  `simple-container-com/api`; the service is deployed at
  `notify.simple-forge.com` and its operator login + tenants gate are
  smoked end to end.
- `uses:` on a sibling **Component** (as opposed to a `client.yaml`
  service) needs an `api` core change and is not in a release yet — see
  `forge/docs/design/2026-11-23/component-uses-wiring/`. Until it lands,
  only a deploying service can be a consumer.
- **No publisher has sent a real event yet.** The tenants table reads
  `count: 0`; the end-to-end smoke (force a backup failure, assert the
  Telegram message, assert the `Delivery` row) is still owed.
- Service image Dockerhub publish gated on operator sign-off (CLAUDE.md
  "package publishing, registries or release pipelines" ratchet).

## License

MIT. See [LICENSE](LICENSE).
