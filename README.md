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
