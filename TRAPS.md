# The traps this repo already prevents

Carries the fifteen traps from `sc-plugin-echo`'s `TRAPS.md` plus four new
ones specific to the SC Components class.

Canonical catalogue (traps 1-15):
**[forge/docs/design/2026-11-01/plugin-extraction-playbook/](https://github.com/simple-container-com/forge/tree/main/docs/design/2026-11-01/plugin-extraction-playbook/)**.
Traps 16-19 live here because they're specific to the Component class
mechanism — not generic to all plugins.

Traps 18 and 19 were both found on the **first deploy**, within an hour of
each other, and neither was reachable from a unit test or a green build:
one needed an invoke, the other needed an operator to open the page. That
is the shape of fault this class of component produces.

## Already encoded in this repo — don't undo

| # | Trap | How the repo handles it |
|---|---|---|
| 1-15 | See sc-plugin-echo TRAPS.md | Inherited verbatim via the shared `api/pkg/plugin/runtime` scaffolding (NewContext, Settle, HarvestExports, ParentInfoFromRequest, RunComputeProcess). |
| **16** | **Go-interface assertion against plugin-owned value never matches in production**. `cfg.(api.X)` where `X` is a core-defined interface and `cfg` crossed go-plugin gRPC falls through to the default branch — production `sc` does not link plugin `/api` modules, so plugin-owned values arrive as opaque `*api.PluginFieldConfig`. 8th exposure in 24h at the time of writing. | Dispatch uses **Schema-advertised `component_resource_types` + `.Raw` reader** on the opaque wrapper. Same pattern as `safe_to_replace_types` (api #557) and api #592's `multiRegistrar.forDomain` fix. No `cfg.(api.…)` anywhere in `plugin/` for plugin-owned values. Pre-release grep: `grep -rn 'cfg\.(api\.' plugin/ && echo REJECT`. |
| **17** | **Push-register call to a freshly-deployed Lambda can cold-start, roll, or 5xx mid-redeploy**. A naive single-attempt call fails consumer deploys on transient 5xx with no retry. | `plugin/internal/compute.go` wraps the `POST /api/v1/tenants/register` call in a retry policy per the sc-components-platform design doc §P3 push-model operational contract: 3 attempts total, exponential backoff `1s → 2s → 4s` + 25% jitter, 10s per-attempt timeout, non-retryable 4xx except 429, consumer deploy fails diagnosably on all-retries-exhausted with a hint naming the component as the blocker. |
| **18** | **A service binary that serves `net/http` directly never joins the Lambda Runtime API — and the boot log looks perfect.** Live-caught on this repo's FIRST deploy (2026-11-23, `infra-my-notifier-callback-556b576`): `main()` ran `srv.ListenAndServe()` in a goroutine and blocked on `ctx.Done()`. The service logged `secret resolved`, `channel ready`, `listening addr=:8080` — and every invoke ended in `platform.initReport … "status":"timeout","durationMs":30000` with the Function URL answering **502**. Nothing in the service's own log says anything is wrong, so the first investigation went to Cloudflare, the worker route and the DNS record before the entrypoint. Note the switch is **`LOCAL_DEBUG=true`** (`go-aws-lambda-sdk/pkg/service/service.go:101`), **not** the presence of `AWS_LAMBDA_FUNCTION_NAME` — the intuitive guess, and wrong. | `service/cmd/notifier/main.go` bridges through `sdkservice.New(context.Background(), sdkservice.WithVanillaHandler(mux))` + `svc.Start()` (observatory's pattern), so one binary covers local dev (`LOCAL_DEBUG=true` → HTTP on `$PORT`) and the deployed artifact, and the two cannot drift. `context.Background()`, never the signal ctx — a cancellable ctx would end the process on the first SIGTERM Lambda sends during a freeze. Guarded by `service/cmd/notifier/lambda_runtime_test.go`, which **builds the real binary** and drives it through a stub Runtime API: no handler-level test can see this class of fault, because every handler was correct and every unit test was green. What was broken is whether anything ever hands a request *to* a handler. |
| **19** | **A resolved secret that nothing reads, and a valid token that nothing checks.** Two faults, found together the moment an operator opened the dashboard for the first time (2026-11-23), and neither was visible from a green build or a 200 on `/healthz`. (a) `NOTIFIER_OPERATOR_PASSWORD` was resolved at boot — ARN unwrapped, no error — and then **passed to nobody**: there was no login route at all, so the SPA asked the operator to paste a hand-minted JWT into `localStorage.notifierToken`. That is unusable, and it is the wrong secret to put in a browser: `SERVICE_JWT_SECRET` also KEKs every tenant's sealed channel credentials. A resolved-and-unused secret is indistinguishable at boot from a working one. (b) `GET /api/v1/tenants` checked the **signature and nothing else**, with a comment claiming "a signed token is enough for the notifier" — but every publisher stack's plugin holds a valid `event` token for this service, and those are scoped to one `tenant` precisely so a publisher cannot reach past itself. The ungated read handed that scoping back: any one tenant could enumerate all of them. Nobody reported (b); it was found while fixing (a). | `service/internal/api/session.go` is the login flow the other two Components already use (D5): shared password, `crypto/subtle` compare, HttpOnly `Secure` SameSite=Lax cookie carrying an `op:"operator"` HS256 JWT, plus an **unauthenticated** `GET /api/v1/session` reporting `{authenticated, password_set}` so the SPA can tell a typo from a deployment with no password configured — a 401 for both is how an operator ends up hunting a typo that cannot exist. `requireOperator` (jwt_middleware.go) is the gate, and `tokenFromRequest` accepts the cookie as well as the Bearer header, header first. The boot log now carries `operator_login=true|false` — the boolean, never the value. Nine entries in the `session` group of `hack/mutation-check.py` hold all of it: drop the gate, drop the op check in either place, flatten the 403 to a 401, compare the password to itself, unset HttpOnly, keep the cookie alive through logout, remove the cookie transport, or flip its precedence — each one goes red. **The rule, stated once: validity is not authorization, and "the config accepted it" is not "the code reads it".** |

## Checklist before cutting a release

Grep across the plugin:

- [ ] `grep -rn 'cfg\.(api\.' plugin/` → nothing (trap 16)
- [ ] `grep -rn 'PluginFieldConfig' plugin/internal/` → ok to appear ONLY in comments or as explicit `.Raw` reads (trap 16)
- [ ] `grep -rn 'defer done()' plugin/` → nothing (trap 1)
- [ ] `grep -rn 'pctx\.Close()' plugin/` → nothing outside `_test.go` (trap 1)
- [ ] `grep -rn 'ProvisionResponse{Urn:' plugin/` → every match carries `Outputs:` (trap 3)
- [ ] `plugin/internal/compute.go` retry policy verified: 3 attempts, exponential 1s/2s/4s + 25% jitter, 10s timeout per attempt, non-retryable 4xx except 429 (trap 17)
- [ ] `grep -rn 'go:embed.*dist' service/` → every match uses `all:dist` prefix (verbatim pattern from forge-notifier/internal/ui/ui.go)
- [ ] `grep -rn 'ListenAndServe' service/cmd/ | grep -v '//\|_test.go'` → nothing; the entrypoint goes through `sdkservice.Start()`. The bare grep matches trap 18's own explanatory comment and the test's failure message, so filter them or the check cries wolf (trap 18)
- [ ] `cd service && go test ./cmd/notifier/ -count=1` → green; this is the build-the-binary Runtime API test, and it is the only thing that proves the deployed artifact is invokable at all (trap 18)
- [ ] `grep -rn 'localStorage\|sessionStorage' service/internal/ui/src/` → matches ok ONLY in comments; the SPA authenticates with the HttpOnly session cookie, and the only token it could hold in storage is the one that KEKs every tenant's credentials (trap 19)
- [ ] every route the SPA reads is inside the `requireOperator` gate — `grep -rn 'func Tenants\|func Channels\|func Deliveries' service/internal/api/` and check each handler's first statement (trap 19)
- [ ] every secret resolved in `main()` is passed to something — `grep -n 'resolveAWSSecretARN' service/cmd/notifier/main.go` and trace each result to a consumer. A resolved secret that nothing reads logs identically to a working one (trap 19)
- [ ] `python3 hack/mutation-check.py` → `all N mutations caught`, then `git diff --exit-code`. A killed run leaves one file mutated and the next run reports it as "not proven" — which looks like a test defect and is not one

## When you hit a new trap

Add it to this file + the forge plugin-extraction playbook. The pre-release
lint checklist is the floor; add greps as you go.
