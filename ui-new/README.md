# Unity Catalog UI (ui-new)

A rebuild of the Unity Catalog web UI as a same-origin SPA backed by a small Rust
bridge. It browses catalogs, schemas, tables, volumes, functions, and models, and
supports the existing cookie-based auth (or an auth-disabled mode).

- `web/` — the SPA: [Bun](https://bun.com/) + Vite + React 19, [shadcn/ui](https://ui.shadcn.com/)
  on [Tailwind CSS](https://tailwindcss.com/) v4, [TanStack Router](https://tanstack.com/router)
  (file-based) + [TanStack Query](https://tanstack.com/query), talking [Connect](https://connectrpc.com/docs/web/getting-started/)
  to the bridge via [connect-query](https://connectrpc.com/docs/web/query/).
- `server/` — the bridge: a Rust [axum](https://docs.rs/axum) service exposing
  typed Connect services for each catalog domain, the allowlisted
  `UnityProxyService/Call` used by control-plane and permissions calls,
  `/config`, `/healthz`, and (in prod) the built SPA.
- `proto/` — the typed domain services plus the generic control/permissions proxy;
  `buf.gen.yaml` generates the TypeScript clients.

## Architecture

```
Browser SPA  --Connect (same-origin)-->  Rust bridge  --REST-->  UC Java server
             <--Set-Cookie propagated--               <--------
```

The bridge being same-origin removes CORS. Auth stays cookie-based (the current
ui's model): the SPA calls `POST /auth/tokens` (token-exchange, `ext=cookie`) and
`GET /scim2/Me` through the passthrough; the bridge forwards the browser's cookie
to UC and copies UC's `Set-Cookie` back onto the (same-origin) response, so the
session lands in the browser. `GET /config` tells the SPA whether auth is enabled
and which providers to show — the runtime replacement for the old build-time
`REACT_APP_*_AUTH_ENABLED` flags.

## Typed domain RPC contract

The protobuf package defines typed services for catalogs, schemas, tables,
volumes, functions, registered models and model versions, and metric views.
The implementation is split across the two-PR stack tracked by
[issue #1888](https://github.com/unitycatalog/unitycatalog/issues/1888) and
[issue #1889](https://github.com/unitycatalog/unitycatalog/issues/1889):

1. The contract PR adds the messages and `buf.validate` rules and keeps
   TypeScript generation working.
2. The implementation PR generates and registers the Rust Connect services,
   enforces the rules with
   [protovalidate-buffa](https://docs.rs/protovalidate-buffa/latest/protovalidate_buffa/),
   translates the typed requests to UC REST, and uses typed connect-query hooks
   in the SPA.

`UnityProxyService/Call` remains for auth, SCIM, and permissions endpoints that
are outside these domain contracts. Generated files under `web/src/gen/` and
Cargo's `OUT_DIR` are reproducible and must not be edited by hand.

## Prerequisites

- Bun 1.3+
- Rust (stable) / cargo
- Buf (or `bun install` in `web/`, which installs the local Buf CLI used by the
  Rust build script)
- A running Unity Catalog server (default `http://localhost:8080` — see the repo
  root `README.md` and `bin/start-uc-server`).

## Develop

Install and generate once, then run two processes. The bridge proxies to UC;
Vite serves the SPA and proxies
`/uc.v1.*`, `/config`, and `/healthz` to the bridge.

```bash
# One-time setup
cd ui-new/web
bun install
bun run generate

# Terminal 1 — the Rust bridge (listens on :8081, proxies to UC on :8080)
cd ui-new/server
UC_SERVER=http://localhost:8080 cargo run

# Terminal 2 — the SPA dev server (listens on :5173, proxies RPCs to :8081)
cd ui-new/web
bun run dev
```

Open http://localhost:5173. Override the bridge target with
`VITE_API_TARGET=http://host:port bun run dev`.

To exercise the login flow, start the bridge with auth enabled:

```bash
UI_AUTH_ENABLED=true GOOGLE_CLIENT_ID=<client-id> UC_SERVER=http://localhost:8080 cargo run
```

## Build (production, single origin)

```bash
cd ui-new/web && bun run build          # -> web/dist
cd ../server && cargo build --release    # -> target/release/uc-ui-bridge
WEB_DIST=../web/dist UC_SERVER=http://localhost:8080 ./target/release/uc-ui-bridge
```

The bridge then serves the SPA and the RPCs from one origin on `PORT` (default
8081), with an `index.html` fallback for client-side routes.

## Bridge configuration (env)

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8081` | Bridge listen port |
| `UC_SERVER` | `http://localhost:8080` | Unity Catalog server base URL |
| `UI_AUTH_ENABLED` | `false` | Gate the SPA behind login |
| `GOOGLE_CLIENT_ID` | _(empty)_ | Enables the Google sign-in button |
| `OKTA_AUTH_ENABLED` | `false` | Advertise Okta as enabled |
| `KEYCLOAK_AUTH_ENABLED` | `false` | Advertise Keycloak as enabled |
| `ALLOWED_ORIGINS` | _(empty)_ | Extra CORS origins (comma-separated); empty = same-origin only |
| `WEB_DIST` | `../web/dist` | Built SPA directory to serve |

## Test

The SPA is tested with [Vitest](https://vitest.dev/) + React Testing Library
(the standard Vite/React stack — Vitest reuses the Vite config). Coverage is
gated at 80% (statements/branches/functions/lines).

```bash
cd ui-new/web
bun run proto:check      # protobuf lint + build
bun run typecheck        # tsr generate + tsc
bun run test             # vitest run
bun run test:watch       # vitest (watch mode)
bun run test:coverage    # vitest run --coverage (enforces the 80% thresholds)

cd ../server && cargo test   # bridge unit/integration tests
```