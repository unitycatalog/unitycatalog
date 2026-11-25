# Unity Catalog UI (ui-new)

A rebuild of the Unity Catalog web UI as a same-origin SPA backed by a small Rust
bridge. It browses catalogs, schemas, tables, volumes, functions, registered
models, and metric views, and supports the existing cookie-based auth (or an
auth-disabled mode).

It can also create, edit, and delete them. Every object type has a create
dialog, metadata can be edited wherever UC supports updates, and delete asks
the user to type the object's name. The UI offers these actions to everyone and
leaves authorization to UC, which reports a refusal as an error toast. When the
bridge's `/config` reports auth enabled, the UI offers Finalize only when the
current SCIM username matches the version's creator; with auth disabled, it
offers the action. This is a UI hint: UC remains authoritative, and a mismatch
between bridge and server auth settings can still result in an authorization
error toast.

- `web/` — the SPA: [Bun](https://bun.com/) + Vite + React 19, [shadcn/ui](https://ui.shadcn.com/)
  on [Tailwind CSS](https://tailwindcss.com/) v4, [TanStack Router](https://tanstack.com/router)
  (file-based) + [TanStack Query](https://tanstack.com/query), talking [Connect](https://connectrpc.com/docs/web/getting-started/)
  to the bridge via [connect-query](https://connectrpc.com/docs/web/query/).
- `server/` — the bridge: a Rust [axum](https://docs.rs/axum) service exposing
  typed Connect services for each catalog domain, the allowlisted
  `UnityProxyService/Call` used by control-plane and permissions calls,
  `/config`, and `/healthz`. Vite or Nginx serves the SPA.
- `proto/` — the typed domain services plus the generic control/permissions proxy;
  `buf.gen.yaml` generates the TypeScript clients.

## Architecture

```
Browser SPA  --Connect (same-origin)-->  Rust bridge  --REST-->  UC Java server
             <--Set-Cookie propagated--               <--------
```

The browser reaches the SPA and the bridge through one origin (Vite in
development, or a web server that proxies to the bridge in production), so no
CORS is needed by default; `ALLOWED_ORIGINS` enables it for a SPA served from
another origin. Auth stays cookie-based
(the current ui's model): the SPA calls `POST /auth/tokens` (token-exchange,
`ext=cookie`) and `GET /scim2/Me` through the passthrough. The bridge forwards
the browser's cookie, `Authorization` header, and `X-Requested-With` header to UC,
and copies UC's `Set-Cookie` back onto the (same-origin) response, so the session
lands in the browser. UC's CSRF check requires `X-Requested-With:
XMLHttpRequest` on cookie-authenticated writes, so the SPA sends it on every
request. `GET /config` tells the SPA whether auth is enabled
and which providers to show — the runtime replacement for the old build-time
`REACT_APP_*_AUTH_ENABLED` flags.

## Typed domain RPC contract

The protobuf package defines typed services for catalogs, schemas, tables,
volumes, functions, registered models and model versions, and metric views.
The bridge generates the Rust Connect services from these contracts, enforces
their `buf.validate` rules with
[protovalidate-buffa](https://docs.rs/protovalidate-buffa/latest/protovalidate_buffa/)
before calling UC, and translates each typed request to the UC REST API. The SPA
calls the services through typed connect-query hooks under `web/src/hooks/`.
See [issue #1888](https://github.com/unitycatalog/unitycatalog/issues/1888) for
the design.

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

## Build artifacts

```bash
cd ui-new/web && bun run build          # -> web/dist
cd ../server && cargo build --release    # -> target/release/uc-ui-bridge
UC_SERVER=http://localhost:8080 ./target/release/uc-ui-bridge
```

Serve `web/dist` from a web server that proxies `/uc.v1.*`, `/config`, and
`/healthz` to the bridge, and falls back to `index.html` for any other path so
client-side routes load on refresh or from a link. The container setup below
supplies that Nginx layer.

## Containers

The Compose files need Docker Compose v2 with BuildKit, the default builder in
current Docker releases: the bridge Dockerfile mounts build secrets and caches,
and the repository Dockerfile uses `COPY --parents` and heredocs. Both the
bridge and web builds fetch the protovalidate module from the Buf Schema
Registry, so they need to reach `buf.build` even when the registry mirrors
below are set.

The default Compose file builds this checkout's Java server and starts the full
local stack: PostgreSQL metadata, RustFS object storage, the Rust bridge, and
the Nginx-hosted SPA. Unity Catalog runs with authorization off, since the stack
sets up no identity provider; to try sign-in, run the remote Compose file
against a server that has it on.

The repository's `etc/conf` remains the configuration source: `config-init`
copies it into a runtime volume and appends local PostgreSQL and storage
overrides. Before the server starts, a one-shot `upgrade-schema` service runs
`bin/start-uc-server --upgrade-schema`, as the Helm chart's init container
does, to set up or upgrade the PostgreSQL schema. Once the server is up,
`storage-init` registers RustFS as an `s3_compatible` storage credential with
an external location for `s3://uc-warehouse`, so the server reaches the bucket
at RustFS and vends short-lived sessions scoped to the requested path.
`RUSTFS_ACCESS_KEY` and `RUSTFS_SECRET_KEY` (both `rustfsadmin` by default) set
RustFS's root keys, and `storage-init` updates the credential to the current
keys on every start. It passes the secret key to `bin/uc` as an argument, so the
key shows in the process list while `storage-init` runs. The bucket name is
fixed: once anything uses the external location, the server refuses to move it
to another bucket. The bridge starts only after `storage-init` succeeds, so a
storage setup failure keeps the UI down; `docker compose logs storage-init`
shows the cause.

```bash
cd ui-new
docker compose up --build
```

Open the UI at http://localhost:3000, Unity Catalog at
http://localhost:8080, and the RustFS console at http://localhost:9001
(`rustfsadmin` / `rustfsadmin` by default). Managed volumes and models need a
storage location on their catalog or schema, so create the catalog with a
storage root in the bucket, such as `s3://uc-warehouse/<catalog>`. Managed
tables, including those created through Iceberg REST and Spark, also work in a
catalog without one: `config-init` sets the deprecated `storage-root.tables`
fallback to the bucket.

To restart the stack in order, run `docker compose down` and then
`docker compose up`; the volumes are kept. `docker compose restart` restarts
every service at once, without that order. Docker restarts the long-running
services when they exit, including after a daemon restart, so the server comes
back if it starts before PostgreSQL is ready. `upgrade-schema` and
`storage-init` reuse the image the `unitycatalog` service builds, so run
`docker compose build unitycatalog` before starting either on its own.

Vended credentials name the endpoint `http://rustfs:9000`, which resolves inside
the Compose network. To read or write with them from an engine on the host, add
`127.0.0.1 rustfs` to `/etc/hosts`; this works while `RUSTFS_API_PORT` is 9000,
its default.

The server encrypts the storage credential's secret with
`UC_SERVER_CREDENTIAL_ENCRYPTION_KEY`, the variable the Helm chart sets from
`server.credentialEncryptionKeySecret`. Export it, as 32 random bytes in base64
(`openssl rand -base64 32`), to use your own key. Left empty, the server uses a
key that `config-init` generates once into the `uc_conf` volume. `storage-init`
re-encrypts its own credential with the current key on every start, but any
other `s3_compatible` credential stored with one key can't be read with another,
so keep using the same key while the database holds such credentials.

Use the remote Compose file to omit Unity Catalog, PostgreSQL, and RustFS and
point the bridge at an existing open-source Unity Catalog server:

```bash
# Replace with your UC URL. The remote file turns UI_AUTH_ENABLED on by default.
UC_SERVER=https://uc.YOUR_DOMAIN.dev \
  docker compose -f docker-compose-remote.yaml up --build
```

With auth on, set `GOOGLE_CLIENT_ID`, `OKTA_AUTH_ENABLED`, or
`KEYCLOAK_AUTH_ENABLED` for the providers your server has. With none set, the
sign-in page says no provider is enabled and offers only pasting a Unity
Catalog access token.

The bridge resolves `UC_SERVER` inside its container, where `localhost` is the
container itself. For a server on the host, use an address the container can
reach, such as `http://host.docker.internal:8080` on Docker Desktop.

Both files use the Compose project `unitycatalog-ui-new`, so starting one
replaces the other's bridge and web containers. Stop a stack with the file
that started it: `docker compose -f docker-compose-remote.yaml down` leaves the
full stack's Unity Catalog, PostgreSQL, and RustFS running. Every
`docker compose` command against the remote file, including `ps` and `down`,
needs `UC_SERVER` set.

The default file publishes the UI on `UI_PORT`, Unity Catalog on `UC_PORT`,
PostgreSQL on `POSTGRES_PORT`, and RustFS on `RUSTFS_API_PORT` and
`RUSTFS_CONSOLE_PORT`; the remote file publishes only the UI, on `UI_PORT`.
Both publish on `127.0.0.1` only; set `BIND_ADDRESS`, for example to `0.0.0.0`,
to publish on other interfaces. In the default file, Unity Catalog's
authorization is off, and PostgreSQL and RustFS use default credentials unless
`POSTGRES_PASSWORD`, `RUSTFS_ACCESS_KEY`, and `RUSTFS_SECRET_KEY` are set, so
publish it only on a network you trust. `UI_AUTH_ENABLED` gates only the UI:
Unity Catalog's own port stays open without authorization. PostgreSQL keeps the
password its volume was created with, so set `POSTGRES_PASSWORD` before the
first start. Local state is kept in named volumes; remove it with
`docker compose down --volumes`.

Set `CARGO_CONFIG_FILE` to pass private Cargo registry settings into the bridge
build stage as a BuildKit secret. When unset, Compose uses a checked-in inert
placeholder and Cargo uses its public registry. To use a mirror instead, set
`CARGO_REGISTRY_URL` to its sparse index URL, ending in `/` and without the
`sparse+` prefix, which the build adds; it takes precedence over
`CARGO_CONFIG_FILE`. `NPM_REGISTRY_URL` sets the npm registry for the web build,
and `MAVEN_PROXY_URL` is forwarded to the repository Dockerfile for the Java
build.

## Bridge configuration (env)

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8081` | Bridge listen port (1–65535) |
| `UC_SERVER` | `http://localhost:8080` | Unity Catalog server base URL (`http` or `https`, without a query or fragment) |
| `UI_AUTH_ENABLED` | `false` | Gate the SPA behind login |
| `GOOGLE_CLIENT_ID` | _(empty)_ | Google OAuth client ID, ending in `.apps.googleusercontent.com`; enables the Google sign-in button. Blank (empty or whitespace only) leaves it off. An ID not in the usual `<number>-<id>.apps.googleusercontent.com` shape is used with a startup warning |
| `OKTA_AUTH_ENABLED` | `false` | Advertise Okta as enabled |
| `KEYCLOAK_AUTH_ENABLED` | `false` | Advertise Keycloak as enabled |
| `ALLOWED_ORIGINS` | _(empty)_ | Extra CORS origins (comma-separated, each must exactly match the browser's serialized `Origin`—no trailing slash or explicit default port; other entries are logged and skipped); empty = same-origin only |

The `*_ENABLED` variables accept `true`/`false`, `1`/`0`, `yes`/`no`, or
`on`/`off`, ignoring case. An unset or empty variable takes its default. A
non-empty invalid boolean or `PORT` (including whitespace only), a `UC_SERVER`
that is whitespace only or not an `http` or `https` base URL without a query
or fragment, a non-blank `GOOGLE_CLIENT_ID` that does not end in
`.apps.googleusercontent.com`, or a value that is not valid Unicode, stops the
bridge at startup with an error naming the variable.

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
