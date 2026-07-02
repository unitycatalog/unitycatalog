# Local OIDC for Unity Catalog

Spin up the [Celonis cloud-oauth-server](https://github.com/celonis/cloud-oauth-server) for testing Unity Catalog authentication locally.

## Options

| Provider | Compose file | Best for |
|---|---|---|
| **Celonis OAuth** (recommended) | `compose.yaml` | Production-like OAuth flows, CLI `auth login`, integration tests |
| **mock-oauth2-server** | `compose.mock-oauth2.yaml` | Fast startup, scripted/token tests, minimal setup |

The Celonis OAuth stack binds **port 443** (TLS gateway) and **9099** (direct HTTP). mock-oauth2 binds **port 9090**. Do not run both Celonis OAuth and mock-oauth2 at once if ports would conflict.

## Celonis OAuth (recommended)

### 1. Hostname

Token issuers use `https://dev.dev.celonis.cloud`. Add this to `/etc/hosts`:

```sh
127.0.0.1 dev.dev.celonis.cloud
```

### 2. Start the OAuth stack

From the repository root:

```sh
docker compose -f docker/oidc/compose.yaml up -d
```

Wait until healthy:

```sh
docker compose -f docker/oidc/compose.yaml ps
```

- Discovery: https://dev.dev.celonis.cloud/.well-known/openid-configuration
- Direct HTTP (tests / debugging): http://localhost:9099
- OAuth client: `unity-catalog-local` / `unity-catalog-local-secret`
- Local tenant: `dev` (`X-Celonis-Team-Domain: dev`)

The stack runs:

- **postgres** — database for the OAuth server
- **oauth-server** — `ghcr.io/celonis/cloud-oauth-server/oauth-server`
- **oauth-gateway** — Caddy TLS proxy for the `dev.dev.celonis.cloud` issuer

Configuration lives in `docker/oidc/oauth/application-local.yaml` and `docker/oidc/oauth/jwks.json`, following the [development-setup example](https://github.com/celonis/development-setup/blob/master/files/docker/docker-compose.with-oauth-server.yml).

### 3. Configure Unity Catalog server

Merge `docker/oidc/server.properties.snippet` into `etc/conf/server.properties`, or copy the auth block manually.

For docker-mode integration tests, `docker/oidc/server.properties.docker.snippet` is merged automatically by `docker/start-uc-for-tests.sh`.

### 4. Trust the local TLS certificate

The Caddy gateway uses an internal CA. Import `docker/oidc/caddy/root.crt` into your JVM truststore before starting an auth-enabled UC server, or let `docker/start-uc-for-tests.sh` do this for binary/docker test runs.

### 5. Start UC and test

```sh
build/sbt server/package   # if not already built
bin/start-uc-server
```

Verify auth is enforced (expect 401):

```sh
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/api/2.1/unity-catalog/catalogs
```

Use the bootstrap admin token (created on first auth-enabled startup):

```sh
bin/uc --auth_token "$(cat etc/conf/token.txt)" user list
```

Interactive CLI login (opens browser → Celonis OAuth):

```sh
bin/uc auth login
```

### 6. UI

The bundled UI login page targets **Google** and **Okta**. For Celonis OAuth-backed servers keep Google auth disabled in `ui/.env` (see `ui.env.snippet`). Use the CLI or API for authenticated testing.

To run the UI against an auth-enabled server:

```sh
cd ui
corepack yarn install
corepack yarn start
```

The UI loads at http://localhost:3000 and proxies API calls to the server.

## mock-oauth2-server (lighter alternative)

[NAV mock-oauth2-server](https://github.com/navikt/mock-oauth2-server) starts in seconds and exposes a built-in login page. It is ideal for CI-style checks and quick token minting, but has no admin UI.

```sh
docker compose -f docker/oidc/compose.mock-oauth2.yaml up -d
```

Use these `server.properties` values instead:

```properties
server.authorization=enable
server.authorization-url=http://localhost:9090/default/authorize
server.token-url=http://localhost:9090/default/token
server.client-id=unity-catalog-local
server.client-secret=unity-catalog-local-secret
server.redirect-port=8080
server.allowed-issuers=http://localhost:9090/default
server.audiences=unity-catalog-local
```

Discovery: http://localhost:9090/default/.well-known/openid-configuration

## Object storage (MinIO)

To run OIDC and S3-compatible catalog storage together:

```sh
docker compose -f docker/compose.yaml -f docker/oidc/compose.yaml up -d
```

See [../minio/README.md](../minio/README.md) for MinIO `server.properties`
settings.

## Stop

```sh
docker compose -f docker/oidc/compose.yaml down
# or
docker compose -f docker/oidc/compose.mock-oauth2.yaml down
```

To stop MinIO as well:

```sh
docker compose -f docker/compose.yaml down
```

## Troubleshooting

- **Port 443 or 9099 in use** — stop the other OIDC compose stack or change the host port mapping.
- **Issuer / JWKS errors** — ensure `dev.dev.celonis.cloud` resolves to `127.0.0.1` and the OAuth gateway is running.
- **CLI login redirect fails** — the `unity-catalog-local` client allows `http://127.0.0.1:8080/callback` and `http://localhost:8080/callback`.
- **Token exchange fails** — ensure `server.allowed-issuers` is `https://dev.dev.celonis.cloud` and `server.audiences` is `unity-catalog-local`.
- **Integration tests** — start the OAuth stack first, then run `docker/tests/run-tests.sh`. Tests use the [`cloud-oauth-server-internal-client`](https://github.com/celonis/maven-internal/packages/1915797) library for internal OAuth server APIs (client lookup, etc.) and direct HTTP for the public authorization-code flow (user `id_token`s). Session JWTs are signed with `my-super-secret-key` (see `docker/oidc/oauth/application-local.yaml`).
