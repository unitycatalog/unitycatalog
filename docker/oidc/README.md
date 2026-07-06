# Local OIDC for Unity Catalog

Spin up the [Celonis cloud-oauth-server](https://github.com/celonis/cloud-oauth-server) for testing Unity Catalog authentication locally.

Ports: **9010** (HTTP Envoy for tests) and **443** (HTTPS Envoy for UC JWKS/issuer discovery). Direct oauth-server HTTP **9099** is exposed for debugging only.

## 1. Tenant hostname

OAuth uses the Celonis pattern `{team}.{realm}`:

| Variable | Default | Example |
|----------|---------|---------|
| `UC_OAUTH_TEAM` | `dev` | `acme` |
| `UC_OAUTH_REALM` | `dev.celonis.cloud` | `dev.celonis.cloud` |
| `UC_OAUTH_HOST` | `{team}.{realm}` | `acme.dev.celonis.cloud` |

Traffic endpoints (derived from env):

- `http://{host}:9010` — integration tests and authorize/token calls
- `https://{host}` — UC issuer/JWKS discovery (Envoy TLS on :443)

Docker integration tests map `{UC_OAUTH_HOST}` to loopback via `jdk.net.hosts.file` in the test
JVM and `--add-host` on the UC container; `/etc/hosts` is not required for the docker test path.
Set `UC_OAUTH_RESOLVE_LOCAL=0` when targeting external Celonis OAuth (public DNS).

## 2. Start the OAuth stack

From the repository root:

```sh
docker compose -f docker/oidc/compose.yaml up -d
```

The first start can take several minutes while **team**, **authentication**, and **bootstrapper** initialize.

Wait until healthy:

```sh
docker compose -f docker/oidc/compose.yaml ps
```

Verify discovery (team context is resolved from the subdomain by gateway; default team `dev`):

```sh
source docker/oidc/resolve-oauth-tenant.sh
curl -sk --resolve "${UC_OAUTH_HOST}:443:127.0.0.1" \
  "https://${UC_OAUTH_HOST}/.well-known/openid-configuration" | jq .issuer
curl -s "http://${UC_OAUTH_HOST}:9010/.well-known/openid-configuration" | jq .issuer
```

- Direct HTTP (debugging only): http://localhost:9099
- OAuth client: `unity-catalog-local` / `unity-catalog-local-secret`
- Gateway realm: `dev.celonis.cloud` (fixed for this local stack; teams are dynamic via Host)

The stack runs:

- **postgres** — databases for oauth-server, team, and authentication
- **team** — team registry (subdomain → team id)
- **authentication** — session/auth service used by gateway
- **bootstrapper** — seeds teams (runs once per fresh volume)
- **gateway** — `cloud-gateway` ext_authz (injects `X-Celonis-Team-*` headers)
- **envoy** — HTTP/HTTPS entry point (:9010 / :443)
- **oauth-server** — `ghcr.io/celonis/cloud-oauth-server/oauth-server`

Configuration lives in `docker/oidc/oauth/application-local.yaml` and `docker/oidc/oauth/jwks.json`.

TLS certs for Envoy HTTPS: `docker/oidc/envoy/certs/` (regenerate with `docker/oidc/envoy/generate-certs.sh`; wildcard `*.dev.celonis.cloud`).

## 3. Configure Unity Catalog server

For docker-mode integration tests, `docker/start-uc-for-tests.sh` generates OIDC `server.properties`
from `UC_OAUTH_TEAM` / `UC_OAUTH_REALM` and imports `docker/oidc/envoy/certs/cert.pem` into a JVM
truststore for HTTPS issuer discovery.

## 4. Start UC and test

```sh
UC_SERVER_MODE=docker UC_DOCKER_IMAGE=... ./docker/start-uc-for-tests.sh
UC_SERVER_MODE=docker UC_DOCKER_IMAGE=... ./docker/tests/run-tests.sh
```

Another tenant (team must exist in team DB):

```sh
UC_OAUTH_TEAM=acme UC_SERVER_MODE=docker UC_DOCKER_IMAGE=... ./docker/tests/run-tests.sh
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

## Object storage (MinIO)

To run OIDC and S3-compatible catalog storage together:

```sh
docker compose -f docker/compose.yaml -f docker/oidc/compose.yaml up -d
```

See [../minio/README.md](../minio/README.md) for MinIO `server.properties` settings.

## Stop

```sh
docker compose -f docker/oidc/compose.yaml down
```

To stop MinIO as well:

```sh
docker compose -f docker/compose.yaml down
```

## Troubleshooting

- **Port 9010 or 9099 in use** — stop the other stack or change the host port mapping.
- **Issuer / JWKS errors** — ensure OAuth stack is healthy, bootstrapper completed, and `UC_OAUTH_HOST` matches a seeded team (`docker compose -f docker/oidc/compose.yaml logs bootstrapper`).
- **403 from Envoy** — gateway could not resolve the team; check team DB has `cpm_domain = UC_OAUTH_TEAM` and requests use hostname `{team}.{realm}` (not `localhost`).
- **CLI login redirect fails** — the `unity-catalog-local` client allows `http://127.0.0.1:8080/callback` and `http://localhost:8080/callback`.
- **Token exchange fails** — ensure `server.allowed-issuers` matches `UC_OAUTH_ALLOWED_ISSUERS` (default `https://*.{realm}`) and authorize/token URLs use `UC_OAUTH_BASE_URL`.
- **Integration tests** — start the OAuth stack first, then run `docker/tests/run-tests.sh` with `UC_SERVER_MODE=docker`.
