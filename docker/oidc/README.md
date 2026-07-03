# Local OIDC for Unity Catalog

Spin up the [Celonis cloud-oauth-server](https://github.com/celonis/cloud-oauth-server) for testing Unity Catalog authentication locally.

Ports: **9010** (HTTP Envoy for tests) and **443** (HTTPS Envoy for UC JWKS/issuer discovery). Direct oauth-server HTTP **9099** is exposed for debugging only.

## 1. Hostname

OAuth traffic uses:

- `http://dev.dev.celonis.cloud:9010` — integration tests and authorize/token calls
- `https://dev.dev.celonis.cloud` — UC issuer/JWKS discovery (Envoy TLS on :443)

Add this to `/etc/hosts`:

```sh
127.0.0.1 dev.dev.celonis.cloud
```

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

Verify discovery (team context is resolved from the subdomain by gateway):

```sh
curl -sk https://dev.dev.celonis.cloud/.well-known/openid-configuration | jq .issuer
curl -s http://dev.dev.celonis.cloud:9010/.well-known/openid-configuration | jq .issuer
```

- Discovery (UC): https://dev.dev.celonis.cloud/.well-known/openid-configuration
- Discovery/tests: http://dev.dev.celonis.cloud:9010/.well-known/openid-configuration
- Direct HTTP (debugging only): http://localhost:9099
- OAuth client: `unity-catalog-local` / `unity-catalog-local-secret`
- Local tenant: `dev` (from hostname `dev.dev.celonis.cloud`)

The stack runs:

- **postgres** — databases for oauth-server, team, and authentication
- **team** — team registry (subdomain → team id)
- **authentication** — session/auth service used by gateway
- **bootstrapper** — seeds the `dev` team (runs once per fresh volume)
- **gateway** — `cloud-gateway` ext_authz (injects `X-Celonis-Team-*` headers)
- **envoy** — HTTP/HTTPS entry point (:9010 / :443)
- **oauth-server** — `ghcr.io/celonis/cloud-oauth-server/oauth-server`

Configuration lives in `docker/oidc/oauth/application-local.yaml` and `docker/oidc/oauth/jwks.json`.

TLS certs for Envoy HTTPS: `docker/oidc/envoy/certs/` (regenerate with `docker/oidc/envoy/generate-certs.sh`).

## 3. Configure Unity Catalog server

Merge `docker/oidc/server.properties.docker.snippet` into `etc/conf/server.properties`, or copy the auth block manually.

For docker-mode integration tests, `docker/start-uc-for-tests.sh` imports `docker/oidc/envoy/certs/cert.pem` into a JVM truststore for HTTPS issuer discovery.

## 4. Start UC and test

```sh
build/sbt server/package   # if not already built
./docker/start-uc-for-tests.sh
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
- **Issuer / JWKS errors** — ensure `dev.dev.celonis.cloud` resolves to `127.0.0.1`, Envoy is healthy, and bootstrapper completed (`docker compose -f docker/oidc/compose.yaml logs bootstrapper`).
- **403 from Envoy** — gateway could not resolve the team; check team/bootstrapper logs and that requests use hostname `dev.dev.celonis.cloud` (not `localhost`).
- **CLI login redirect fails** — the `unity-catalog-local` client allows `http://127.0.0.1:8080/callback` and `http://localhost:8080/callback`.
- **Token exchange fails** — ensure `server.allowed-issuers` is `https://dev.dev.celonis.cloud` and authorize/token URLs point at `http://dev.dev.celonis.cloud:9010`.
- **Integration tests** — start the OAuth stack first, then run `docker/tests/run-tests.sh`. Tests reach OAuth via Envoy at `http://dev.dev.celonis.cloud:9010`; gateway injects team headers from the subdomain.
