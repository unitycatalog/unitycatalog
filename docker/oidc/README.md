# Local OIDC for Unity Catalog

Spin up an OpenID Connect provider for testing Unity Catalog authentication locally.

## Options

| Provider | Compose file | Best for |
|---|---|---|
| **Keycloak** (recommended) | `compose.yaml` | Interactive login, admin UI, CLI `auth login`, realistic OAuth flows |
| **mock-oauth2-server** | `compose.mock-oauth2.yaml` | Fast startup, scripted/token tests, minimal setup |

Both bind **port 9090** on the host. Do not run both at once.

## Keycloak (recommended)

### 1. Start Keycloak

From the repository root:

```sh
docker compose -f docker/oidc/compose.yaml up -d
```

Wait until healthy:

```sh
docker compose -f docker/oidc/compose.yaml ps
```

- Admin console: http://localhost:9090/admin (`admin` / `admin`)
- Realm: `unity-catalog`
- Pre-created user: `admin` / `admin` (email claim `admin`, matches UC bootstrap admin)

### 2. Configure Unity Catalog server

Merge `docker/oidc/server.properties.snippet` into `etc/conf/server.properties`, or copy the auth block manually.

### 3. Start UC and test

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

Interactive CLI login (opens browser → Keycloak):

```sh
bin/uc auth login
```

### 4. UI

The bundled UI login page targets **Google** and **Okta**. Keycloak UI wiring is still a stub, so for Keycloak-backed servers keep Google auth disabled in `ui/.env` (see `ui.env.snippet`). Use the CLI or API for authenticated testing.

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

- **Port 9090 in use** — stop the other OIDC compose stack or change the host port mapping.
- **CLI login redirect fails** — Keycloak client allows `http://localhost:*` redirects for the CLI callback.
- **Token exchange fails** — ensure `server.allowed-issuers` and `server.audiences` match the IdP issuer URL and client id (`unity-catalog-local`).
