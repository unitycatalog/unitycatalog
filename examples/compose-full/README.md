# Full Unity Catalog Deployment

A Docker Compose setup that deploys Unity Catalog with all components running as
separate services:

- **Envoy** -- reverse proxy handling OIDC authentication, routing, and
  distributed tracing
- **Unity Catalog Server** -- distroless image connected to PostgreSQL, using
  trusted-header authentication from Envoy
- **Unity Catalog UI** -- React frontend
- **PostgreSQL** -- persistent catalog metadata store
- **Jaeger** -- distributed tracing backend (receives OpenTelemetry traces from
  Envoy)

```
                  ┌────────────────┐
  Client :8080 ──▶│  Envoy Proxy   │
                  └───────┬────────┘
              /api/ │ /jaeger │ /* (else)
            ┌───────┼────────┼────────┐
            ▼       │        ▼        ▼
     ┌────────────┐ │ ┌────────────┐ ┌────────────┐
     │  UC Server │ │ │   Jaeger   │ │   UC UI    │
     └──────┬─────┘ │ └────────────┘ └────────────┘
            ▼       │     ▲ OTLP
     ┌────────────┐ └─────┘
     │ PostgreSQL │
     └────────────┘
```

## Prerequisites

1. Docker and Docker Compose v2
2. An OIDC provider (Okta, Azure Entra ID, Keycloak, etc.) with a registered
   application. See `../../etc/envoy/examples/` for provider-specific guidance.
3. The distroless server image built locally:

   ```bash
   # From the repository root:
   docker build -t unitycatalog/unitycatalog:local-distroless ../../
   ```

## Quick Start

1. **Configure OIDC credentials:**

   ```bash
   cp .env.example .env
   # Edit .env and fill in your OIDC provider values.
   ```

2. **Start the stack:**

   ```bash
   docker compose up
   ```

3. **Access the services:**

   | Service        | URL                            |
   | -------------- | ------------------------------ |
   | UI + API       | http://localhost:8080           |
   | Jaeger UI      | http://localhost:8080/jaeger    |
   | Envoy Admin    | http://localhost:9901           |

## Configuration

### OIDC

All OIDC variables are documented in `.env.example`. Copy one of the
provider-specific templates from `../../etc/envoy/examples/` as a starting
point.

### PostgreSQL

The default credentials (`uc_default_user` / `uc_default_password`) are shared
between `.env` (used by the `postgres` container) and `hibernate.properties`
(read by the UC server). Change both if you modify the credentials.

Data is persisted in a Docker named volume (`postgres_data`).

### Server

`server.properties` enables authorization with trusted-header authentication:

```properties
server.authorization=enable
server.auth-type=trusted-headers
```

The UC server is **not** directly exposed -- only Envoy is published on port
8080. This is required for security when using trusted-header authentication.

### Security

The Envoy proxy enforces several security best practices:

- **Header stripping** -- The `x-auth-user-email` and `x-jwt-payload` headers
  are stripped from every incoming request before any filter processing. This
  prevents clients from spoofing identity headers. Only the JWT authentication
  filter can set these headers after successful token validation.

- **CORS** -- A restrictive Cross-Origin Resource Sharing policy is configured
  on the virtual host, allowing requests only from the same origin
  (`http://localhost:8080`). Adjust `allow_origin_string_match` in the Envoy
  template if you need to allow programmatic API access from other origins
  (e.g. notebooks on a different port).

- **Security response headers** -- Every response includes:
  - `X-Content-Type-Options: nosniff` -- prevents MIME-type sniffing
  - `X-Frame-Options: SAMEORIGIN` -- prevents clickjacking
  - `Referrer-Policy: strict-origin-when-cross-origin` -- limits referrer leakage
  - `Permissions-Policy` -- disables camera, microphone, and geolocation APIs

- **No direct backend access** -- The UC server, UI, and Jaeger are only
  reachable through Envoy. The Envoy admin interface on port 9901 is the only
  other exposed port; restrict or disable it in production.

### Tracing

Envoy sends OpenTelemetry (OTLP/gRPC) traces to Jaeger on port 4317. All
HTTP requests flowing through the proxy are automatically traced. View them
in the Jaeger UI at http://localhost:8080/jaeger under the
`unity-catalog-envoy` service.

Jaeger is configured with `QUERY_BASE_PATH=/jaeger` so its UI is served
under the `/jaeger` path, and Envoy routes that prefix to the Jaeger
query service. No separate port is exposed for Jaeger -- all access flows
through Envoy on port 8080.
