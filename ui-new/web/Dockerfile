# syntax=docker/dockerfile:1.7

# Built from the ui-new directory: the TypeScript clients are generated from
# ../proto, which a checkout does not contain pre-built (web/src/gen is ignored).
FROM oven/bun:1.3.14-slim AS build

# `bun run generate` runs the buf that bun.lock pins, which fetches the
# protovalidate module from the Buf Schema Registry over TLS.
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG NPM_REGISTRY_URL

WORKDIR /app/web

COPY web/package.json web/bun.lock ./
RUN if [ -n "$NPM_REGISTRY_URL" ]; then \
        bun install --frozen-lockfile --registry "$NPM_REGISTRY_URL"; \
    else \
        bun install --frozen-lockfile; \
    fi

COPY buf.yaml buf.lock buf.gen.yaml /app/
COPY proto /app/proto
COPY web ./
RUN bun run generate && bun run build

FROM nginxinc/nginx-unprivileged:1.30-alpine AS runtime

COPY web/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/web/dist /usr/share/nginx/html

EXPOSE 8080
