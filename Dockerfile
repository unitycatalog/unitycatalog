# syntax=docker/dockerfile:1.7

# ---------------------------------------------------------------------------
# Build stage: full JDK (glibc) + sbt. The Coursier/ivy/sbt caches live only in
# BuildKit cache mounts and are never written into an image layer.
# ---------------------------------------------------------------------------
FROM eclipse-temurin:17-jdk-jammy@sha256:beabb759e6f9653c843958d1d1f5cecb881dfb85aa6081e2bef099ab1260344e AS build

WORKDIR /workspace

# Copy only what sbt needs to resolve dependencies and compile the server + CLI.
# Each directory keeps its name under /workspace (so build.sbt's module paths resolve).
COPY version.sbt build.sbt ./
COPY dev/ ./dev/
COPY build/ ./build/
COPY project/ ./project/
COPY examples/ ./examples/
COPY server/ ./server/
COPY api/ ./api/
COPY clients/ ./clients/
COPY bin/ ./bin/
COPY etc/ ./etc/

# Stage the relocatable distribution tree (jars/ bin/ etc/) into target/dist.
# The dependency caches are mounted, so they accelerate the build without being
# baked into any layer. `sharing=locked` serialises concurrent (multi-arch) builds
# that share the cache to avoid corruption.
RUN --mount=type=cache,target=/root/.cache/coursier,sharing=locked \
    --mount=type=cache,target=/root/.sbt,sharing=locked \
    --mount=type=cache,target=/root/.ivy2,sharing=locked \
    bash ./build/sbt -info clean stageDist

# ---------------------------------------------------------------------------
# Runtime stage (default): JRE (glibc, has a shell so the bash launchers work).
# Ships only the relocatable dist tree -- no JDK, no sbt, no dependency cache.
# ---------------------------------------------------------------------------
FROM eclipse-temurin:17-jre-jammy@sha256:47c73dc23524b031bed0a5030410c722af6a8b49d4b25898ea8f4615895065f0 AS runtime

ARG USER=unitycatalog
ARG HOME=/home/unitycatalog

LABEL org.opencontainers.image.title="Unity Catalog Server" \
      org.opencontainers.image.description="Unity Catalog server" \
      org.opencontainers.image.source="https://github.com/unitycatalog/unitycatalog" \
      org.opencontainers.image.licenses="Apache-2.0"

# Service account: numeric-friendly system user, no login shell.
RUN groupadd -r "$USER" \
 && useradd -r -g "$USER" -d "$HOME" -s /usr/sbin/nologin "$USER"

WORKDIR $HOME

# Copy only the relocatable distribution: flat jars + launchers + config.
COPY --from=build --chown=$USER:$USER /workspace/target/dist/jars ./jars
COPY --from=build --chown=$USER:$USER /workspace/target/dist/bin  ./bin
COPY --from=build --chown=$USER:$USER /workspace/target/dist/etc  ./etc

USER $USER

EXPOSE 8080

# No dedicated health endpoint exists; check that the server is accepting TCP
# connections on 8080. The JRE base ships bash, so /dev/tcp is available.
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/8080' || exit 1

ENTRYPOINT ["./bin/start-uc-server"]

# ---------------------------------------------------------------------------
# Distroless stage (production): distroless, glibc, no shell, non-root by default.
# Build with `--target distroless`. Because there is no shell, the bash launchers
# cannot run -- the distroless java entrypoint invokes `java` directly and we
# pass the (relocatable) classpath wildcard via CMD. Ships ONLY the server jars
# (no CLI -- it cannot be invoked here without a shell, and dropping it keeps the
# production image lean).
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/java17-debian13:nonroot@sha256:81d09cac6ec47f6a13c61a941557f95079213320f3ddbf9d353de9317669aab5 AS distroless

ARG HOME=/home/nonroot

LABEL org.opencontainers.image.title="Unity Catalog Server (distroless)" \
      org.opencontainers.image.description="Unity Catalog server, distroless production build" \
      org.opencontainers.image.source="https://github.com/unitycatalog/unitycatalog" \
      org.opencontainers.image.licenses="Apache-2.0"

WORKDIR $HOME

# Only the server jars + config -- no bin/ (no shell), no CLI, no JDK.
COPY --from=build --chown=nonroot:nonroot /workspace/target/dist/jars/server ./jars/server
COPY --from=build --chown=nonroot:nonroot /workspace/target/dist/etc         ./etc

USER nonroot

EXPOSE 8080

# The distroless java image's ENTRYPOINT is ["java"]; supply JVM flags + the
# main class via CMD. -Djava.io.tmpdir=/tmp keeps it working under a read-only
# root filesystem (mount a writable /tmp). No HEALTHCHECK: distroless has no
# shell -- rely on orchestrator liveness/readiness probes.
CMD ["-XX:MaxRAMPercentage=75.0", "-Djava.io.tmpdir=/tmp", "-cp", "jars/server/*", "io.unitycatalog.server.UnityCatalogServer"]
