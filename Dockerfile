# syntax=docker.io/docker/dockerfile:1.7-labs
ARG HOME="/home/unitycatalog"
ARG ALPINE_VERSION="3.20"
# Set at build time for image metadata, e.g. --build-arg VERSION=0.5.0
ARG VERSION=""

# Dependency cache: resolve and download deps so later stages reuse them (faster rebuilds)
FROM amazoncorretto:17-alpine${ALPINE_VERSION}-jdk AS deps
ARG HOME
ENV HOME=$HOME
WORKDIR $HOME
COPY --parents build/ project/ version.sbt build.sbt ./
RUN apk add --no-cache bash && ./build/sbt -info update

# Build stage: produce the official distribution tarball (bin + etc + jars + classpath)
FROM amazoncorretto:17-alpine${ALPINE_VERSION}-jdk AS builder
ARG HOME
ENV HOME=$HOME
WORKDIR $HOME
# Reuse Coursier/dependency cache from deps stage
COPY --from=deps $HOME/.cache $HOME/.cache
COPY --parents dev/ build/ project/ examples/ server/ api/ clients/ version.sbt build.sbt ./

# Build the full distribution tarball (server + CLI + client JARs and deps, bin scripts, etc)
RUN apk add --no-cache bash && \
    ./build/sbt -info clean createTarball

# Extract tarball so runtime stage only copies the dist layout (no source, no .cache, no full target/)
RUN mkdir -p $HOME/dist && \
    tar -xzf target/unitycatalog-*.tar.gz -C $HOME/dist

# Minimal runtime image: JVM + extracted tarball only
FROM alpine:${ALPINE_VERSION} AS runtime
ARG JAVA_HOME="/usr/lib/jvm/default-jvm"
ARG USER="unitycatalog"
ARG HOME
ARG VERSION

# JVM from builder
COPY --from=builder $JAVA_HOME $JAVA_HOME
ENV HOME=$HOME \
    JAVA_HOME=$JAVA_HOME \
    PATH="${JAVA_HOME}/bin:${PATH}"

# Production layout: only bin, etc, jars (from tarball) — no source trees or build artifacts
COPY --from=builder $HOME/dist/ $HOME/

# Optional version label for traceability (set at build: --build-arg VERSION=0.5.0)
LABEL org.opencontainers.image.title="Unity Catalog" \
      org.opencontainers.image.version="${VERSION}"

RUN <<EOF
apk add --no-cache bash wget
addgroup -S $USER
adduser -S -G $USER $USER
chmod -R 550 $HOME
mkdir -p $HOME/etc
chmod -R 770 $HOME/etc
chown -R $USER:$USER $HOME
EOF

USER $USER
WORKDIR $HOME

# Default server port (override with -p when running if needed)
EXPOSE 8080

CMD ["./bin/start-uc-server"]
