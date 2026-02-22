# syntax=docker.io/docker/dockerfile:1.7-labs
# Single definition for app home (used in build and runtime stages)
ARG HOME="/opt/unitycatalog"
ARG ALPINE_VERSION="3.20"
ARG VERSION=""

# --- deps: resolve dependencies for faster rebuilds when only code changes
FROM amazoncorretto:17-alpine${ALPINE_VERSION}-jdk AS deps
ARG HOME
ENV HOME=$HOME
WORKDIR $HOME
COPY --parents build/ project/ version.sbt build.sbt ./
RUN apk add --no-cache bash && ./build/sbt -info update

# --- builder: create distribution tarball and extract to dist/
FROM amazoncorretto:17-alpine${ALPINE_VERSION}-jdk AS builder
ARG HOME
ENV HOME=$HOME
WORKDIR $HOME
COPY --from=deps $HOME/.cache $HOME/.cache
COPY --parents dev/ build/ project/ examples/ server/ api/ clients/ bin/ etc/ version.sbt build.sbt ./

RUN apk add --no-cache bash && ./build/sbt -info clean createTarball

RUN mkdir -p $HOME/dist && tar -xzf target/unitycatalog-*.tar.gz -C $HOME/dist

# Classpath order matters (e.g. ANTLR). Generate @args from tarball classpath instead of using jars/*
RUN cd $HOME/dist/jars && \
    { echo '-cp'; awk -F: '{for(i=1;i<=NF;i++){n=split($i,a,"/"); printf "%s/opt/unitycatalog/jars/%s", (i>1?":":""), a[n];} print ""}' classpath; echo 'io.unitycatalog.server.UnityCatalogServer'; } > args

# --- runtime (Alpine): full image with shell and HEALTHCHECK — docker build --target runtime
FROM alpine:${ALPINE_VERSION} AS runtime
ARG JAVA_HOME="/usr/lib/jvm/default-jvm"
ARG USER="unitycatalog"
ARG HOME
ARG VERSION

COPY --from=builder $JAVA_HOME $JAVA_HOME
ENV HOME=$HOME JAVA_HOME=$JAVA_HOME PATH="${JAVA_HOME}/bin:${PATH}"
COPY --from=builder $HOME/dist/ $HOME/

LABEL org.opencontainers.image.title="Unity Catalog" \
      org.opencontainers.image.version="${VERSION}"

RUN <<EOF
apk add --no-cache bash wget
addgroup -S $USER && adduser -S -G $USER $USER
chmod -R 550 $HOME && mkdir -p $HOME/etc && chmod -R 770 $HOME/etc
chown -R $USER:$USER $HOME
EOF

USER $USER
WORKDIR $HOME
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1:8080/ || exit 1
CMD ["./bin/start-uc-server"]

# --- distroless (default): minimal image, no shell — docker build
FROM gcr.io/distroless/java17-debian12:nonroot AS distroless
ARG HOME
ARG VERSION

USER nonroot:nonroot
WORKDIR $HOME

COPY --from=builder --chown=nonroot:nonroot $HOME/dist/etc $HOME/etc
COPY --from=builder --chown=nonroot:nonroot $HOME/dist/jars $HOME/jars

LABEL org.opencontainers.image.title="Unity Catalog" \
      org.opencontainers.image.version="${VERSION}"

EXPOSE 8080
ENTRYPOINT ["java", "@/opt/unitycatalog/jars/args"]