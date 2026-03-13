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

# Classpath order matters (e.g. ANTLR). Rewrite classpath and args with container paths so both runtime (script) and distroless work.
RUN cd $HOME/dist/jars && \
    awk -F: -v home="$HOME" '{for(i=1;i<=NF;i++){n=split($i,a,"/"); printf "%s%s/jars/%s", (i>1?":":""), home, a[n];} print ""}' classpath > classpath.new && mv classpath.new classpath && \
    { echo '-cp'; cat classpath; echo 'io.unitycatalog.server.UnityCatalogServer'; } > args

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

# --- jdk-debian: glibc-compatible JDK for use in the Ubuntu-based envoy image
FROM eclipse-temurin:17-jre-jammy AS jdk-debian

# --- all-in-one: UC + Envoy in a single container — docker build --target all-in-one
#     The envoy base image sets ENV HOME=/home/envoy which shadows the global
#     ARG HOME, so we use literal paths here to avoid the collision.
FROM envoyproxy/envoy:v1.32-latest AS all-in-one
ARG VERSION

COPY --from=jdk-debian /opt/java/openjdk /opt/java/openjdk
ENV HOME=/opt/unitycatalog JAVA_HOME=/opt/java/openjdk PATH="/opt/java/openjdk/bin:${PATH}"
WORKDIR /opt/unitycatalog

COPY --from=builder /opt/unitycatalog/dist/ /opt/unitycatalog/
COPY bin/start-all-in-one /opt/unitycatalog/bin/start-all-in-one
COPY etc/envoy/ /opt/unitycatalog/etc/envoy/

RUN apt-get update && apt-get install -y --no-install-recommends wget gettext-base openssl && rm -rf /var/lib/apt/lists/* && \
    chmod +x /opt/unitycatalog/bin/start-all-in-one /opt/unitycatalog/bin/start-uc-server && \
    chown -R envoy:envoy /opt/unitycatalog

LABEL org.opencontainers.image.title="Unity Catalog (All-in-One with Envoy)" \
      org.opencontainers.image.version="${VERSION}"

EXPOSE 8080 9901
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1:8080/ || exit 1
CMD ["./bin/start-all-in-one"]