# syntax=docker.io/docker/dockerfile:1.7-labs@sha256:b99fecfe00268a8b556fad7d9c37ee25d716ae08a5d7320e6d51c4dd83246894
ARG HOME="/home/unitycatalog"

# Build stage, using Amazon Corretto jdk 17 on alpine with arm64 support
FROM amazoncorretto:17-alpine3.20-jdk@sha256:c045f0537bc890f9e61924f33f35e9667f696b4f372dad4a73861a9396b5d0b5 AS base

# Dependencies are installed in $HOME/.cache by sbt
ARG HOME
ENV HOME=$HOME

# The JVM derives user.home from the passwd entry (/root for uid 0) and ignores
# HOME, so coursier would otherwise cache jars in /root/.cache. sbt bakes absolute
# jar paths into the classpath files, and the runtime stage copies only $HOME at
# the same path, so the cache must be under $HOME.
ENV COURSIER_CACHE=$HOME/.cache/coursier/v1

# Corporate Maven mirror for the sbt launcher / Ivy (see build/sbt).
# Pass at build time: --build-arg MAVEN_PROXY_URL=$MAVEN_PROXY_URL
ARG MAVEN_PROXY_URL
ENV MAVEN_PROXY_URL=${MAVEN_PROXY_URL}

# A Java runtime without the jmods, headers, and man pages, which only building
# against the JDK uses. It keeps every module: a missing one fails only on the
# code path that loads it, and that code includes drivers added at deploy time,
# such as the MySQL driver in helm/templates/server/deployment.yaml. cmp checks
# that TLS keeps the JDK's trust store.
RUN jlink --add-modules ALL-MODULE-PATH --no-man-pages --no-header-files \
    --generate-cds-archive --output /jre \
    && cmp $JAVA_HOME/lib/security/cacerts /jre/lib/security/cacerts

WORKDIR $HOME

COPY --parents dev/ build/ project/ examples/ server/ api/ clients/ version.sbt build.sbt ./

# Only the server and the CLI ship in the image. The root `package` would also
# resolve Spark, Delta, and Hadoop for the connectors.
RUN apk add --no-cache bash && ./build/sbt -info clean server/package cli/package

# Stage only what the launchers load, at its build path, because the classpath
# files hold absolute paths: the classpath files, their entries, and the source
# file bin/start-uc-server and bin/uc each check to detect a source tree. sbt,
# its caches, and the rest of the sources stay in this stage. The runtime stage
# copies only $HOME, so a path elsewhere fails the build here, not the server at
# startup.
RUN <<EOF
set -eo pipefail
{
  for classpath in $HOME/server/target/classpath $HOME/examples/cli/target/classpath; do
    echo $classpath
    tr ':' '\n' < $classpath
    echo
  done
  echo $HOME/server/src/main/java/io/unitycatalog/server/UnityCatalogServer.java
  echo $HOME/examples/cli/src/main/java/io/unitycatalog/cli/UnityCatalogCli.java
} | sort -u > /tmp/runtime-paths
outside=$(grep -v "^$HOME/" /tmp/runtime-paths || true)
if [ -n "$outside" ]; then
  echo "These classpath paths are outside $HOME, which is all the runtime stage copies:" >&2
  echo "$outside" >&2
  exit 1
fi
mkdir /staging
tar -cf - -T /tmp/runtime-paths | tar -xf - -C /staging
EOF

# Small runtime image
FROM alpine:3.20@sha256:a4f4213abb84c497377b8544c81b3564f313746700372ec4fe84653e4fb03805 AS runtime

# Same JAVA_HOME as Amazon Corretto, for images built on this one
ARG JAVA_HOME="/usr/lib/jvm/default-jvm"
ARG USER="unitycatalog"
ARG HOME

ENV HOME=$HOME \
    JAVA_HOME=$JAVA_HOME \
    PATH="${JAVA_HOME}/bin:${PATH}"

# bash runs the bin/ launchers. The service user exists before the COPYs below so
# they set owner and mode as they write: a recursive chown or chmod in a later
# layer would store every file a second time. The Helm chart expects uid 100 and
# gid 101 (its fsGroup and its chown of etc/conf).
RUN apk add --no-cache bash \
    && addgroup -S -g 101 $USER \
    && adduser -S -H -u 100 -h $HOME -G $USER $USER

COPY --from=base /jre $JAVA_HOME

# Build artifacts are read and execute only for the service user
COPY --from=base --chown=$USER:$USER --chmod=550 /staging$HOME/ $HOME/

USER $USER

# Copy remaining directories here for caching optimization. The server writes
# its database, data, and logs under etc/.
COPY --chown=$USER:$USER --parents bin/ etc/ $HOME/

WORKDIR $HOME

CMD ["./bin/start-uc-server"]
