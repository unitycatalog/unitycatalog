# syntax=docker.io/docker/dockerfile:1.7-labs@sha256:b99fecfe00268a8b556fad7d9c37ee25d716ae08a5d7320e6d51c4dd83246894
ARG HOME="/home/unitycatalog"

# Build stage, using Amazon Corretto jdk 17 on alpine with arm64 support
FROM amazoncorretto:17-alpine3.20-jdk@sha256:c045f0537bc890f9e61924f33f35e9667f696b4f372dad4a73861a9396b5d0b5 AS base

# Dependencies are installed in $HOME/.cache by sbt
ARG HOME
ENV HOME=$HOME

# The JVM derives user.home from the passwd entry (/root for uid 0) and ignores
# HOME, so coursier would otherwise cache jars in /root/.cache, outside the tree
# the runtime stage copies. sbt bakes absolute jar paths into
# server/target/classpath, so this directory must resolve identically in both
# stages or the server starts with some classpath entries missing.
ENV COURSIER_CACHE=$HOME/.cache/coursier/v1

# Corporate Maven mirror for the sbt launcher / Ivy (see build/sbt).
# Pass at build time: --build-arg MAVEN_PROXY_URL=$MAVEN_PROXY_URL
ARG MAVEN_PROXY_URL
ENV MAVEN_PROXY_URL=${MAVEN_PROXY_URL}

# A Java runtime with every JDK module, so no code path loses one, but without the
# jmods, headers, and man pages that only building against the JDK uses.
RUN jlink --add-modules ALL-MODULE-PATH --no-man-pages --no-header-files \
    --generate-cds-archive --output /jre

WORKDIR $HOME

COPY --parents dev/ build/ project/ examples/ server/ api/ clients/ version.sbt build.sbt ./

# Only the server and the CLI ship in the image. The root `package` would also
# resolve Spark, Delta, and Hadoop for the connectors.
RUN apk add --no-cache bash && ./build/sbt -info clean server/package cli/package

# Stage only what the launchers load, at its build path, because the classpath
# files hold absolute paths: the classpath files, their entries, and the source
# file bin/start-uc-server and bin/uc each check to detect a source tree. sbt,
# its caches, and the rest of the sources stay in this stage.
RUN <<EOF
set -eo pipefail
mkdir /staging
{
  for classpath in $HOME/server/target/classpath $HOME/examples/cli/target/classpath; do
    echo $classpath
    tr ':' '\n' < $classpath
    echo
  done
  echo $HOME/server/src/main/java/io/unitycatalog/server/UnityCatalogServer.java
  echo $HOME/examples/cli/src/main/java/io/unitycatalog/cli/UnityCatalogCli.java
} | sort -u | tar -cf - -T - | tar -xf - -C /staging
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
# layer would store every file a second time.
RUN apk add --no-cache bash && addgroup -S $USER && adduser -S -H -h $HOME -G $USER $USER

COPY --from=base /jre $JAVA_HOME

# Build artifacts are read and execute only for the service user
COPY --from=base --chown=$USER:$USER --chmod=550 /staging$HOME/ $HOME/

USER $USER

# Copy remaining directories here for caching optimization. The server writes
# its database, data, and logs under etc/.
COPY --chown=$USER:$USER --parents bin/ etc/ $HOME/

WORKDIR $HOME

CMD ["./bin/start-uc-server"]
