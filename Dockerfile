# syntax=docker.io/docker/dockerfile:1.7-labs
ARG HOME="/unitycatalog"

# Build stage
FROM eclipse-temurin:17-jdk-jammy AS base

# Dependencies are installed in $HOME/.cache by sbt
ARG HOME
ENV HOME=$HOME

WORKDIR $HOME

COPY --parents build/ project/ examples/ server/ api/ version.sbt build.sbt ./

RUN ./build/sbt -info clean package

# Runtime stage with JRE for reduced image size
FROM eclipse-temurin:17-jre-jammy AS main

ARG USER="unitycatalog"
ARG HOME
ENV HOME=$HOME

# Copy build artifacts from base stage
COPY --from=base --parents \
    $HOME/build/ \
    $HOME/project/ \
    $HOME/examples/ \
    $HOME/server/ \
    $HOME/api/ \
    $HOME/target/ \
    $HOME/.cache/ \
    /

# Create a service user with read and execute permissions and write permissions of the ./etc directory
RUN <<EOF
groupadd --system $USER
useradd --system --gid $USER $USER
chmod -R 550 $HOME
mkdir -p $HOME/etc/
chmod -R 770 $HOME/etc/
chown -R $USER:$USER $HOME
EOF

USER $USER

# Copy remaining directories here for caching optimization
COPY --chown=$USER:$USER --parents bin/ etc/ $HOME/

WORKDIR $HOME

CMD ["./bin/start-uc-server"]
