# syntax=docker.io/docker/dockerfile:1.7-labs
ARG HOME="/unitycatalog"

# Build stage
FROM eclipse-temurin:17-jdk-jammy AS base

# Dependencies are installed in $HOME/.cache by sbt
ARG HOME
ENV HOME=$HOME

WORKDIR $HOME

COPY --parents build/ project/ examples/ server/ api/ bin/ /etc /bin version.sbt build.sbt ./

RUN ./build/sbt -info clean package

# Runtime stage with JRE for reduced image size
FROM eclipse-temurin:17-jre-jammy AS main

ARG HOME

ENV HOME=$HOME \
    USER="unitycatalog"

COPY --from=base --parents \
    $HOME/bin/ \
    $HOME/etc/ \
    $HOME/examples/ \
    $HOME/server/ \
    $HOME/target/ \
    $HOME/.cache/ \
    /

# Create a service user with read and execute permissions and write permissions of the ./etc directory
RUN groupadd --system $USER \
    && useradd --system --gid $USER $USER \
    && chmod -R 550 $HOME \
    && chmod -R 770 $HOME/etc/ \
    && chown -R $USER:$USER $HOME

USER $USER

WORKDIR $HOME

# TODO: Combine bin/ scripts into one entrypoint
