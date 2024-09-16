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

ARG POSTGRES_JDBC_VERSION="postgresql-42.7.3"
ARG MYSQL_JDBC_VERSION="mysql-connector-j-9.0.0"

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
# Install JDBC drivers for Postgres and MySQL
RUN <<EOF
#!/bin/bash

# Exit on error, print commands
set -ex

JDBC_DRIVERS_ABS_PATH="${HOME}/.jdbc"
POSTGRES_JDBC_JAR_ABS_PATH="${JDBC_DRIVERS_ABS_PATH}/${POSTGRES_JDBC_VERSION}.jar"
MYSQL_JDBC_JAR_ABS_PATH="${JDBC_DRIVERS_ABS_PATH}/${MYSQL_JDBC_VERSION}.jar"

mkdir -p $JDBC_DRIVERS_ABS_PATH

# Download the postgres jdbc jar
curl -L -o $POSTGRES_JDBC_JAR_ABS_PATH \
    https://jdbc.postgresql.org/download/$POSTGRES_JDBC_VERSION.jar

# Download and untar the mysql jdbc jar
curl -L -o  $MYSQL_JDBC_VERSION.tar.gz \
    https://dev.mysql.com/get/Downloads/Connector-J/$MYSQL_JDBC_VERSION.tar.gz
tar -xf $MYSQL_JDBC_VERSION.tar.gz
mv $MYSQL_JDBC_VERSION/$MYSQL_JDBC_VERSION.jar $MYSQL_JDBC_JAR_ABS_PATH
rm -rf $MYSQL_JDBC_VERSION.tar.gz

# Append the jdbc jars to any classpath files
find $HOME -type f -name "classpath" \
    | xargs -I {} sh -c 'echo ":$1:$2" >> {}' sh "$POSTGRES_JDBC_JAR_ABS_PATH" "$MYSQL_JDBC_JAR_ABS_PATH"
EOF

# Create a service user with read and execute permissions and write permissions of the ./etc directory
RUN <<EOF
groupadd --system $USER
useradd --system --gid $USER $USER
chmod -R 550 $HOME
chmod -R 770 $HOME/etc/
chown -R $USER:$USER $HOME
EOF


USER $USER

WORKDIR $HOME

# TODO: Combine bin/ scripts into one entrypoint
