ARG unitycatalog_uid=185
ARG unitycatalog_home="/opt/unitycatalog"
ARG unitycatalog_repo="${unitycatalog_home}/repo"
ARG unitycatalog_jar="server/target"
ARG unitycatalog_jars="${unitycatalog_home}/jars"
ARG unitycatalog_etc="etc"
ARG unitycatalog_bin="bin"
ARG unitycatalog_user_name="unitycatalog"
ARG unitycatalog_user_home="home"
ARG unitycatalog_user_basedir="${unitycatalog_home}/${unitycatalog_user_home}"
# Specify any custom parameters necessary to generate
# the Uber-Jar by SBT. 
# Note: The default allocated heam memory size is too small
# and will cause the process to fail when attempting to compile
# and generate the Uber-Jar. Therefore it is important to choose
# a size large enough for the compiler to run. 
ARG sbt_args="-J-Xmx2G"
# FIXME Pass it from the outside
ARG unitycatalog_version="0.2.0-SNAPSHOT"
ARG jars_directory="server/target/jars"

FROM eclipse-temurin:22-jdk-alpine AS package_server

ARG unitycatalog_repo
ARG sbt_args
ARG jars_directory

# Install required packages
RUN <<EOF
    set -ex;
    apk update;
    apk upgrade; 
    apk add bash git;
    rm -R /var/cache/apk/*;
EOF

WORKDIR "${unitycatalog_repo}"

COPY . .

# Builds server/target/unitycatalog-server-0.2.0-SNAPSHOT.jar
RUN build/sbt ${sbt_args} server/package

# As a side effect all the required jars to build the UC server
# are listed in server/target/classpath that is generated at build time
# Copy the jar files into a single directory
RUN ./docker/copy_jars_from_classpath.sh ${jars_directory}

FROM eclipse-temurin:22-jre-alpine AS build_uc

ARG unitycatalog_uid
ARG unitycatalog_home
ARG unitycatalog_repo
ARG unitycatalog_jar
ARG unitycatalog_jars
ARG unitycatalog_etc
ARG unitycatalog_bin
ARG unitycatalog_user_name
ARG unitycatalog_user_home
ARG unitycatalog_user_basedir
ARG sbt_args
ARG unitycatalog_version
ARG jars_directory

EXPOSE 8081

RUN <<EOF
    set -ex;
    apk update;
    apk upgrade; 
    apk add bash;
    rm -R /var/cache/apk/*;
EOF

# Define the shell used within the container
SHELL ["/bin/bash", "-i", "-c", "-o", "pipefail"]

ENV UC_SERVER_BIN="${unitycatalog_home}/${unitycatalog_bin}/start-uc-server"

RUN <<-EOF
    set -ex;
    mkdir -p "${unitycatalog_jars}";
    mkdir -p "${unitycatalog_home}/${unitycatalog_etc}";
    mkdir -p "${unitycatalog_home}/${unitycatalog_bin}";
    mkdir -p "${unitycatalog_home}/${unitycatalog_user_home}";
EOF

# Create system group and user for Unity Catalog
# Ensure the user created has their HOME pointing to the volume
# created to persist user data and the sbt cached files that 
# are created as a result of compiling the unity catalog.
# This also ensures that the container can run independently from
# the storage, so we can have ephemeral docker instances with --rm
# and still be able to run the unity catalog each time without problems.
RUN <<-EOF
    #!/usr/bin/env bash
    set -ex;
    addgroup --system --gid "${unitycatalog_uid}" "${unitycatalog_user_name}";
    adduser --system --uid "${unitycatalog_uid}" \
            --ingroup "${unitycatalog_user_name}" \
            --home "${unitycatalog_user_basedir}" \
            --shell "$(/usr/bin/env bash)" \
            "${unitycatalog_user_name}";
EOF

# Define volume to persist Unity Catalog data
VOLUME  "${unitycatalog_home}"

WORKDIR "$unitycatalog_home"

COPY --from=package_server "${server_jar}" "${unitycatalog_jars}/"

# Copy the etc folder which contains the config files and the data folder
COPY --from=package_server "${unitycatalog_repo}/${unitycatalog_etc}" "${unitycatalog_home}/${unitycatalog_etc}/"

COPY --from=package_server "${unitycatalog_repo}/${jars_directory}" "${unitycatalog_jars}/"

# Create the script that executes the server
# FIXME It could be already created and simply copied over
COPY <<-"EOF" "${UC_SERVER_BIN}"
    #!/usr/bin/env bash

    SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
    ROOT_DIR="$(dirname "$SCRIPT_DIR")"

    SERVER_CLASS_NAME="io.unitycatalog.server.UnityCatalogServer"
    # A classpath entry consisting simply of * expands to a list of all the jar files in the current directory.
    # A class path entry that contains * will not match class files.
    SERVER_JAVA_COMMAND="java -cp "jars/classes:jars/*" ${SERVER_CLASS_NAME} $@"

    cd ${ROOT_DIR}

    exec ${SERVER_JAVA_COMMAND}
EOF

# Set ownership of directories and Unity Catalog home directory to a less
# priviledged user
RUN <<-"EOF"
    #!/usr/bin/env bash
    set -ex;
    chown -R "${unitycatalog_user_name}":"${unitycatalog_user_name}" "$unitycatalog_home";
    chmod u+x "$UC_SERVER_BIN";
EOF

USER "${unitycatalog_user_name}"

ENTRYPOINT ["/bin/bash", "bin/start-uc-server"]
