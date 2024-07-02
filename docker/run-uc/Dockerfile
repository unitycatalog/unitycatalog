FROM eclipse-temurin:22-jre-alpine AS run_uc

RUN <<EOF
    set -ex;
    apk update;
    apk upgrade; 
    apk add bash;
    rm -R /var/cache/apk/*;
EOF

# Define the shell used within the container
SHELL ["/bin/bash", "-i", "-c", "-o", "pipefail"] 

ARG unitycatalog_uid=185
ARG unitycatalog_home="/opt/unitycatalog"
ARG unitycatalog_repo="${unitycatalog_home}/repo"
ARG unitycatalog_jar="server/target"
ARG unitycatalog_etc="etc"
ARG unitycatalog_bin="bin"
ARG unitycatalog_user_name="unitycatalog"
ARG unitycatalog_user_home="home"
ARG unitycatalog_user_basedir="${unitycatalog_home}/${unitycatalog_user_home}"

ENV SERVER_TARGET_DIR="${unitycatalog_home}/${unitycatalog_jar}"
ENV UC_SERVER_BIN="${unitycatalog_home}/${unitycatalog_bin}/start-uc-server"

# Create the UC directories
RUN <<-EOF 
    set -ex;
    mkdir -p "${unitycatalog_home}";
    mkdir -p "${unitycatalog_home}/${unitycatalog_jar}";
    mkdir -p "${unitycatalog_home}/${unitycatalog_etc}";
    mkdir -p "${unitycatalog_home}/${unitycatalog_bin}";
    mkdir -p "${unitycatalog_home}/${unitycatalog_user_home}";
EOF


# Create system group and user for Unity Catalog
# ENsure the user created has their HOME pointing to the volume
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
VOLUME  ${unitycatalog_home}

WORKDIR "$unitycatalog_home"

# Copy the Uber-Jar from the previous stage
COPY --from=unitycatalog_jar:0.1.0 "${unitycatalog_repo}/${unitycatalog_jar}/*.jar" "${unitycatalog_home}/${unitycatalog_jar}/"

# Copy the etc folder which contains the config files and the data folder
COPY --from=unitycatalog_jar:0.1.0 "${unitycatalog_repo}/${unitycatalog_etc}" "${unitycatalog_home}/${unitycatalog_etc}/"

# Create the script that executes the server
# The script looks for the JAR automatially and saves its
# path into a variable then runs the server.
COPY <<-"EOF" "${UC_SERVER_BIN}"
    #!/usr/bin/env bash
    set -ex;

    SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
    ROOT_DIR="$(dirname "$SCRIPT_DIR")"

    # Find the jar and store its absolute path into a variable
    SERVER_JAR=$(find "$SERVER_TARGET_DIR" -name "unitycatalog-server*.jar" | head -n 1)
    if [ -z "$SERVER_JAR" ]; then
        echo "Server JAR not found starting with 'unitycatalog-server*' in the target directory '$SERVER_TARGET_DIR'."
        exit 1
    fi

    # Create relative path to jar so that it is able to find the 
    # configuration files in etc/conf/...
    relative_path_to_jar="${SERVER_JAR//"$ROOT_DIR/"/}"

    # Prepare the command to be executed
    SERVER_JAVA_COMMAND="java -jar $relative_path_to_jar"

    # Execute the jar / ie Run the server
    $SERVER_JAVA_COMMAND $@ || exit
EOF


# Set ownership of directories and Unity Catalog home directory to a less
# priviledged user
RUN <<-"EOF"
    #!/usr/bin/env bash
    set -ex;
    chown -R "${unitycatalog_user_name}":"${unitycatalog_user_name}" "$unitycatalog_home";
    chmod u+x "$UC_SERVER_BIN";
EOF


# Set the user for the container process
USER "${unitycatalog_user_name}"

ENTRYPOINT exec ${UC_SERVER_BIN}
