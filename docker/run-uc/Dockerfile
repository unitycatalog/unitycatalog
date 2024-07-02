FROM eclipse-temurin:17-jre-alpine AS run_uc

RUN <<EOF
    set -ex;
    apk update;
    apk upgrade; 
    apk add bash;
EOF

# Define the shell used within the container
SHELL ["/bin/bash", "-i", "-c", "-o", "pipefail"] 

ARG unitycatalog_uid=185
ARG unitycatalog_home="/opt/unitycatalog"
ARG unitycatalog_repo="${unitycatalog_home}/repo"
ARG unitycatalog_jar="server/target"
ARG unitycatalog_etc="etc"
ARG unitycatalog_bin="bin"

ENV SERVER_TARGET_DIR="${unitycatalog_home}/${unitycatalog_jar}"

# Create temporary directories for initialization files
RUN <<EOF 
    set -ex;
    mkdir -p "${unitycatalog_home}/${unitycatalog_jar}";
    mkdir -p "${unitycatalog_home}/${unitycatalog_etc}";
    mkdir -p "${unitycatalog_home}/${unitycatalog_bin}";
EOF

COPY --from=unitycatalogue_builder:0.1.0 "${unitycatalog_repo}/${unitycatalog_jar}/*.jar" "${unitycatalog_home}/${unitycatalog_jar}/"
COPY --from=unitycatalogue_builder:0.1.0 "${unitycatalog_repo}/${unitycatalog_etc}" "${unitycatalog_home}/${unitycatalog_etc}/"

WORKDIR "$unitycatalog_home"

ENV UC_SERVER_BIN="${unitycatalog_home}/${unitycatalog_bin}/start-uc-server"
COPY <<"EOF" "${UC_SERVER_BIN}"
#!/usr/bin/env bash

set -ex;

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

SERVER_JAR=$(find "$SERVER_TARGET_DIR" -name "unitycatalog-server*.jar" | head -n 1)
if [ -z "$SERVER_JAR" ]; then
    echo "Server JAR not found starting with 'unitycatalog-server*' in the target directory '$SERVER_TARGET_DIR'."
    exit 1
fi

relative_path_to_jar="${SERVER_JAR//"$ROOT_DIR/"/}"

SERVER_JAVA_COMMAND="java -jar $relative_path_to_jar"

$SERVER_JAVA_COMMAND $@ || exit

EOF

RUN chmod +x $UC_SERVER_BIN

ENTRYPOINT exec ${UC_SERVER_BIN}
