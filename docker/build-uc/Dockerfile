FROM eclipse-temurin:17-jdk-alpine AS build_uc

# Install any required packages
RUN <<EOF
    set -ex;
    apk update;
    apk upgrade; 
    apk add bash git;
EOF

# Define the shell used within the container
SHELL ["/bin/bash", "-i", "-c", "-o", "pipefail"] 

ARG unitycatalog_home="/opt/unitycatalog"
ARG unitycatalog_repo="${unitycatalog_home}/repo"
ARG unitycatalog_github="https://github.com/unitycatalog/unitycatalog.git"
# Specify which version to pull ex.: 0.1.0
ARG unitycatalog_version="main"
# Specify any custom parameters necessary to generate
# the Uber-Jar by SBT. 
# Note: The default allocated heam memory size is too small
# and will cause the process to fail when attempting to compile
# and generate the Uber-Jar. Therefore it is important to choose
# a size large enough for the compiler to run. 
ARG sbt_args="-J-Xmx5G"


# Create temporary directories for initialization files
RUN <<-EOF 
    set -ex;
    mkdir -p "${unitycatalog_repo}";
    cd "${unitycatalog_repo}";
    git clone ${unitycatalog_github} --branch ${unitycatalog_version} --single-branch .;
EOF

WORKDIR "${unitycatalog_repo}"

RUN <<-EOF
    #!/usr/bin/env bash
    set -ex;
    build/sbt ${sbt_args} assembly
EOF

ENTRYPOINT ["/usr/bin/env", "bash"]
