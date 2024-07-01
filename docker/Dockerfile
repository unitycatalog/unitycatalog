# Base image for the container - using Eclipse Temurin with OpenJDK 17 and Jammy base
# This is the same image used in the offical Spark dockerfile which was used as reference
# for this dockerfile
FROM eclipse-temurin:17-jdk-jammy

# Set environment variable to avoid interactive prompts during package installation
# especially with krb5-user
ENV DEBIAN_FRONTEND=noninteractive

# Update package lists and install required packages
# The most important are bash gosu libnss-wrapper git ca-certificates
# The rest might be removed in the future.
# hadolint suggests adding `--no-install-recommends` to avoid additional packages so
# it should be added in later versions.
RUN set -ex; \
    apt-get update; \
    apt-get install -y gnupg2 wget bash tini libc6 libpam-modules krb5-user libnss3 procps net-tools gosu libnss-wrapper git ca-certificates; \
    # Remove temporary files to reduce image size
    rm -rf /var/lib/apt/lists/*

# Define the shell used within the container
SHELL ["/bin/bash", "-i", "-c", "-o", "pipefail"] 

# Define arguments for user and directory configuration
ARG unitycatalog_uid=185
ARG unitycatalog_home="/opt/unitycatalog"
ARG user_basedir="${unitycatalog_home}/home"
ARG temp_dir="/_init_files"

# Set environment variables for paths
ENV UNITYCATALOG_HOME=${unitycatalog_home}
ENV UNITYCATALOG_INIT_FILES=${temp_dir}

# Define volume to persist Unity Catalog data
VOLUME  ${unitycatalog_home}

# Create system group and user for Unity Catalog
# ENsure the user created has their HOME pointing to the volume
# created to persist user data and the sbt cached files that 
# are created as a result of compiling the unity catalog.
# This also ensures that the container can run independently from
# the storage, so we can have ephemeral docker instances with --rm
# and still be able to run the unity catalog each time without problems.
RUN groupadd --system --gid=${unitycatalog_uid} unitycatalog; \
    useradd --system --uid=${unitycatalog_uid} \
            --gid=unitycatalog unitycatalog \
            --create-home -b "$unitycatalog_home" -d "$user_basedir" \
            --shell "$(/usr/bin/env bash)";

# Create temporary directories for initialization files
RUN set -ex; \
    mkdir -p "$temp_dir"; \
    mkdir -p "$temp_dir"/custom_certs; \
    mkdir -p "$temp_dir"/scripts; \
    mkdir -p "$temp_dir"/home;

# The next few steps are especially for people trying to run 
# unity catalogue in environments like corporates that use their
# own certificates, and that create problems to sbt or any application
# tryint to do SSL verification if the custom certificates are not
# imported into the trust store.

# Copy custom certificates to the temporary directory
COPY ./custom_certs "$temp_dir"/custom_certs

# Copy initialization scripts to the temporary directory
COPY ./scripts "$temp_dir"/scripts

# Set ownership of temporary directories and Unity Catalog home directory
RUN set -ex; \
    chown -R unitycatalog:unitycatalog "$temp_dir"; \
    chown -R unitycatalog:unitycatalog "$unitycatalog_home"; \
    chmod +x -v "${temp_dir}"/scripts/*.sh;

# Create directories for certificates and copy them to the trusted store
RUN mkdir -p /etc/ssl/certs && \
    cp -rv "$temp_dir"/custom_certs/pem/* /etc/ssl/certs && \
    c_rehash

# Add custom certificates to the keystore using a loop
RUN /bin/bash -c 'counter=0; \
for cert in "$temp_dir"/custom_certs/pem/*.cer; do \
    echo "Adding cert: CustomCert$((++counter))"; \
    keytool -import -file "$cert" -alias "CustomCert$counter" -cacerts -storepass "changeit" -noprompt; \
done;'

# Set the user for the container process
USER unitycatalog

# Set the working directory for the container process
WORKDIR ${unitycatalog_home}

# Define the entrypoint to execute the initialization script
ENTRYPOINT exec "${UNITYCATALOG_INIT_FILES}/scripts/entrypoint.sh"
