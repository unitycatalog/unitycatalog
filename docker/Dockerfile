FROM eclipse-temurin:17-jdk-jammy

ENV DEBIAN_FRONTEND=noninteractive

RUN set -ex; \
    apt-get update; \
    apt-get install -y gnupg2 wget bash tini libc6 libpam-modules krb5-user libnss3 procps net-tools gosu libnss-wrapper git ca-certificates; \
    rm -rf /var/lib/apt/lists/*

SHELL ["/bin/bash", "-i", "-c", "-o", "pipefail"] 

ARG unitycatalog_uid=185
ARG unitycatalog_home="/opt/unitycatalog"
ARG user_basedir="${unitycatalog_home}/home"
ARG temp_dir="/_init_files"

ENV UNITYCATALOG_HOME=${unitycatalog_home}
ENV UNITYCATALOG_INIT_FILES=${temp_dir}

VOLUME  ${unitycatalog_home}

RUN groupadd --system --gid=${unitycatalog_uid} unitycatalog; \
    useradd --system --uid=${unitycatalog_uid} \
            --gid=unitycatalog unitycatalog \
            --create-home -b "$unitycatalog_home" -d "$user_basedir" \
            --shell "$(/usr/bin/env bash)";


RUN set -ex; \
    mkdir -p "$temp_dir"; \
    mkdir -p "$temp_dir"/custom_certs; \
    mkdir -p "$temp_dir"/scripts; \
    mkdir -p "$temp_dir"/home;

COPY ./custom_certs "$temp_dir"/custom_certs
COPY ./scripts "$temp_dir"/scripts

RUN set -ex; \
    chown -R unitycatalog:unitycatalog "$temp_dir"; \
    chown -R unitycatalog:unitycatalog "$unitycatalog_home"; \
    chmod +x -v "${temp_dir}"/scripts/*.sh;


RUN mkdir -p /etc/ssl/certs && \
    cp -rv "$temp_dir"/custom_certs/pem/* /etc/ssl/certs && \
    c_rehash

RUN /bin/bash -c 'counter=0; \
for cert in "$temp_dir"/custom_certs/pem/*.cer; do \
    echo "Adding cert: CustomCert$((++counter))"; \
    keytool -import -file "$cert" -alias "CustomCert$counter" -cacerts -storepass "changeit" -noprompt; \
done;'

USER unitycatalog

WORKDIR ${unitycatalog_home}

ENTRYPOINT exec "${UNITYCATALOG_INIT_FILES}/scripts/entrypoint.sh"
