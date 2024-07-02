FROM eclipse-temurin:17-jdk-jammy AS build_uc


ARG unitycatalog_uid=185
ARG unitycatalog_home="/opt/unitycatalog"

RUN set -ex; \
    apt-get update; \
    apt-get install -y bash git; \
    # Remove temporary files to reduce image size
    rm -rf /var/lib/apt/lists/*


# COPY ../../ /opt/unitycatalog
# COPY build_uc.sh /opt/unitycatalog/build_uc.sh


WORKDIR /opt/unitycatalog
COPY <<"EOF" /opt/unitycatalog/build_uc.sh
#!/usr/bin/env bash
set -ex;

REPO_DIR="${unitycatalog_home}/repo"
REPO_URL="https://github.com/unitycatalog/unitycatalog.git"


[ ! -d $REPO_DIR ] && mkdir -p $REPO_DIR

cd $REPO_DIR

if [ -d ".git" ]; then
    echo "Repository already cloned. Pulling latest changes..."
    git pull
else
    echo "Cloning repository..."
    git clone $REPO_URL .
fi


build/sbt -v -d -J-Xmx5G assembly

EOF

RUN <<EOF 
#!/usr/bin/env bash
set -ex;
pwd
chmod +x build_uc.sh
. build_uc.sh
EOF

CMD ["echo", "Hello"]
