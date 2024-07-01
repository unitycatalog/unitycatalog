#!/usr/bin/env bash
set -ex;

# Call the initialise script
${UNITYCATALOG_INIT_FILES}/scripts/initialise.sh


REPO_DIR="${UNITYCATALOG_HOME}/repo"
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

# Start the server
exec bin/start-uc-server