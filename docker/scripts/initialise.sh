#!/usr/bin/env bash

# Define the paths
UNITYCATALOG_INIT_FILES="${UNITYCATALOG_INIT_FILES:-/opt/unitycatalog/_init_files}"
INITIALISED_FILE_DIR="$HOME"
INITIALISED_FILE_PREFIX=".initialised_"

# Generate the initialised file name with the current timestamp
INITIALISED_FILE="${INITIALISED_FILE_DIR}/${INITIALISED_FILE_PREFIX}${EPOCHSECONDS}"

# Function to copy files if not initialised
initialise() {
    echo "Initialising..."

    ##### PUT ANY ONE-TIME INITIALISATION CODE HERE #####
    ## > do_something
    #####################################################
    
    touch "${INITIALISED_FILE}"
    echo "Initialised at $(date)"
}

# Check for the existence of the initialised file
if compgen -G "${INITIALISED_FILE_DIR}/${INITIALISED_FILE_PREFIX}*" > /dev/null; then
    # Get the existing timestamp
    EXISTING_FILE=$(ls ${INITIALISED_FILE_DIR}/${INITIALISED_FILE_PREFIX}* | head -n 1)
    TIMESTAMP=$(basename "${EXISTING_FILE}" | sed "s/${INITIALISED_FILE_PREFIX}//")
    echo "Already initialised on $(date -d @${TIMESTAMP})"
else
    initialise
fi
