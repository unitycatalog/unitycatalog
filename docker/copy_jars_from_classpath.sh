#!/bin/bash

JARS_DIR="server/target/jars"
if [ "$1" != "" ]; then
  JARS_DIR="$1"
fi

echo "Creating $JARS_DIR directory"
mkdir -p "$JARS_DIR"

ONE_SINGLE_CLASSPATH_LINE=$(< server/target/classpath)

IFS=':' read -ra JARS <<< "$ONE_SINGLE_CLASSPATH_LINE"
for jar_file in "${JARS[@]}"
do
  # -R recursively copies two classes directories into one classes output directory
  # happily, -R works with files too
  cp -R "$jar_file" "$JARS_DIR"
done
