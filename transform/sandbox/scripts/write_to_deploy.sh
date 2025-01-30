#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <source_file> <destination_dir>"
    exit 1
fi

SOURCE_FILE="$1"
TARGET_DIR="$2"

cp "$SOURCE_FILE" "$TARGET_DIR/"
echo "Successfully copied $SOURCE_FILE to $TARGET_DIR."