#!/usr/bin/env bash

if [[ $# -eq 0 ]] ; then
    echo 'Usage: [git-url] [path]'
    exit 1
fi

# Create a temporary directory
directory="$2"
mkdir -p "$directory"
repo_name=$(basename "$1")
repo_id="${repo_name%.*}"
# Clone the repository
git clone --depth 1 "$1" "$directory" --no-checkout --branch=code

# Rename all files in .git/objects/pack to the basename of the repository, keeping the extension
for file in "$directory"/.git/objects/pack/*; do
    extension="${file##*.}"
    mv "$file" "$directory/.git/objects/pack/$repo_id.$extension"
done
