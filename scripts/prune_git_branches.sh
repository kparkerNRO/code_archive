#!/bin/bash

# Check if repository directory is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <repo-directory>"
  exit 1
fi

# Navigate to the repository directory
cd "$1"

# Check if the directory change was successful
if [ $? -ne 0 ]; then
  echo "Error: Failed to navigate to directory $1"
  exit 1
fi

# Fetch the latest updates from origin and prune
git fetch --prune

echo "scanning local branches"
git for-each-ref --format="%(refname:short)" refs/heads | \
while read branch
do
    upstream=$(git rev-parse --symbolic-full-name ${branch}@{upstream} 2>/dev/null)
    if [[ $? == 0 ]]; then
        echo "     $branch tracks $upstream"
    else
        echo "$branch has no upstream configured - deleting"
        git branch -D $branch
    fi
done

echo "done processing"