#!/bin/bash

if [[ (-z "$1") || (-z "$2") ]]; then
    echo "Usage: bin/release.sh release_version next_version"
    echo "Versions are required in the format x.y.z"
    exit 1
fi

RELEASE_VERSION=$1
NEXT_VERSION="${2}-SNAPSHOT"

if [[ $(git diff --stat) != '' ]]; then
    echo "[!] Commit local changes first before releasing"
#    exit 1
fi

SBT_VERSION=$(grep -o 'version := "[^"]*"' build.sbt | grep -o '"[^"]*"' | tr -d '"')

if [[ $(echo "$SBT_VERSION" | wc -l) -ne 1 ]]; then
    echo "Cannot determine version from build.sbt: "$SBT_VERSION
    exit 1
fi

echo "Current version in build.sbt: $SBT_VERSION"
echo "Version to be released: $RELEASE_VERSION"
echo "New version: $NEXT_VERSION"

echo "Updating build.sbt to $RELEASE_VERSION"
sed -i "s/\(\bversion := \)\"${SBT_VERSION}\"/\1\"${RELEASE_VERSION}\"/" build.sbt

echo "Update Changelog"
sed -i "s/## Unreleased/## ${RELEASE_VERSION} ($(date -Idate))/" Changelog.md

echo "Committing and tagging release"
git add build.sbt Changelog.md
git commit -v -q -m "Release v${RELEASE_VERSION}"
git tag -a v${RELEASE_VERSION} -m "Release v${RELEASE_VERSION}"

echo "Updating build.sbt to $NEXT_VERSION"
sed -i "s/\(\bversion := \)\"$RELEASE_VERSION\"/\1\"${NEXT_VERSION}\"/" build.sbt
sed -i "1s/^/## Unreleased\n\n/" Changelog.md
git add build.sbt Changelog.md
git commit -v -q -m "Starting v${NEXT_VERSION}"
git tag -a v${NEXT_VERSION} -m "Start snapshot for v${NEXT_VERSION}"

# push commits & tags
git push --follow-tags

echo "Release was successful"
