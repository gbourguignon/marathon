#!/bin/bash

# this script will patch version.sbt to put the right version if we are building a tag

if [[ ! -z "$TRAVIS_TAG" ]]; then
  VERSION=$(echo $TRAVIS_TAG | sed 's/^v//')
  echo "version in ThisBuild := \"$VERSION\"" > version.sbt
fi
