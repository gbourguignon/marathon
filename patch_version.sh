#!/bin/bash

# this script will patch version.sbt to put the right version if we are building a tag

if [[ ! -z "$TRAVIS_TAG" ]]; then
  echo "version in ThisBuild := \"$TRAVIS_TAG\"" > version.sbt
fi
