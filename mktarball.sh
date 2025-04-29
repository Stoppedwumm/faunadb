#!/bin/bash

set -euo pipefail

# build faunadb.jar
export FAUNADB_RELEASE=true
sbt service/assembly

# copy resources
mkdir -p tarball/bin
mkdir -p tarball/lib
cp service/target/scala-2.13/faunadb.jar tarball/lib/
cp service/src/main/scripts/faunadb{,-admin,-backup-s3-upload} tarball/bin/

# make tarball
cd tarball
tar czf ../fauna-2025-XX.tar.gz ./*
rm -rf tarball
