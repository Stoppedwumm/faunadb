#!/bin/bash

set -xe

# Install node modules if missing
if [ ! -d "node_modules" ]; then
  npm install --silent
fi

# Install Fauna CLI in the global context
npm install -g --silent fauna-shell@^2.0.0
