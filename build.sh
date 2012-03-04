#!/usr/bin/env bash

# Set this to be GNU make.
MAKE=${MAKE-'make'}

set -xe
test -f configure || ./bootstrap
test -d build || mkdir build
cd build
test -f Makefile || ../configure "$@"
exec ${MAKE} "$@"
