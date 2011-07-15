#!/bin/bash
set -xe
test -f configure || ./bootstrap
test -d build || mkdir build
cd build
test -f Makefile || ../configure "$@"
exec make "$@"
