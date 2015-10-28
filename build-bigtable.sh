#!/usr/bin/env bash
set -xe
test -f configure || ./bootstrap
test -d build || mkdir build
cd build
test -f Makefile || ../configure --with-bigtable "$@"
MAKE=make
[ `uname -s` = "FreeBSD" ] && MAKE=gmake
exec ${MAKE} "$@"