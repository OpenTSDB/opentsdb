#!/usr/bin/env bash

CACHE_DIR=/tmp/tsd

diskSpaceIsShort() {
   df -h "$CACHE_DIR" \
   | awk 'NR==2{pct=$5; sub(/%/, "", pct); if (pct < 90) exit 1; exit 0;}'
}

if diskSpaceIsShort; then
  ( cd ${CACHE_DIR} && find . -x -exec rm {} \; )
fi
