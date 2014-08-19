#!/bin/sh
# Because !@#$%^ Java can't fucking do this without a bazillion lines of codes.
set -e
stdout=$1
shift
stderr=$1
shift
echo ""
echo "========================================================"
echo "$@"
echo "========================================================"
echo ""
exec nice gnuplot "$@" >"$stdout" 2>"$stderr"
