#!/bin/sh
# Generates opentsdb_build.properties
# Usage: gen_build_data.sh path/to/opentsdb_build.properties
# Author: Benoit Sigoure (tsuna@stumbleupon.com)

# Fail entirely if any command fails.
set -e

DST=$1

fatal() {
  echo >&2 "$0: error: $*."
  exit 1
}

[ -n "$DST" ] || fatal 'missing destination path'

echo "Generating $DST"
# Make sure the directory where we'll put `$DST' exists.
dir=`dirname "$DST"`
mkdir -p "$dir"

TZ=UTC
export TZ
# Can't use the system `date' tool because it's not portable.
sh=`python <<EOF
import time
t = time.time();
print "timestamp=%d" % t;
print "date=%r" % time.strftime("%Y/%m/%d %T %z", time.gmtime(t))
EOF`
eval "$sh"  # Sets the timestamp and date variables.

user=`whoami`
host=`hostname`
repo=`pwd`

sh=`git rev-list --pretty=format:%h HEAD --max-count=1 \
    | sed '1s/commit /full_rev=/;2s/^/short_rev=/'`
eval "$sh"  # Sets the full_rev & short_rev variables.

is_mint_repo() {
  git rev-parse --verify HEAD >/dev/null &&
  git update-index --refresh >/dev/null &&
  git diff-files --quiet &&
  git diff-index --cached --quiet HEAD
}

if is_mint_repo; then
  repo_status='MINT'
else
  repo_status='MODIFIED'
fi

cat >"$DST" <<EOF
short_revision = $short_rev
full_revision = $full_rev
date = $date
timestamp = $timestamp
repo_status = $repo_status
user = $user
host = $host
repo = $repo
EOF
