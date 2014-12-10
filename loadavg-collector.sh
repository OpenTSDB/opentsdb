#!/bin/bash
set -e
while true; do
  oneminute=`sudo loads.d | awk '/./ { printf "%.2f %.2f %.2f\n", $7, $8, $9 }' | cut -d " " -f 1`
  fiveminute=`sudo loads.d | awk '/./ { printf "%.2f %.2f %.2f\n", $7, $8, $9 }' | cut -d " " -f 2`
  awk -v now=`date +%s` -v host=`hostname` -v oneminute="$oneminute" -v fiveminute="$fiveminute" \
  'BEGIN { print "put proc.loadavg.1m " now " " oneminute " host=" host;
    print "put proc.loadavg.5m " now " " fiveminute " host=" host }' 
  sleep 1
done | nc -w 30 localhost 4242
