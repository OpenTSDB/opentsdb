# Creates directory structure overlay on top of original source directories so
# that the overlay matches Java package hierarchy.
#!/usr/bin/env bash

if [ ! -d src-main ]; then
  mkdir src-main
  mkdir src-main/net
  mkdir src-main/tsd
  (cd src-main/net && ln -s ../../src opentsdb)
  (cd src-main/tsd && ln -s ../../src/tsd/QueryUi.gwt.xml QueryUi.gwt.xml)
  (cd src-main/tsd && ln -s ../../src/tsd/client client)
fi
if [ ! -d src-test ]; then
  mkdir src-test
  mkdir src-test/net
  (cd src-test/net && ln -s ../../test opentsdb)
fi
if [ ! -d src-resources ]; then
  mkdir src-resources
  (cd src-resources && ln -s ../fat-jar/logback.xml)
  (cd src-resources && ln -s ../fat-jar/file-logback.xml)
  (cd src-resources && ln -s ../fat-jar/opentsdb.conf.json)
fi
if [ ! -d test-resources ]; then
  mkdir test-resources
  (cd test-resources && ln -s ../fat-jar/test-logback.xml)
  (cd test-resources && ln -s ../fat-jar/opentsdb.conf.json)
fi

