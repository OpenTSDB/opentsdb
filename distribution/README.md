       ___                 _____ ____  ____  ____
      / _ \ _ __   ___ _ _|_   _/ ___||  _ \| __ )
     | | | | '_ \ / _ \ '_ \| | \___ \| | | |  _ \
     | |_| | |_) |  __/ | | | |  ___) | |_| | |_) |
      \___/| .__/ \___|_| |_|_| |____/|____/|____/
           |_|    The modern time series database.

# ***** NOTICE *****
Version 3 of OpenTSDB is currently in a development state. APIs and data
structures are expected to change before moving to the "put" branch where
development should stabilize. But please take a look, contribute and let
us know what you think.

## Distribution

This module is responsible for building distribution packages. So far what we have working are:

* Tarball - created by running "mvn package". In the target director you'll see an ``opentsdb-<VERSION>.tar.gz`` along with a directory. The directory has the same contents of the tarball and these can be copied to a location and executed via ``bin/tsdb tsd --config=conf/opentsdb.conf``.
* Docker - created by running "mvn package -Pdocker". This will, assuming you have docker installed, run the Docker file in ``src/resources/docker`` to copy the tarball into a docker image loaded on your machine that you can then post somewhere for execution. To run the docker execute ``docker run -p 4242:4242 opentsdb``. **Note:** We could use some help configuring docker correctly. So far there are a few environment variables that control the config locations (see the Dockerfile).
