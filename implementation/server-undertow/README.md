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

## Undertow Server

Undertow is a stripped down J2EE server comparable to Jetty. We use it at Yahoo for a number of projects so we'll offer this as a TSDB host for providing the HTTP API.

The following parameters can be used in the ``opentsdb.yaml`` file to control the server's behavior.

* ``tsd.network.port`` - Copied from 2.x, controls the HTTP port the server will listen on. Required.
* ``tsd.network.ssl_port`` - If SSL is required, the port for HTTPS connections. Optional
* ``tsd.network.bind`` - What address to bind to, ``0.0.0.0`` by default, same as 2.x.
* ``tsd.http.root`` - The root URI path. Defaults to ``/`` same as 2.x.
* ``tsd.core.load_plugins`` - Whether or not to load plugins on boot. Defaults to ``true``. Note that if you set this to false, the TSD won't even work right now.
* ``tsd.network.keystore.location`` - If SSL is required, this must be the path to a keystore file. See the Java ``keytool`` documentation for importing certificates.
* ``tsd.network.keystore.password`` - The keystore password for SSL uses.