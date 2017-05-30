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

## Http Executor

This executor takes a query to the new 3.x TSD, converts it to the V2 query format [/api/query] (http://opentsdb.net/docs/build/html/api_http/query/index.html) and fires it off against an existing 2.x TSD instance (preferably to a load balancer instead). 

The most important executor config is the ``endpoint`` parameter that defines the base address and port of the TSD or load balancer to use. E.g. ``http://localhost:4244`` would query the local host over port 4244. 