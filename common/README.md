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

## Common Module

The goal of this module is to provide a location for common utilities or objects that are shared with all of OpenTSDB's components. One of the most *critical* aspects of this module is that it does **not** pull in a lot of dependencies. 

Modules and reasoning behind including them are:

* **slf4j** - Logging helps debug a system and this is still the best abstraction for logging out there. Every component is expected to write to logs and should use this API.
* **guava** - Though many of Guava's code moved to Java 8 there are still a number of utilities that we use (the Cache, File utilities, object instantiations) that are common across all components.
* **async** - OpenTSDB is designed to use resources efficiently via asynchronous communication. We have some methods and exceptions in here useful for handling deferred errors and grouping callbacks. See the note below about *Why not use Futures?*
* **Netty** - This may move but for now Netty has a great timer class that we use throughout OpenTSDB.

## Why not use Futures?

Benoit and folks developed the Async library (``Deferred<?>``) at StumbleUpon based on the Python Twisted library in order to support callback chains with small overhead. Futures have come a long way and are much more composable thanks to Guava, but they still have a bit more overhead that Async so we'll stick with that for OpenTSDB 3.x. 