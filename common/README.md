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
* **Jackson** - This library has great serdes support that is used by the configuration components.

## Why not use Futures?

Benoit and folks developed the Async library (``Deferred<?>``) at StumbleUpon based on the Python Twisted library in order to support callback chains with small overhead. Futures have come a long way and are much more composable thanks to Guava, but they still have a bit more overhead that Async so we'll stick with that for OpenTSDB 3.x. 

## Configuration

This module provides a configuration for OpenTSDB to bring it into the containerized present. It allows for multiple sources, including remote and local providers, to be merged into a single configuration state. Features include:

* Ordered and merged view of configuration providers. For example, you can initialize the library to pull first from a remote repository, then read a local file and override the remote settings with those from the local and then read the environment variables for more overrides. This allows for a central config store with global settings and local overrides for specific settings.
* Reloadable providers so updates can be given to the implementation without having to restart the process.
* Key binding so that the application can receive updates whenever a setting has changed to avoid polling the config library periodically.
* Typed schema to handle validation and provide useful details to users of the application by printing out debug information about what types the setting supports and describing what it's supposed to do.
* Store complex config objects (maps, lists, POJOs) as well as primitives.
* Obfuscation of secret values in logs and API calls.

While the configuration is still a key-value store it is a big improvement over the 2.x OpenTSDB config.

### Usage

An example usage appears below:

```java
public static void main(final String[] args) throws Exception {
    // New config instance
    final Configuration config = new Configuration(args);
    
    // Register a key with a schema. Required before access. 
    config.registerSchema(ConfigurationEntrySchema.newBuilder()
        .setKey("my.key")
        .isDynamic()
        .setType(String.class)
        .setDefaultValue("Fjnords!")
        .setSource("Main")
        );
    System.out.println(config.getString("my.key"));
    
    // Binding for dynamic variables.
    class MyCallback implements ConfigurationCallback<String> {
      @Override
      public void update(String key, String value) {
        System.out.println("Updated key [" + key + "] with value: " + value);
      }
    }
    config.bind("my.key", new MyCallback());
    
    // A runtime override that will execute callbacks.
    config.addOverride("my.key", ConfigurationOverride.newBuilder()
        .setSource(RuntimeOverrideProvider.SOURCE)
        .setValue("Overriden"));
    System.out.println(config.getString("my.key"));
    
    // Make sure to close the config before exiting your program.
    config.close();
}
```

TODO - further documentation.