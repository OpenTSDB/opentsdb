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

## Core Module

The core module is the heart of OpenTSDB, containing the definitions of plugins, query pipeline code, data type handling and the main TSDB.java class. TSD servers and clients will always have core code, thus this module should maintain code that most implementations and plugins will share as well as keep the dependency list down to avoid conflicts.

## New for 3.x

As of OpenTSDB 3.0 the code has undergone an extensive re-write which necessitates a breaking Java API change. We'll try our best to remain compatible with the old Java APIs but some classes and methods will move around. We'll try to document these as much as possible.

From the external API standpoint we'll strive to keep things the same and continue adding more features. However since the new design is much more extensible, admins will have to choose which features to enable or disable for their individual installations.

Goals and features for 3.x and beyond include:

* A composable query layer to allow:
  * Fetching data from multiple data stores including read and write caches, multiple data centers for master/master merging and integrating with meta stores.
  * Flexible operation ordering.
  * Extended filtering options.
* Multiple storage backends including the original HBase schema along with Bigtable, InfluxDB, Cloudwatch, Prometheus, etc. A single TSD will be able to query data from multiple sources for analysis.
* Pluggable data types beyond the included numerics and annotations.

**TODOs**

* The QueryResult pipeline is working well and is memory efficient but operationally slow. While it has the benefit of providing access to the raw data with precise timestamps, most monitoring users don't care as much about precision and are happy to use aggregated data. Thus most modern time series databases will emit downsampled vectors for CPU cache efficiency and fast query times. We're in the process of moving the 3.0 code to a *push* based pipeline where data is bubbled up from storage through the pipeline in either raw and precise data or aggregates.

### Finding your way around the new code Base

#### **[Core] (https://github.com/OpenTSDB/opentsdb/tree/3.0/core/src/main/java/net/opentsdb/core)** 

These are the most common classes shared amongst OpenTSDB components. Some important classes include:

* **[TSDB.java] (https://github.com/OpenTSDB/opentsdb/blob/3.0/core/src/main/java/net/opentsdb/core/TSDB.java)** - The main container object that all servers and clients will instantiate to perform OpenTSDB operations.
* **[Registry] (https://github.com/OpenTSDB/opentsdb/blob/3.0/core/src/main/java/net/opentsdb/core/Registry.java)** - A central location, belonging to the TSDB, where plugins, factories, etc are located so that operations can get their work done. We're making sure to avoid static instances in TSDB so that multiple TSDs can run in the same JVM. Instead instances belonging to a TSD should be registered here.
* **[TsdbPlugin.java] (https://github.com/OpenTSDB/opentsdb/blob/3.0/core/src/main/java/net/opentsdb/core/TsdbPlugin.java)** - The base abstract used for all TSDB plugin instances.

#### **[Data] (https://github.com/OpenTSDB/opentsdb/tree/3.0/core/src/main/java/net/opentsdb/data)**

The code in here includes the abstract and interfaces that define time series data types or perform common operations over them. Built in data types are found here such as the ``NumericType`` and ``Annotation`` so if you would like to add a new type to OpenTSDB, look at these as an example. Additionally, the base iterators are defined here that allow for grouping and working over sets of time series data.

#### **[Query] (https://github.com/OpenTSDB/opentsdb/tree/3.0/core/src/main/java/net/opentsdb/query)**

OpenTSDB's new query pipeline is located here. Queries are now flexible DAGs currently defined programatically or via an ugly semantic JSON/YAML format. The high level components include:

* **Context** - An object that travels with individual queries storing session data as well as state used to execute and complete the query.
* **Filters** - Filters over data and tags to determine what data is fetched for a query.
* **Processors** - Components that mutate the queried data prior to serialization. These are responsible for things like downsampling, expression calculation and rate conversion. 
* **Routers** - Handles routing queries to the proper destiations including HA support, etc.

#### **[Storage] (https://github.com/OpenTSDB/opentsdb/tree/3.0/core/src/main/java/net/opentsdb/storage)**

This contains the abstract interface to a concrete data store (responsible for storing and fetching time series data but not responsible for mutating it at query time; that is up to the processors above). Also contains common classes used by some of the storage implementations.

## Configuration

Until the main documentation is up we'll note some important configs here.

TSDB V3 has a new config system supporting command line, environment, JVM, properties formatted files, YAML or JSON formatted files and HTTP fetches for configuration. Multiple configs can be provided and flattened into one view allowing for easier distribution of TSDs in a containerized environment. YAML/JSON will be the format going forward as it allows for storing advanced configs like lists and maps as well as complex objects.

Because just about everything is a plugin in v3, a TSD won't do much without a configuration that tells it, at a minimum, what data store to load. A dummy in-memory store is provided with some test metrics to test with but of course a TSD can configured to talk to an existing HBase or Bigtable instance. Below we'll describe the quick start config and some config details.

### Quickstart Config

```yaml
# The TCP port TSD should use for the HTTP Server
# *** REQUIRED or use tsd.network.ssl_port ***
tsd.network.port: 4242

# The Plugin JSON (with escaped quotes, on one line) or config file (ending in .json)
# that determines what plugins to load and in what order.
tsd.plugin.config:
  configs:
    -
      plugin: net.opentsdb.storage.MockDataStoreFactory
      isDefault: true
      type: net.opentsdb.data.TimeSeriesDataSourceFactory
  
  pluginLocations:
  continueOnError: true
  loadDefaultInstances: true

```

This is the file found in the Docker image under the name `opentsdb_dev.yaml`.

The first entry `tsd.network.port` is used to set the listening port for the Undertow HTTP server. Note that some properties from the v2 config can transfer over to the new v3 config.

Next is a required, complex config called the `tsd.plugin.conf` that determines what plugins are loaded and in what order.

### Plugin Config

From the example above, we see that we are loading a single plugin with the Java class name `net.opentsdb.storage.MockDataStoreFactory` of the type `net.opentsdb.data.TimeSeriesDataSourceFactory`. This means we'll load a single data store (the dummy in-memory store) and it will be the default store used by queries as `isDefault` is set to true.

Multiple plugins can be loaded and multiple plugins of the same time can be loaded as long as each has a unique ID. The order of the plugins in the YAML config is important as those at the top are loaded first. So if a plugin, such as a Redis cache, depends on another plugin to be loaded, make sure it appears *after* the dependency.

The top-level fields available for the overall plugin config are:

* ``configs`` - A list of plugin configs. Details follow below.
* ``pluginLocations`` - A list (or JSON array) of paths that may include either directories or explicit JAR files (ending in ``.jar``) to load. These are processed before loading the configs.
* ``continueOnError`` - Whether or not to allow the TSD to continue loading plugins if one of the configurations could not find implementations or had an error on instantiation. When set to false (the default) then the TSD will stop and load the errors.
* `loadDefaultInstances` Whether or not to load the default plugins, such as built in filters and processors, are loaded. Note that if this set set to true, the config will likely need to be pretty big to explicitly load all of the types of plugins available.

Each config entry has the following fields:

* ``type`` - The fully qualified class name of the plugin *type*. Not the concrete implementation. E.g. ``net.opentsdb.data.TimeSeriesDataSourceFactory`` is a *type* of plugin of which there are many implementations.
* ``plugin`` - The fully qualified class name of the plugin *implementation*. E.g. ``net.opentsdb.storage.schemas.tsdb1x.SchemaFactory`` is a concrete implementation of the ``net.opentsdb.data.TimeSeriesDataSourceFactory`` *type*.
* ``id`` - A *unique name* for the instance of this plugin. This allows more than one instance to be loaded with different configurations. If this ID is null or empty, then ``default`` must be set to ``true``.
* ``default`` - Determines whether or not the plugin should be the default implementation for it's ``type``. This is set to ``false`` by default.

A couple of rules define these configs:

1. A config may contain **only** the ``type`` field and all concrete implementations for that type will be loaded. In each case the ``id`` of the loaded plugin will be the full class name of the implementation. This is useful for plugins like the ``net.opentsdb.query.execution.QueryExecutorFactory`` where many implementations are provided and you want to load all of them without defining each.
1. Otherwise a config must contain the ``type``, ``plugin`` and either a non-null and non-empty ``id`` or ``default`` must be set to true. Every ID must also be unique.