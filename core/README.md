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

From the external API standpoint we'll keep things the same and continue adding more features. However since the new design is much more extensible, admins will have to choose which features to enable or disable for their individual installations.

Goals and features for 3.x and beyond include:

* A composable query layer to allow:
  * Fetching data from multiple data stores including read and write caches, multiple data centers for master/master merging and integrating with meta stores.
  * Flexible operation ordering.
  * Streaming and time sharded queries.
* Multiple storage backends including the original HBase schema along with newer Berengi, Bigtable or Cassandra.
* Pluggable data types beyond the included numerics and annotations.

### Finding your way around the new code Base

#### **[Core] (https://github.com/OpenTSDB/opentsdb/tree/3.0/core/src/main/java/net/opentsdb/core)** 

These are the most common classes shared amongst OpenTSDB components. Some important classes include:

* **[TSDB.java] (https://github.com/OpenTSDB/opentsdb/blob/3.0/core/src/main/java/net/opentsdb/core/TSDB.java)** - The main container object that all servers and clients will instantiate to perform OpenTSDB operations.
* **[Registry] (https://github.com/OpenTSDB/opentsdb/blob/3.0/core/src/main/java/net/opentsdb/core/Registry.java)** - A central location, belonging to the TSDB, where plugins, factories, etc are located so that operations can get their work done. We're making sure to avoid static instances in TSDB so that multiple TSDs can run in the same JVM. Instead instances belonging to a TSD should be registered here.
* **[TsdbPlugin.java] (https://github.com/OpenTSDB/opentsdb/blob/3.0/core/src/main/java/net/opentsdb/core/TsdbPlugin.java)** - The base abstract used for all TSDB plugin instances.

#### **[Data] (https://github.com/OpenTSDB/opentsdb/tree/3.0/core/src/main/java/net/opentsdb/data)**

The code in here includes the abstract and interfaces that define time series data types or perform common operations over them. Built in data types are found here such as the ``NumericType`` and ``Annotation`` so if you would like to add a new type to OpenTSDB, look at these as an example. Additionally, the base iterators are defined here that allow for grouping and working over sets of time series data.

#### **[Query] (https://github.com/OpenTSDB/opentsdb/tree/3.0/core/src/main/java/net/opentsdb/query)**

OpenTSDB's new query pipeline is located here. The high level components include:

* **Context** - An object that travels with individual queries storing session data as well as state used to execute and complete the query.
* **Execution** - Query pipeline components that assemble into a query graph for fetching and serializing data. Examples include sharding a query by metric and time, reading/writing to caches and sending queries to external components.
* **Filters/Pojos** - The query definition that tells the TSD what to fetch.
* **Processor** - Components that mutate the queried data prior to serialization. These are responsible for things like downsampling, expression calculation and rate conversion.

#### **[Storage] (https://github.com/OpenTSDB/opentsdb/tree/3.0/core/src/main/java/net/opentsdb/storage)**

This contains the abstract interface to a concrete data store (responsible for storing and fetching time series data but not responsible for mutating it at query time; that is up to the processors above). Also contains common classes used by some of the storage implementations.

## Configuration

Until the main documentation is up we'll note some important configs here.

So far, the 3.x configuration is similar to the 2.x branch with the ``opentsdb.conf`` file. Here are some important, common configs for 3.x.

* ``tsd.query.default_execution_graphs`` - A JSON config that defines the default execution graphs available on startup for the TSD. If the config value ends with ``.json`` (in lower case) then it's assumed the value is the path to a file that will be opened and parsed. Otherwise users can paste quote-escaped (``\"``) JSON in the config file. (But since it's ugly, try using the file method).
* ``tsd.plugin.config`` - A JSON config that defines the plugins loaded by the TSD and the order in which they are initialized.

Both of these configs are currently required and we'll clean them up in the future.

### Execution Graphs Config

This config contains a list of execution graphs available for use at query time. At least one default must be provided in order for queries to execute. For example:

```javascript
[{
  "id": null,
  "nodes": [{
    "executorId": "LocalCache",
    "dataType": "timeseries",
    "upstream": null,
    "executorType": "TimeSlicedCachingExecutor",
    "defaultConfig": {
      "executorId": "LocalCache",
      "executorType": "TimeSlicedCachingExecutor",
      "plannerId": "IteratorGroupsSlicePlanner",
      "expiration": 60000
    }
  }, {
    "executorId": "http",
    "dataType": "timeseries",
    "upstream": "LocalCache",
    "executorType": "HttpQueryV2Executor",
    "defaultConfig": {
      "executorType": "HttpQueryV2Executor",
      "executorId": "http",
      "endpoint": "**** YOUR TSD ROTATION HERE ****",
      "timeout": 60000
    }
  }]
}]
```

The two components of each execution graph are:

* ``id`` - The unique name for the graph. If null or empty then the graph is treated as the default execution graph.
* ``nodes`` - The list of execution nodes creating the graph.

Each node config has the following fields:

* ``executorId`` - A required unique, descriptive name for the node within the graph.
* ``dataType`` - Always set to ``timeseries`` for now.
* ``upstream`` - If the node is downstream of another node (e.g. the HTTP executor can be downstream of a cache node) then the ``executorId`` of the upstream node is given here.
* ``executorType`` - This is the class name of an executor implementation (defaults or plugins). It can be the fully qualified class name or just the simple name.
* ``defaultConfig`` - This is the default configuration for the executor that is passed in during instantiation. Each config is a little different but has the following common fields:
    * ``executorId`` - Must be the same name as the node config's ``executorId``.
    * ``executorType`` - The same class name as the node config's ``executorType``.

On TSD startup, the executors are initialized one time and queries flow through the graphs independently.

For details on what parameters are available for each executor, until we get them documented nicely you'll have to look at the ``Config`` class towards the bottom of each source code file. The builder methods are documented in the source.

### Plugin Config

This config file contains information about what plugins to load, where to find them, and what to name them. For example:

```javascript
{
  "configs": [
  {
    "type": "net.opentsdb.query.execution.QueryExecutorFactory"
  },{
    "plugin": "net.opentsdb.stats.BraveTracer",
    "default": true,
    "type": "net.opentsdb.stats.TsdbTracer"
  }],
  "pluginLocations": [],
  "continueOnError": true
}
```

The fields available are:

* ``configs`` - A list of plugin configs, defined below.
* ``pluginLocations`` - A list of paths that may include either directories or explicit JAR files (ending in ``.jar``) to load. These are processed before loading the configs.
* ``continueOnError`` - Whether or not to allow the TSD to continue loading plugins if one of the configurations could not find implementations or had an error on instantiation. When set to false (the default) then the TSD will stop and load the errors.

Each config entry has the following fields:

* ``type`` - The fully qualified class name of the plugin *type*. Not the concrete implementation. E.g. ``net.opentsdb.query.execution.QueryExecutorFactory`` is a *type* of plugin of which there are many implementations.
* ``plugin`` - The fully qualified class name of the plugin *implementation*. E.g. ``net.opentsdb.query.execution.CachingQueryExecutor`` is a concrete implementation of the ``net.opentsdb.query.execution.QueryExecutorFactory`` *type*.
* ``id`` - A unique name for the instance of this plugin. This allows more than one instance to be loaded with different configurations. If this ID is null or empty, then ``default`` must be set to ``true``.
* ``default`` - Determines whether or not the plugin should be the default implementation for it's ``type``. This is set to ``false`` by default.

A couple of rules define these configs:

1. A config may contain **only** the ``type`` field and all concrete implementations for that type will be loaded. In each case the ``id`` of the loaded plugin will be the full class name of the implementation. This is useful for plugins like the ``net.opentsdb.query.execution.QueryExecutorFactory`` where many implementations are provided and you want to load all of them without defining each.
1. Otherwise a config must contain the ``type``, ``plugin`` and either a non-null and non-empty ``id`` or ``default`` must be set to true. Every ID must also be unique.