## OpenTSDB FatJar

### Introduction

FatJar is a modified build procedure for OpenTSDB intended to provide a single executable jar that contains all of the java artifacts and resources required to run an OpenTSDB 2.1+ instance. This includes the tsd and all associated command line tools. It does not include the following which remain external dependencies:
* The gnuplot executable which generates data visualizations in the OpenTSDB graphical web console.
* The HBase instance or cluster where time-series data is stored.
* The Java runtime.

With a FatJar build and the above dependencies, it is possible to start an OpenTSDB as simply as:

```java -jar opentsdb-2.1.jar```

#### Building the FatJar

These steps assume building from my current fork repo.

1. Clone: ```git clone https://github.com/nickman/opentsdb.git```
2. Switch to project directory: ```cd opentsdb```
3. Switch to **next** branch: ```git checkout next```
4. Build the **fat-jar-pom.xml**: ```./build.sh fat-jar-pom.xml```
5. Run maven, specifying the fat-jar pom (and skipping the gpg plugin and tests): ```mvn -f fat-jar-pom.xml -Dgpg.skip -DskipTests clean install```
6. Fire her up, Scotty:  ```java -jar ./target/opentsdb-2.1.0RC1-fat.jar tsd```

**NOTE**: FatJar requires *maven 3*.

**ANOTHER NOTE**: THe OpenTSDB code base has some non-compliant/missing javadoc tags. If you are compiling with Java 8, [the build will fail](http://http://stackoverflow.com/questions/15886209/maven-is-not-working-in-java-8-when-javadoc-tags-are-incomplete), so:
1. Use Java 6 or 7  OR...
2. Edit the fat-jar-pom.xml, find the javadoc plugin and uncomment the line:

```<!-- <additionalparam>-Xdoclint:none</additionalparam> -->```

###### This is currently on line 356. It is commented because is it not supported for Java 6 & 7.

(Note that an [unreviewed] aspect of FatJar is the provision of default values for *all* configuration parameters so the three traditionally required parameters, **port**, **staticroot** and **cachedir** are not required.)

FatJar is accomplished with:
1. Small changes to 3 existing OpenTSDB classes (all other aspects of the code base remain unchanged)
2. The addition of 8 new classes
3. Additions to the OpenTSDB make scripts to generate the FatJar maven pom
3. A maven based build that packages the following into a single jar:
	*  The OpenTSDB core
	*  All of the maven defined dependencies (jars)
	*  An executable jar defining manifest
	*  A JSON based configuration definition repository
	*  All of the OpenTSDB UI resources

### Motivation and Benefits

The original motivation for a FatJar build was the simplification of deploying OpenTSDB to multiple "locked-down" environments where it was not feasible to build from source. The addition of RPM and DEB packages for Linux have significantly eased this process, but deployment to non-Linux environments remains a largely manual procedure. While it would be conceivable to implement Solaris and Windows install packages (our 2 pain points), it seemed simpler to adopt the FatJar approach and kill the entire flock of birds with one stone.

The FatJar has been optimized to support simplicity of deployment and configuration, while maintaining complete backward compatability so the modifications are completely additive.

Among some of the benefits we enjoy from the FatJar build:
* Simplified deployment
* No requirement for a pre-deployed UI static content directory. The FatJar contains all the static resources and unloads this content into the configured/default directory at start time, including the GWT Web App content and the gnuplot wrapper script. If any content already exists and has a newer timestamp than the corresponding resource in the FatJar, the resource is left as is. An additional FatJar provided command line tool allows for the pre-creation of the static content directory, which is useful in development.
* The process PID is written to a default [and configurable] pid file, and deleted on clean shutdown.
* URL based config and include supporting the load of configuration files from http:// or file:// based sources.
* Configuration properties can be tokenized with System Prop and/or Environmental Variable decodes as well as JavaScript snippets to support dynamically computed property values according to the deployment environment.
* Refined logging configuration with configuration override-able external logging config file location and command line options to set logging levels for major packages in OpenTSDB and associated libraries.


### Changes and Additions

#### Modified Classes and Files

* **.gitignore**: Added transient artifacts created by FatJar builds.
* **Makefile.am**: Added target **fat-jar-pom.xml** that generates the fat-jar symbolic source overlay and fat-jar-pom.xml. Added the new source and test files to **tsdb_SRC** and **test_SRC**.
* **src/tsd/GraphHandler.java**: FatJar introduced a class GnuplotInstaller which locates the gnuplot executable by scanning the path and installs the Gnuplot wrapper script. This modification integrates GnuplotInstaller into the GraphHandler.
* **src/tsd/PipelineFactory.java**: The pipeline's idle-timeout handler HashedWheelTimer was using a non-daemon thread and preventing an orderly shutdown. This is also a fix for #455.
* **src/utils/Config.java**: Made ```loadStaticVariables()``` public.

#### New Classes and Files

* **fat-jar/create-src-dir-overlay.sh**: Creates a maven oriented source overlay and generates a **fat-jar-pom.xml**.
* **fat-jar/fat-jar-pom.xml.in**: The input template for **fat-jar-pom.xml**.
* **fat-jar/logback.xml**: The FatJar OpenTSDB boot time and default logging configuration.
* **fat-jar/file-logback.xml**: The parameterized file based logging and file-rolling configuration if a log file name is configured.
* **fat-jar/test-logback.xml**: Test resource logback configuration. Pretty much like **fat-jar/logback.xml** but without the jmx component enabled since it messes up the use of PowerMock.
* **fat-jar/opentsdb.conf.json**: The configuration parameter definition repository
* **src/tools/ArgValueValidator.java**: Interface that defines a class that validates a type of configuration parameter.
* **src/tools/ConfigArgP.java**: Configuration parameter manager. Conceptually a merge of Config and ArgP. Contains a variety of testing hooks.
* **src/tools/ConfigMetaType.java**: Enumeration of configuration parameter types, each with a link to an **ArgValueValidator** so that each parameter value can be validated as accurately as possible.
* **src/tools/GnuplotInstaller.java**: Validates the presence of the gnuplot executable and installs the OpenTSDB gnuplot invocation wrapper shell script.
* **src/tools/Main.java**: The FatJar substitute for **TSDMain**, serving as the boot entry point for the TSD as well as the command line tools.
* **test/tools/TestConfigArgP.java**: Test suite for the **ConfigArgP** class.

#### New Config Parameters

* **tsd.logback.file**: The name of the log file the OpenTSDB logback configuration will log to when running the FatJar.
* **tsd.logback.rollpattern**: The logback rolling file appender roll pattern used to roll the file defined in **tsd.logback.file**.
* **tsd.logback.console**: If specified, logback will log to the configured file and keep logging to the console, otherwise, when the file appender is activated, the console appender is removed.
* **tsd.logback.config**: Points to an external [outside the FatJar] logback config file, in which case the internal file based logging configuration is ignored once the config has been loaded.
* **tsd.process.pid.file**: The name and location of the OpenTSDB PID file.
* **tsd.process.pid.ignore.existing**: Normally, if the PID file defined in **tsd.process.pid.file** is found at startup, startup will be aborted. This directive ignores the existing file and overwrites it.
* **tsd.core.config**: Points to a core configuration file which should be used as the base line config. This means the built in config can simply contain this item and point to a shared file or HTTP based location for the main configuration.
* **tsd.ui.noexport**: Disables the automatic UI content export to the local file system.
* **tsd.core.config.include**: Specifies a file or HTTP based config that will override the config specified in the core configuration. (See Presedence below)
* **help**: Not really a configuration, but present to flag the word as an OpenTSDB processable command.
* Default Bindings: These are default JS bindings which can be useful if scripting dynamic configuration values for thread counts and/or cache sizes. Note that all system properties and environmental variables are automatically available in the JavaScript context.
    * **processors**: The number of cores available to the JVM
    * **maxbytes**: The maximum amount of memory available to the JVM

#### Configuration Presedence

In order of presedence:
1. Command line parameters
2. Included parameters
3. Core parameters

#### Installing the UI content

The FATJar supports a command line tool to install the UI static content to a specified location:

```
Usage:  java -jar <opentsdb.jar> exportui --d <destination> [--p]
  --d=DIR The directory to export the UI content to
  --p     Create the directory if it does not exist
```
Example:

```java -jar opentsdb-2.1.0RC1.jar exportui --d /tmp/opentsdb/static-content --p```

Output:

```
2015-03-15 16:48:14,988 INFO  [main] Main: Created exportui directory [/tmp/opentsdb/static-content]
2015-03-15 16:48:15,258 INFO  [main] Main:

	===================================================
	Static Root Directory:[/tmp/opentsdb/static-content]
	Total Files Written:24
	Total Bytes Written:1162522
	File Write Failures:0
	Existing File Newer Than Content:0
	Elapsed (ms):270
	===================================================
```

#### Command Line Help

The FatJar supports a slightly modified command line help model. The command help will print the main “menu” of options for the FatJar.

```
java -jar opentsdb-x.x.x.jar help
Usage: java -jar [opentsdb.jar] [command] [args] 
Valid commands: 
	tsd: Starts a new TSDB instance 
	fsck: Searches for and optionally fixes corrupted data in a TSDB 
	import: Imports data from a file into HBase through a TSDB 
	mkmetric: Creates a new metric 
	query: Queries time series data from a TSDB 
	scan: Dumps data straight from HBase 
	uid: Provides various functions to search or modify information in the tsdb-uid table. 
	exportui: Exports the OpenTSDB UI static content 

	Use help <command> for details on a command 
```

Using java -jar opentsdb-x.x.x.jar help <command> for all CLI tools will print the tool's existing usage message. Help for tsd has a couple of additional options. Using the call above, with tsd as the command, the standard usage will be printed. With the additional argument of the keyword **extended**, the full set of configuration options is printed. Full example [here](https://gist.github.com/nickman/673852d8d88675043f45).

#### Eclipse Support

An [almost] incidental benefit for users of the Eclipse IDE is that the FatJar source overlay simplifies the setup of a "working-out-of-the-box" Eclipse project using maven. Just do this:

```mvn -f fat-jar-pom.xml eclipse:eclipse```

Then load up the project in Eclipse and launch **net.opentsdb.tools.Main** with the parameter **tsd"**.

All at no cost to you.

#### Last Note

There's some big changes here. I recognize it's not perfect. Please let me have any feedback. It will not be taken personally, but only to improve OpenTSDB.






