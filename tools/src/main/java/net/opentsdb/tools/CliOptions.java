// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.io.IOException;
import java.util.Map;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.Level;

import com.typesafe.config.Config;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.slf4j.LoggerFactory;

import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;

/** Helper functions to parse arguments passed to {@code main}.  */
public final class CliOptions {

  static {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
  }

  /** Adds common TSDB options to the given {@code argp}.  */
  public static void addCommon(final ArgP argp) {
    argp.addOption("--table", "TABLE",
                   "Name of the HBase table where to store the time series"
                   + " (default: tsdb).");
    argp.addOption("--uidtable", "TABLE",
                   "Name of the HBase table to use for Unique IDs"
                   + " (default: tsdb-uid).");
    argp.addOption("--zkquorum", "SPEC",
                   "Specification of the ZooKeeper quorum to use"
                   + " (default: localhost).");
    argp.addOption("--zkbasedir", "PATH",
                   "Path under which is the znode for the -ROOT- region"
                   + " (default: /hbase).");
    argp.addOption("--config", "PATH",
                   "Path to a configuration file"
                   + " (default: Searches for file see docs).");
  }

  /** Adds a --verbose flag.  */
  static void addVerbose(final ArgP argp) {
    argp.addOption("--verbose",
                   "Print more logging messages and not just errors.");
    argp.addOption("-v", "Short for --verbose.");
  }

  /** Adds the --auto-metric flag.  */
  public static void addAutoMetricFlag(final ArgP argp) {
    argp.addOption("--auto-metric", "Automatically add metrics to tsdb as they"
                   + " are inserted.  Warning: this may cause unexpected"
                   + " metrics to be tracked");
  }

  /**
   * Parse the command line arguments with the given options.
   * @param options Options to parse in the given args.
   * @param args Command line arguments to parse.
   * @return The remainder of the command line or
   * {@code null} if {@code args} were invalid and couldn't be parsed.
   */
  public static String[] parse(final ArgP argp, String[] args) {
    try {
      args = argp.parse(args);
    } catch (IllegalArgumentException e) {
      System.err.println("Invalid usage.  " + e.getMessage());
      return null;
    }
    honorVerboseFlag(argp);
    return args;
  }

  /**
   * Attempts to load a configuration given a file or default files
   * and overrides with command line arguments
   * @return A config object with user settings or defaults
   * @throws IOException If there was an error opening any of the config files
   * @throws FileNotFoundException If the user provided config file was not found
   * @since 2.0
   */
  public static final Config getConfig(final ArgP argp) throws IOException {
    // load configuration
    final Config config;
    final String config_file = argp.get("--config", "");
    if (!config_file.isEmpty())
      config = ConfigFactory.load(config_file);
    else
      config = ConfigFactory.load();

    // load CLI overloads
    return overloadConfig(argp, config);
  }
  
  /**
   * Copies the parsed command line options to the {@link Config} class
   * @param config Configuration instance to override
   * @since 2.0
   */
  static Config overloadConfig(final ArgP argp, Config config) {
    // loop and switch so we can map cli options to tsdb options
    for (Map.Entry<String, String> entry : argp.getParsed().entrySet()) {
      // map the overrides
      if ("--auto-metric".equals(entry.getKey().toLowerCase())) {
        config = config.withValue("tsd.core.auto_create_metrics",
                ConfigValueFactory.fromAnyRef(true, "opentsdb --auto-metric"));
      } else if ("--table".equals(entry.getKey().toLowerCase())) {
        config = config.withValue("tsd.storage.hbase.data_table",
                ConfigValueFactory.fromAnyRef(entry.getValue(), "opentsdb --table"));
      } else if ("--uidtable".equals(entry.getKey().toLowerCase())) {
        config = config.withValue("tsd.storage.hbase.uid_table",
                ConfigValueFactory.fromAnyRef(entry.getValue(), "opentsdb --uidtable"));
      } else if ("--zkquorum".equals(entry.getKey().toLowerCase())) {
        config = config.withValue("tsd.storage.hbase.zk_quorum",
                ConfigValueFactory.fromAnyRef(entry.getValue(), "opentsdb --zkquorum"));
      } else if ("--zkbasedir".equals(entry.getKey().toLowerCase())) {
        config = config.withValue("tsd.storage.hbase.zk_basedir",
                ConfigValueFactory.fromAnyRef(entry.getValue(), "opentsdb --zkbasedir"));
      } else if ("--port".equals(entry.getKey().toLowerCase())) {
        config = config.withValue("tsd.network.port",
                ConfigValueFactory.fromAnyRef(entry.getValue(), "opentsdb --port"));
      } else if ("--staticroot".equals(entry.getKey().toLowerCase())) {
        config = config.withValue("tsd.http.staticroot",
                ConfigValueFactory.fromAnyRef(entry.getValue(), "opentsdb --staticroot"));
      } else if ("--cachedir".equals(entry.getKey().toLowerCase())) {
        config = config.withValue("tsd.http.cachedir",
                ConfigValueFactory.fromAnyRef(entry.getValue(), "opentsdb --cachedir"));
      } else if ("--flush-interval".equals(entry.getKey().toLowerCase())) {
        config = config.withValue("tsd.core.flushinterval",
                ConfigValueFactory.fromAnyRef(entry.getValue(), "opentsdb --flush-interval"));
      } else if ("--backlog".equals(entry.getKey().toLowerCase())) {
        config = config.withValue("tsd.network.backlog",
                ConfigValueFactory.fromAnyRef(entry.getValue(), "opentsdb --backlog"));
      } else if ("--bind".equals(entry.getKey().toLowerCase())) {
        config = config.withValue("tsd.network.bind",
                ConfigValueFactory.fromAnyRef(entry.getValue(), "opentsdb --bind"));
      } else if ("--async-io".equals(entry.getKey().toLowerCase())) {
        config = config.withValue("tsd.network.async_io",
                ConfigValueFactory.fromAnyRef(entry.getValue(), "opentsdb --async-io"));
      } else if ("--worker-threads".equals(entry.getKey().toLowerCase())) {
        config = config.withValue("tsd.network.worker_threads",
                ConfigValueFactory.fromAnyRef(entry.getValue(), "opentsdb --worker-threads"));
      }
    }

    return config;
  }
  
  /** Changes the log level to 'WARN' unless --verbose is passed.  */
  private static void honorVerboseFlag(final ArgP argp) {
    if (argp.optionExists("--verbose") && !argp.has("--verbose")
        && !argp.has("-v")) {
      // SLF4J doesn't provide any API to programmatically set the logging
      // level of the underlying logging library.  So we have to violate the
      // encapsulation provided by SLF4J.
      for (final Logger logger :
           ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME))
             .getLoggerContext().getLoggerList()) {
        logger.setLevel(Level.WARN);
      }
    }
  }
}
