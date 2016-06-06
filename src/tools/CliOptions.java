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

import net.opentsdb.utils.Config;

import org.slf4j.LoggerFactory;

import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;

/** Helper functions to parse arguments passed to {@code main}.  */
final class CliOptions {

  static {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
  }

  /** Adds common TSDB options to the given {@code argp}.  */
  static void addCommon(final ArgP argp) {
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
  static void addAutoMetricFlag(final ArgP argp) {
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
  static String[] parse(final ArgP argp, String[] args) {
    try {
      args = argp.parse(args);
    } catch (IllegalArgumentException e) {
      System.err.println("Invalid usage.  " + e.getMessage());
      System.exit(2);
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
  static final Config getConfig(final ArgP argp) throws IOException {
    // load configuration
    final Config config;
    final String config_file = argp.get("--config", "");
    if (!config_file.isEmpty())
      config = new Config(config_file);
    else
      config = new Config(true);

    // load CLI overloads
    overloadConfig(argp, config);
    // the auto metric is recorded to a class boolean flag since it's used so
    // often. We have to set it manually after overriding.
    config.setAutoMetric(config.getBoolean("tsd.core.auto_create_metrics"));
    return config;
  }
  
  /**
   * Copies the parsed command line options to the {@link Config} class
   * @param config Configuration instance to override
   * @since 2.0
   */
  static void overloadConfig(final ArgP argp, final Config config) {

    // loop and switch so we can map cli options to tsdb options
    for (Map.Entry<String, String> entry : argp.getParsed().entrySet()) {
      // map the overrides
      if (entry.getKey().toLowerCase().equals("--auto-metric")) {
        config.overrideConfig("tsd.core.auto_create_metrics", "true");
      } else if (entry.getKey().toLowerCase().equals("--table")) {
        config.overrideConfig("tsd.storage.hbase.data_table", entry.getValue());
      } else if (entry.getKey().toLowerCase().equals("--uidtable")) {
        config.overrideConfig("tsd.storage.hbase.uid_table", entry.getValue());
      } else if (entry.getKey().toLowerCase().equals("--zkquorum")) {
        config.overrideConfig("tsd.storage.hbase.zk_quorum",
            entry.getValue());
      } else if (entry.getKey().toLowerCase().equals("--zkbasedir")) {
        config.overrideConfig("tsd.storage.hbase.zk_basedir",
            entry.getValue());
      } else if (entry.getKey().toLowerCase().equals("--port")) {
        config.overrideConfig("tsd.network.port", entry.getValue());
      } else if (entry.getKey().toLowerCase().equals("--staticroot")) {
        config.overrideConfig("tsd.http.staticroot", entry.getValue());
      } else if (entry.getKey().toLowerCase().equals("--cachedir")) {
        config.overrideConfig("tsd.http.cachedir", entry.getValue());
      } else if (entry.getKey().toLowerCase().equals("--flush-interval")) {
        config.overrideConfig("tsd.core.flushinterval", entry.getValue());
      } else if (entry.getKey().toLowerCase().equals("--backlog")) {
        config.overrideConfig("tsd.network.backlog", entry.getValue());
      } else if (entry.getKey().toLowerCase().equals("--read-only")) {
        config.overrideConfig("tsd.mode", "ro");
      } else if (entry.getKey().toLowerCase().equals("--bind")) {
        config.overrideConfig("tsd.network.bind", entry.getValue());
      } else if (entry.getKey().toLowerCase().equals("--async-io")) {
        config.overrideConfig("tsd.network.async_io", entry.getValue());
      } else if (entry.getKey().toLowerCase().equals("--worker-threads")) {
        config.overrideConfig("tsd.network.worker_threads", entry.getValue());
      } 	  
    }
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
