// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Tags;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.WritableDataPoints;

final class TextImporter {

  private static final Logger LOG = LoggerFactory.getLogger(TextImporter.class);

  /** Prints usage and exits with the given retval.  */
  static void usage(final ArgP argp, final int retval) {
    System.err.println("Usage: import path [more paths]");
    System.err.print(argp.usage());
    System.err.println("This tool can directly read gzip'ed input files.");
    System.exit(retval);
  }

  public static void main(String[] args) throws IOException {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, 1);
    } else if (args.length < 1) {
      usage(argp, 2);
    }

    final TSDB tsdb = new TSDB(argp.get("--table", "tsdb"),
                               argp.get("--uidtable", "tsdb-uid"));
    argp = null;
    for (final String path : args) {
      importFile(tsdb, path);
    }
    tsdb.flush();
  }

  private static void importFile(final TSDB tsdb,
                                 final String path) throws IOException {
    final long start_time = System.nanoTime();
    final BufferedReader in = open(path);
    String line = null;
    int points = 0;
    try {
      while ((line = in.readLine()) != null) {
        final String[] words = Tags.splitString(line, ' ');
        final String metric = words[0];
        if (metric.length() <= 0) {
          throw new RuntimeException("invalid metric: " + metric);
        }
        final long timestamp = Long.parseLong(words[1]);
        if (timestamp <= 0) {
          throw new RuntimeException("invalid timestamp: " + timestamp);
        }
        final String value = words[2];
        if (value.length() <= 0) {
          throw new RuntimeException("invalid value: " + value);
        }
        final HashMap<String, String> tags = new HashMap<String, String>();
        for (int i = 3; i < words.length; i++) {
          if (!words[i].isEmpty()) {
            Tags.parse(tags, words[i]);
          }
        }
        final WritableDataPoints dp = getDataPoints(tsdb, metric, tags);
        if (value.indexOf('.') < 0) {  // integer value
          dp.addPoint(timestamp, Long.parseLong(value));
        } else {  // floating point value
          dp.addPoint(timestamp, Float.parseFloat(value));
        }
        points++;
      }
    } catch (RuntimeException e) {
        LOG.error("Exception caught while processing file "
                  + path + " line=" + line);
        throw e;
    } finally {
      in.close();
    }
    final long time_delta = (System.nanoTime() - start_time) / 1000000;
    LOG.info(String.format("Processed %s in %d ms, %d data points"
                           + " (%.1f points/s)",
                           path, time_delta, points,
                           (points * 1000.0 / time_delta)));
  }

  /**
   * Opens a file for reading, handling gzipped files.
   * @param path The file to open.
   * @return A buffered reader to read the file, decompressing it if needed.
   * @throws IOException when shit happens.
   */
  private static BufferedReader open(final String path) throws IOException {
    InputStream is = new FileInputStream(path);
    if (path.endsWith(".gz")) {
      is = new GZIPInputStream(is);
    }
    // I <3 Java's IO library.
    return new BufferedReader(new InputStreamReader(is));
  }

  private static final HashMap<String, WritableDataPoints> datapoints =
    new HashMap<String, WritableDataPoints>();

  private static
    WritableDataPoints getDataPoints(final TSDB tsdb,
                                     final String metric,
                                     final HashMap<String, String> tags) {
    final String key = metric + tags;
    WritableDataPoints dp = datapoints.get(key);
    if (dp != null) {
      return dp;
    }
    dp = tsdb.newDataPoints();
    dp.setSeries(metric, tags);
    dp.setBatchImport(true);
    datapoints.put(key, dp);
    return dp;
  }

}
