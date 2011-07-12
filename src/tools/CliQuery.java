// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.HBaseClient;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.Query;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Tags;
import net.opentsdb.core.TSDB;
import net.opentsdb.graph.Plot;

final class CliQuery {

  private static final Logger LOG = LoggerFactory.getLogger(CliQuery.class);

  /** Prints usage and exits with the given retval.  */
  private static void usage(final ArgP argp, final String errmsg,
                            final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: query"
        + " [Gnuplot opts] START-DATE [END-DATE] <query> [queries...]\n"
        + "A query has the form:\n"
        + "  FUNC [rate] [downsample FUNC N] SERIES [TAGS]\n"
        + "For example:\n"
        + " 2010/03/11-20:57 sum my.awsum.metric host=blah"
        + " sum some.other.metric host=blah state=foo\n"
        + "Dates must follow this format: [YYYY/MM/DD-]HH:MM[:SS]\n"
        + "Supported values for FUNC: " + Aggregators.set()
        + "\nGnuplot options are of the form: +option=value");
    if (argp != null) {
      System.err.print(argp.usage());
    }
    System.exit(retval);
  }

  /** Parses the date in argument and returns a UNIX timestamp in seconds. */
  private static long parseDate(final String s) {
    SimpleDateFormat format;
    switch (s.length()) {
      case 5:
        format = new SimpleDateFormat("HH:mm");
        break;
      case 8:
        format = new SimpleDateFormat("HH:mm:ss");
        break;
      case 10:
        format = new SimpleDateFormat("yyyy/MM/dd");
        break;
      case 16:
        format = new SimpleDateFormat("yyyy/MM/dd-HH:mm");
        break;
      case 19:
        format = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
        break;
      default:
        usage(null, "Invalid date: " + s, 3);
        return -1; // Never executed as usage() exits.
    }
    try {
      return format.parse(s).getTime() / 1000;
    } catch (ParseException e) {
      usage(null, "Invalid date: " + s, 3);
      return -1; // Never executed as usage() exits.
    }
  }

  public static void main(String[] args) throws IOException {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    CliOptions.addVerbose(argp);
    argp.addOption("--graph", "BASEPATH",
                   "Output data points to a set of files for gnuplot."
                   + "  The path of the output files will start with"
                   + " BASEPATH.");
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, "Invalid usage.", 1);
    } else if (args.length < 3) {
      usage(argp, "Not enough arguments.", 2);
    }

    final HBaseClient client = CliOptions.clientFromOptions(argp);
    final TSDB tsdb = new TSDB(client, argp.get("--table", "tsdb"),
                               argp.get("--uidtable", "tsdb-uid"));
    final String basepath = argp.get("--graph");
    argp = null;

    Plot plot = null;
    try {
      plot = doQuery(tsdb, args, basepath != null);
    } finally {
      try {
        tsdb.shutdown().joinUninterruptibly();
      } catch (Exception e) {
        LOG.error("Unexpected exception", e);
        System.exit(1);
      }
    }

    if (plot != null) {
      try {
        final int npoints = plot.dumpToFiles(basepath);
        LOG.info("Wrote " + npoints + " for Gnuplot");
      } catch (IOException e) {
        LOG.error("Failed to write the Gnuplot file under " + basepath, e);
        System.exit(1);
      }
    }
  }

  private static Plot doQuery(final TSDB tsdb,
                              final String args[],
                              final boolean want_plot) {
    final ArrayList<String> plotparams = new ArrayList<String>();
    final ArrayList<Query> queries = new ArrayList<Query>();
    final ArrayList<String> plotoptions = new ArrayList<String>();
    parseCommandLineQuery(args, tsdb, queries, plotparams, plotoptions);
    if (queries.isEmpty()) {
      usage(null, "Not enough arguments, need at least one query.", 2);
    }

    final Plot plot = (want_plot ? new Plot(queries.get(0).getStartTime(),
                                            queries.get(0).getEndTime())
                       : null);
    if (want_plot) {
      plot.setParams(parsePlotParams(plotparams));
    }
    final int nqueries = queries.size();
    for (int i = 0; i < nqueries; i++) {
      // TODO(tsuna): Optimization: run each query in parallel.
      final StringBuilder buf = want_plot ? null : new StringBuilder();
      for (final DataPoints datapoints : queries.get(i).run()) {
        if (want_plot) {
          plot.add(datapoints, plotoptions.get(i));
        } else {
          final String metric = datapoints.metricName();
          final String tagz = datapoints.getTags().toString();
          for (final DataPoint datapoint : datapoints) {
            buf.append(metric)
               .append(' ')
               .append(datapoint.timestamp())
               .append(' ');
            if (datapoint.isInteger()) {
              buf.append(datapoint.longValue());
            } else {
              buf.append(String.format("%f", datapoint.doubleValue()));
            }
            buf.append(' ').append(tagz).append('\n');
            System.out.print(buf);
            buf.delete(0, buf.length());
          }
        }
      }
    }
    return plot;
  }

  /**
   * Parses the query from the command lines.
   * @param args The command line arguments.
   * @param tsdb The TSDB to use.
   * @param queries The list in which {@link Query}s will be appended.
   * @param plotparams The list in which global plot parameters will be
   * appended.  Ignored if {@code null}.
   * @param plotoptions The list in which per-line plot options will be
   * appended.  Ignored if {@code null}.
   */
  static void parseCommandLineQuery(final String[] args,
                                    final TSDB tsdb,
                                    final ArrayList<Query> queries,
                                    final ArrayList<String> plotparams,
                                    final ArrayList<String> plotoptions) {
    final long start_ts = parseDate(args[0]);
    final long end_ts = (args.length > 3
                         && (args[1].charAt(0) != '+'
                             && (args[1].indexOf(':') >= 0
                                 || args[1].indexOf('/') >= 0))
                         ? parseDate(args[1]) : -1);

    int i = end_ts < 0 ? 1 : 2;
    while (i < args.length && args[i].charAt(0) == '+') {
      if (plotparams != null) {
        plotparams.add(args[i]);
      }
      i++;
    }

    while (i < args.length) {
      final Aggregator agg = Aggregators.get(args[i++]);
      final boolean rate = args[i].equals("rate");
      if (rate) {
        i++;
      }
      final boolean downsample = args[i].equals("downsample");
      if (downsample) {
        i++;
      }
      final int interval = downsample ? Integer.parseInt(args[i++]) : 0;
      final Aggregator sampler = downsample ? Aggregators.get(args[i++]) : null;
      final String metric = args[i++];
      final HashMap<String, String> tags = new HashMap<String, String>();
      while (i < args.length && args[i].indexOf(' ', 1) < 0
             && args[i].indexOf('=', 1) > 0) {
        Tags.parse(tags, args[i++]);
      }
      if (i < args.length && args[i].indexOf(' ', 1) > 0) {
        plotoptions.add(args[i++]);
      }
      final Query query = tsdb.newQuery();
      query.setStartTime(start_ts);
      if (end_ts > 0) {
        query.setEndTime(end_ts);
      }
      query.setTimeSeries(metric, tags, agg, rate);
      if (downsample) {
        query.downsample(interval, sampler);
      }
      queries.add(query);
    }
  }

  private static HashMap<String, String> parsePlotParams(final ArrayList<String> params) {
    final HashMap<String, String> result =
      new HashMap<String, String>(params.size());
    for (final String param : params) {
      Tags.parse(result, param.substring(1));
    }
    return result;
  }

}
