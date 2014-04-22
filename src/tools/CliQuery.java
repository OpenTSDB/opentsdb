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
import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.Query;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.RateOptions;
import net.opentsdb.core.Tags;
import net.opentsdb.core.TSDB;
import net.opentsdb.graph.Plot;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

final class CliQuery {

  private static final Logger LOG = LoggerFactory.getLogger(CliQuery.class);

  /** Prints usage and exits with the given retval.  */
  private static void usage(final ArgP argp, final String errmsg,
                            final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: query"
        + " [Gnuplot opts] START-DATE [END-DATE] <query> [queries...]\n"
        + "A query has the form:\n"
        + "  FUNC [rate] [counter,max,reset] [downsample FUNC N] SERIES [TAGS]\n"
        + "For example:\n"
        + " 2010/03/11-20:57 sum my.awsum.metric host=blah"
        + " sum some.other.metric host=blah state=foo\n"
        + "Dates must follow this format: YYYY/MM/DD-HH:MM[:SS] or Unix Epoch\n"
        + " or relative time such as 1y-ago, 2d-ago, etc.\n"
        + "Supported values for FUNC: " + Aggregators.set()
        + "\nGnuplot options are of the form: +option=value");
    if (argp != null) {
      System.err.print(argp.usage());
    }
    System.exit(retval);
  }

  public static void main(String[] args) throws Exception {
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

    // get a config object
    Config config = CliOptions.getConfig(argp);
    
    final TSDB tsdb = new TSDB(config);
    tsdb.checkNecessaryTablesExist().joinUninterruptibly();
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
    long start_ts = DateTime.parseDateTimeString(args[0], null);
    if (start_ts >= 0)
      start_ts /= 1000;
    long end_ts = -1;
    if (args.length > 3){
      // see if we can detect an end time
      try{
      if (args[1].charAt(0) != '+'
           && (args[1].indexOf(':') >= 0
               || args[1].indexOf('/') >= 0
               || args[1].indexOf('-') >= 0
               || Long.parseLong(args[1]) > 0)){
          end_ts = DateTime.parseDateTimeString(args[1], null);
        }
      }catch (NumberFormatException nfe) {
        // ignore it as it means the third parameter is likely the aggregator
      }
    }
    // temp fixup to seconds from ms until the rest of TSDB supports ms
    // Note you can't append this to the DateTime.parseDateTimeString() call as
    // it clobbers -1 results
    if (end_ts >= 0)
      end_ts /= 1000;

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
      RateOptions rate_options = new RateOptions(false, Long.MAX_VALUE,
          RateOptions.DEFAULT_RESET_VALUE);
      if (rate) {
        i++;
        
        long counterMax = Long.MAX_VALUE;
        long resetValue = RateOptions.DEFAULT_RESET_VALUE;
        if (args[i].startsWith("counter")) {
          String[] parts = Tags.splitString(args[i], ',');
          if (parts.length >= 2 && parts[1].length() > 0) {
            counterMax = Long.parseLong(parts[1]);
          }
          if (parts.length >= 3 && parts[2].length() > 0) {
            resetValue = Long.parseLong(parts[2]);
          }
          rate_options = new RateOptions(true, counterMax, resetValue);
          i++;
        }
      }
      final boolean downsample = args[i].equals("downsample");
      if (downsample) {
        i++;
      }
      final long interval = downsample ? Long.parseLong(args[i++]) : 0;
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
      query.setTimeSeries(metric, tags, agg, rate, rate_options);
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
