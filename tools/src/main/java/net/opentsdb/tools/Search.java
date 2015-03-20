// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import dagger.ObjectGraph;
import net.opentsdb.core.TsdbModule;
import net.opentsdb.storage.hbase.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.search.SearchQuery.SearchType;
import net.opentsdb.uid.IdUtils;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UidFormatter;
import net.opentsdb.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles searching from the command line. Enables lookups of time series
 * information given a metric, tagk, tagv or combination thereof
 */
final class Search {
  private static final Logger LOG = LoggerFactory.getLogger(Search.class);
  
  /** Prints usage. */
  static void usage(final ArgP argp, final String errmsg) {
    System.err.println(errmsg);
    System.err.println("Usage: search <subcommand> args\n"
        + "Sub commands:\n"
        + "  lookup <query>: Retreives a list of time series with the given\n"
        + "                  metric, tagk, tagv or any combination thereof.\n");
    if (argp != null) {
      System.err.print(argp.usage());
    }
  }
  
  /**
   * Entry point to run the search utility
   * @param args Command line arguments
   * @throws Exception If something goes wrong
   */
  public static void main(String[] args) throws Exception {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    argp.addOption("--use-data-table",
        "Scan against the raw data table instead of the meta data table.");
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, "Invalid usage");
      System.exit(2);
    } else if (args.length < 1) {
      usage(argp, "Not enough arguments");
      System.exit(2);
    }
    
    final boolean use_data_table = argp.has("--use-data-table");

    final String defaultConfig = new File(System.getProperty("app.home"), "opentsdb").getPath();
    ObjectGraph objectGraph = ObjectGraph.create(new TsdbModule(argp.get("--config", defaultConfig)));
    final TSDB tsdb = objectGraph.get(TSDB.class);
    
    int rc;
    try {
      rc = runCommand(tsdb, use_data_table, args);
    } finally {
      try {
        tsdb.getTsdbStore().shutdown().joinUninterruptibly();
        LOG.info("Gracefully shutdown the TSD");
      } catch (Exception e) {
        LOG.error("Unexpected exception while shutting down", e);
        rc = 42;
      }
    }
    System.exit(rc);
  }
  
  /**
   * Determines the command requested of the user can calls the appropriate
   * method.
   * @param tsdb The TSDB to use for communication
   * @param use_data_table Whether or not lookups should be done on the full
   * data table
   * @param args Arguments to parse
   * @return An exit code
   */
  private static int runCommand(final TSDB tsdb,
                                final boolean use_data_table,
                                final String[] args) throws Exception {
    final int nargs = args.length;
    if (args[0].equals("lookup")) {
      if (nargs < 2) { // need a query
        usage(null, "Not enough arguments");
        return 2;
      }
      return lookup(tsdb, use_data_table, args);
    } else {
      usage(null, "Unknown sub command: " + args[0]);
      return 2;
    }
  }
  
  /**
   * Performs a time series lookup given a query like "metric tagk=tagv" where
   * a list of all time series containing the given metric and tag pair will be
   * dumped to standard out. Tag pairs can be given with empty tagk or tagvs to
   * and the metric is option. E.g. a query of "=web01" will return all time 
   * series with a tag value of "web01".
   * By default the lookup is performed against the tsdb-meta table. If the 
   * "--use_data_table" flag is supplied, the main data table will be scanned.
   * @param tsdb The TSDB to use for communication
   * @param use_data_table Whether or not lookups should be done on the full
   * data table
   * @param args Arguments to parse
   * @return An exit code
   */
  private static int lookup(final TSDB tsdb,
                            final boolean use_data_table,
                            final String[] args) throws Exception {
    final SearchQuery query = new SearchQuery();
    query.setType(SearchType.LOOKUP);
    
    int index = 1;
    if (!args[index].contains("=")) {
      query.setMetric(args[index++]);
    }
    
    final List<Pair<String, String>> tags = 
        new ArrayList<Pair<String, String>>(args.length - index);
    for (; index < args.length; index++) {
      Tags.parse(tags, args[index]);
    }
    query.setTags(tags);

    Iterable<byte[]> tsuids = tsdb.getUniqueIdClient()
        .executeTimeSeriesQuery(query).joinUninterruptibly();

    UidFormatter formatter = new UidFormatter(tsdb);

    for (final byte[] tsuid : tsuids) {
      try {
        final StringBuffer buf = new StringBuffer(2048);
        buf.append(IdUtils.uidToString(tsuid)).append(" ");

        byte[] metric_id = RowKey.metric(tsuid);
        buf.append(formatter.formatMetric(metric_id).joinUninterruptibly());
        buf.append(" ");

        final List<byte[]> tag_ids = IdUtils.getTagPairsFromTSUID(tsuid);
        final Map<String, String> resolved_tags =
            tsdb.getUniqueIdClient().getTagNames(tag_ids).joinUninterruptibly();

        for (final Map.Entry<String, String> tag_pair : resolved_tags.entrySet()) {
          buf.append(tag_pair.getKey()).append("=")
              .append(tag_pair.getValue()).append(" ");
        }

        System.out.println(buf);
      } catch (NoSuchUniqueId nsui) {
        LOG.error("Unable to resolve UID in TSUID ({}) {}", IdUtils.uidToString(tsuid), nsui.getMessage());
      }
    }

    return 0;
  }
}
