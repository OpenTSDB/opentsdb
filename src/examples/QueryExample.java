// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

/**
 * One example on how to query.
 * Taken from 
 * <a href="https://groups.google.com/forum/#!searchin/opentsdb/java$20api/opentsdb/6MKs-FkSLoA/gifHF327CIAJ">
 * this thread</a>
 * The metric and key query arguments assume that you've input data from the Quick Start tutorial
 * <a href="http://opentsdb.net/docs/build/html/user_guide/quickstart.html">here.</a>
 */
public class QueryExample {

  public static void main(final String[] args) throws IOException {
    
    // Set these as arguments so you don't have to keep path information in
    // source files 
    String pathToConfigFile = (args != null && args.length > 0 ? args[0] : null);
    
    // Create a config object with a path to the file for parsing. Or manually
    // override settings.
    // e.g. config.overrideConfig("tsd.storage.hbase.zk_quorum", "localhost");
    final Config config;
    if (pathToConfigFile != null && !pathToConfigFile.isEmpty()) {
      config = new Config(pathToConfigFile);
    } else {
      // Search for a default config from /etc/opentsdb/opentsdb.conf, etc.
      config = new Config(true);
    }
    final TSDB tsdb = new TSDB(config);
    
    // main query
    final TSQuery query = new TSQuery();
    
    // use any string format from
    // http://opentsdb.net/docs/build/html/user_guide/query/dates.html
    query.setStart("1h-ago");
    // Optional: set other global query params

    // at least one sub query required. This is where you specify the metric and
    // tags
    final TSSubQuery subQuery = new TSSubQuery();
    subQuery.setMetric("my.tsdb.test.metric");

    // filters are optional but useful.
    final List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
    filters.add(new TagVFilter.Builder()
        .setType("literal_or")
        .setFilter("example1")
        .setTagk("script")
        .setGroupBy(true)
        .build());
    subQuery.setFilters(filters);
    
    // you do have to set an aggregator. Just provide the name as a string
    subQuery.setAggregator("sum");

    // IMPORTANT: don't forget to add the subQuery
    final ArrayList<TSSubQuery> subQueries = new ArrayList<TSSubQuery>(1);
    subQueries.add(subQuery);
    query.setQueries(subQueries);
    query.setMsResolution(true); // otherwise we aggregate on the second. 

    // make sure the query is valid. This will throw exceptions if something
    // is missing
    query.validateAndSetQuery();

    // compile the queries into TsdbQuery objects behind the scenes
    Query[] tsdbqueries = query.buildQueries(tsdb);

    // create some arrays for storing the results and the async calls
    final int nqueries = tsdbqueries.length;
    final ArrayList<DataPoints[]> results = new ArrayList<DataPoints[]>(
        nqueries);
    final ArrayList<Deferred<DataPoints[]>> deferreds = 
        new ArrayList<Deferred<DataPoints[]>>(nqueries);

    // this executes each of the sub queries asynchronously and puts the
    // deferred in an array so we can wait for them to complete.
    for (int i = 0; i < nqueries; i++) {
      deferreds.add(tsdbqueries[i].runAsync());
    }

    // Start timer
    long startTime = DateTime.nanoTime();
    
    // This is a required callback class to store the results after each
    // query has finished
    class QueriesCB implements Callback<Object, ArrayList<DataPoints[]>> {
      public Object call(final ArrayList<DataPoints[]> queryResults)
          throws Exception {
        results.addAll(queryResults);
        return null;
      }
    }
    
    // Make sure to handle any errors that might crop up
    class QueriesEB implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception e) throws Exception {
        System.err.println("Queries failed");
        e.printStackTrace();
        return null;
      }
    }

    // this will cause the calling thread to wait until ALL of the queries
    // have completed.
    try {
      Deferred.groupInOrder(deferreds)
          .addCallback(new QueriesCB())
          .addErrback(new QueriesEB())
          .join();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // End timer.
    double elapsedTime = DateTime.msFromNanoDiff(DateTime.nanoTime(), startTime);
    System.out.println("Query returned in: " + elapsedTime + " milliseconds.");
    
    // now all of the results are in so we just iterate over each set of
    // results and do any processing necessary.
    for (final DataPoints[] dataSets : results) {
      for (final DataPoints data : dataSets) {
        System.out.print(data.metricName());
        Map<String, String> resolvedTags = data.getTags();
        for (final Map.Entry<String, String> pair : resolvedTags.entrySet()) {
          System.out.print(" " + pair.getKey() + "=" + pair.getValue());
        }
        System.out.print("\n");

        final SeekableView it = data.iterator();
        /*
         * An important point about SeekableView:
         * Because no data is copied during iteration and no new object gets
         * created, the DataPoint returned must not be stored and gets
         * invalidated as soon as next is called on the iterator (actually it
         * doesn't get invalidated but rather its contents changes). If you want
         * to store individual data points, you need to copy the timestamp and
         * value out of each DataPoint into your own data structures.
         * 
         * In the vast majority of cases, the iterator will be used to go once
         * through all the data points, which is why it's not a problem if the
         * iterator acts just as a transient "view". Iterating will be very
         * cheap since no memory allocation is required (except to instantiate
         * the actual iterator at the beginning).
         */
        while (it.hasNext()) {
          final DataPoint dp = it.next();
          System.out.println("  " + dp.timestamp() + " "
              + (dp.isInteger() ? dp.longValue() : dp.doubleValue()));
        }
        System.out.println("");
      }
    }
    
    // Gracefully shutdown connection to TSDB
    try {
      tsdb.shutdown().join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}