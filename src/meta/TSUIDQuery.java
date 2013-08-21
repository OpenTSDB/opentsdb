// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.meta;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.Internal;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;

import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

/**
 * Methods for querying the tsdb-meta table. This can be used to figure out what
 * time series are actually stored as well as fetch TSMeta objects or optimize
 * queries against the data table.
 * @since 2.1
 */
public class TSUIDQuery {
  private static final Logger LOG = LoggerFactory.getLogger(TSUIDQuery.class);
  
  /**
   * Charset to use with our server-side row-filter.
   * We use this one because it preserves every possible byte unchanged.
   */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");

  /** ID of the metric being looked up. */
  private byte[] metric;

  /**
   * Tags of the metrics being looked up.
   * Each tag is a byte array holding the ID of both the name and value
   * of the tag.
   * Invariant: an element cannot be both in this array and in group_bys.
   */
  private ArrayList<byte[]> tags;

  /** The TSDB we belong to. */
  private final TSDB tsdb;
  
  /**
   * Constructor.
   * @param tsdb The TSDB to use for storage access
   */
  public TSUIDQuery(final TSDB tsdb) {
    this.tsdb = tsdb;
  }
  
  /**
   * Sets the query to perform
   * @param metric Name of the metric to search for
   * @param tags A map of tag value pairs or simply an empty map
   * @throws NoSuchUniqueName if the metric or any of the tag names/values did
   * not exist
   */
  public void setQuery(final String metric, final HashMap<String, String> tags) {
    this.metric = tsdb.getUID(UniqueIdType.METRIC, metric);
    this.tags = Tags.resolveAll(tsdb, tags);
  }
  
  /**
   * Fetches a list of TSUIDs given the metric and optional tag pairs. The query
   * format is similar to TsdbQuery but doesn't support grouping operators for 
   * tags. Only TSUIDs that had "ts_counter" qualifiers will be returned.
   * @return A map of TSUIDs to the last timestamp (in milliseconds) when the
   * "ts_counter" was updated. Note that the timestamp will be the time stored
   * by HBase, not the actual timestamp of the data point
   * @throws IllegalArgumentException if the metric was not set or the tag map
   * was null
   */
  public Deferred<ByteMap<Long>> getLastWriteTimes() {
    // we need at least a metric name and the tags can't be null. Empty tags are
    // fine, but the map can't be null.
    if (metric == null || metric.length < 0) {
      throw new IllegalArgumentException("Missing metric UID");
    }
    if (tags == null) {
      throw new IllegalArgumentException("Tag map was null");
    }
    
    final Scanner scanner = getScanner();
    scanner.setQualifier(TSMeta.COUNTER_QUALIFIER());
    final Deferred<ByteMap<Long>> results = new Deferred<ByteMap<Long>>();
    final ByteMap<Long> tsuids = new ByteMap<Long>();
    
    /**
     * Scanner callback that will call itself while iterating through the 
     * tsdb-meta table
     */
    final class ScannerCB implements Callback<Object,
      ArrayList<ArrayList<KeyValue>>> {
      
      /**
       * Starts the scanner and is called recursively to fetch the next set of
       * rows from the scanner.
       * @return The map of spans if loaded successfully, null if no data was
       * found
       */
      public Object scan() {
        return scanner.nextRows().addCallback(this);
      }
      
      /**
       * Loops through each row of the scanner results and parses out data
       * points and optional meta data
       * @return null if no rows were found, otherwise the TreeMap with spans
       */
      @Override
      public Object call(final ArrayList<ArrayList<KeyValue>> rows)
        throws Exception {
        try {
          if (rows == null) {
            results.callback(tsuids);
            return null;
          }
          
          for (final ArrayList<KeyValue> row : rows) {
            final byte[] tsuid = row.get(0).key();
            tsuids.put(tsuid, row.get(0).timestamp());
          }
          return scan();
        } catch (Exception e) {
          results.callback(e);
          return null;
        }
      }
    }
    
    new ScannerCB().scan();
    return results;
  }
  
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("TSUIDQuery(metric=")
       .append(Arrays.toString(metric));
    try {
      buf.append("), tags=").append(Tags.resolveIds(tsdb, tags));
    } catch (NoSuchUniqueId e) {
      buf.append("), tags=<").append(e.getMessage()).append('>');
    }
    buf.append("))");
    return buf.toString();
  }
  
  /**
   * Attempts to retrieve the last data point for the given TSUID. 
   * This operates by checking the meta table for the {@link #COUNTER_QUALIFIER}
   * and if found, parses the HBase timestamp for the counter (i.e. the time when
   * the counter was written) and tries to load the row in the data table for
   * the hour where that timestamp would have landed. If the counter does not
   * exist or the data row doesn't exist or is empty, the results will be a null
   * IncomingDataPoint. 
   * <b>Note:</b> This will be accurate most of the time since the counter will
   * be updated at the time a data point is written within a second or so. 
   * However if the user is writing historical data, putting data with older 
   * timestamps or performing an import, then the counter timestamp will be
   * inaccurate.
   * @param tsdb The TSDB to which we belong
   * @param tsuid TSUID to fetch
   * @param resolve_names Whether or not to resolve the TSUID to names and 
   * return them in the IncomingDataPoint
   * @param max_lookups If set to a positive integer, will look "max_lookups"
   * hours into the past for a valid data point starting with the current hour. 
   * Setting this to 0 will only use the TSUID counter timestamp.
   * @param last_timestamp Optional timestamp in milliseconds when the last data
   * point was written
   * @return An {@link IncomingDataPoint} if data was found, null if not
   * @throws NoSuchUniqueId if one of the tag lookups failed
   */
  public static Deferred<IncomingDataPoint> getLastPoint(final TSDB tsdb, 
      final byte[] tsuid, final boolean resolve_names, final int max_lookups, 
      final long last_timestamp) {
   
    final Deferred<IncomingDataPoint> result = new Deferred<IncomingDataPoint>();
    final long start_time;
    final long time_limit;
    if (max_lookups < 1) {
      start_time = 0;
      time_limit = 0;
    } else { 
      start_time = System.currentTimeMillis() / 1000;
      time_limit = start_time - (3600 * max_lookups);
    }

    final class ErrBack implements Callback<Object, Exception> {
      public Object call(final Exception e) throws Exception {
        Throwable ex = e;
        while (ex.getClass().equals(DeferredGroupException.class)) {
          if (ex.getCause() == null) {
            LOG.warn("Unable to get to the root cause of the DGE");
            break;
          }
          ex = ex.getCause();
        }
        if (ex instanceof RuntimeException) {
          result.callback(ex);
        } else {
          result.callback(e);
        }
        return null;
      }  
    }
    
    /**
     * Called after GetLastDataPointCB has completed. If nothing was found then
     * we just return a null, otherwise we set the TSUID and optionally resolve
     * the metric and tag names.
     */
    final class ReturnCB implements Callback<Object, IncomingDataPoint> {
      final long timestamp;
      
      public ReturnCB(final long timestamp) {
        this.timestamp = timestamp;
      }
      
      /**
       * Callback implementation. If the time_limit was set and the result was
       * null we call the local getPrevious() method to issue a Get on the 
       * previous row.
       */
      public Object call(final IncomingDataPoint dp) 
        throws Exception {
        if (dp == null) {
          if (time_limit > 0) {
            getPrevious();
            return null;
          }
          result.callback(null);
          return null;
        }
       
        dp.setTSUID(UniqueId.uidToString(tsuid));
        if (!resolve_names) {
          result.callback(dp);
          return null;
        }
        
        class TagsCB implements Callback<Object, HashMap<String, String>> {
          public IncomingDataPoint call(final HashMap<String, String> tags) 
            throws Exception {
            dp.setTags(tags);
            result.callback(dp);
            return null;
          }
        }
        
        class MetricCB implements Callback<Object, String> {
          public Object call(final String name) throws Exception {
            dp.setMetric(name);
            final List<byte[]> tags = UniqueId.getTagPairsFromTSUID(tsuid);
            return Tags.resolveIdsAsync(tsdb, tags).addCallback(new TagsCB());
          }
        }
        
        // start the resolve dance
        final byte[] metric_uid = Arrays.copyOfRange(tsuid, 0, TSDB.metrics_width());     
        return tsdb.getUidName(UniqueIdType.METRIC, metric_uid)
          .addCallback(new MetricCB());
      }
      
      /**
       * Issues a GetRequest on the previous hour's row of data if we haven't 
       * exceeded the limit
       * @return Null if we've hit the time limit or another deferred to wait
       * on 
       */
      private void getPrevious() {
        if (timestamp <= time_limit) {
          // we hit our limit and didn't find a valid data point
          result.callback(null);
          return;
        }
        
        // look one row into the past
        final long previous_time = timestamp - 3600;
        
        final byte[] key = RowKey.rowKeyFromTSUID(tsdb, tsuid, previous_time);
        final GetRequest get = new GetRequest(tsdb.dataTable(), key);
        get.family(TSDB.FAMILY());
        tsdb.getClient().get(get).addCallback(
            new Internal.GetLastDataPointCB(tsdb))
            .addCallback(new ReturnCB(previous_time))
            .addErrback(new ErrBack());
      }
    }
    
    /**
     * Callback from the GetRequest that simply determines if the row is empty
     * or not
     */
    final class ExistsCB implements Callback<Object, ArrayList<KeyValue>> {
      public Object call(final ArrayList<KeyValue> row) 
        throws Exception {
        if (row == null || row.isEmpty() || row.get(0).value() == null) {
          result.callback(null);
          return null;
        }
        
        // we want the timestamp in seconds so we can figure out what row the
        // data *should* be in (though it's not guaranteed)
        final long last_write = row.get(0).timestamp();
        final byte[] key = RowKey.rowKeyFromTSUID(tsdb, tsuid, last_write);
        final GetRequest get = new GetRequest(tsdb.dataTable(), key);
        get.family(TSDB.FAMILY());
        
        tsdb.getClient().get(get).addCallback(
            new Internal.GetLastDataPointCB(tsdb))
            .addCallback(new ReturnCB(0))
            .addErrback(new ErrBack());
        return null;
      }
    }
    
    // we need to determine a course of action. We can either:
    // 1) Lookup the last time a data point was written for a TSUID
    // 2) Immediately fetch data from a row if we were given a timestamp
    // 3) Or start at NOW and iterate backwards until we find a point or hit
    //    the user supplied limit
    if (time_limit == 0) {
      if (last_timestamp < 1) {
        final GetRequest get = new GetRequest(tsdb.metaTable(), tsuid);
        get.family(TSMeta.FAMILY());
        get.qualifier(TSMeta.COUNTER_QUALIFIER());
        tsdb.getClient().get(get)
          .addCallback(new ExistsCB())
          .addErrback(new ErrBack());
      } else {
        final byte[] key = RowKey.rowKeyFromTSUID(tsdb, tsuid, last_timestamp);
        final GetRequest get = new GetRequest(tsdb.dataTable(), key);
        get.family(TSDB.FAMILY());
        
        tsdb.getClient().get(get).addCallback(
            new Internal.GetLastDataPointCB(tsdb))
            .addCallback(new ReturnCB(0))
            .addErrback(new ErrBack());
      }
    } else {
      final byte[] key = RowKey.rowKeyFromTSUID(tsdb, tsuid, start_time);
      final GetRequest get = new GetRequest(tsdb.dataTable(), key);
      get.family(TSDB.FAMILY());
      tsdb.getClient().get(get).addCallback(
          new Internal.GetLastDataPointCB(tsdb))
          .addCallback(new ReturnCB(start_time))
          .addErrback(new ErrBack());
    }
    
    return result;
  } 
  
  /**
   * Configures the scanner for a specific metric and optional tags
   * @return A configured scanner
   */
  private Scanner getScanner() {
    final Scanner scanner = tsdb.getClient().newScanner(tsdb.metaTable());
    scanner.setStartKey(metric);
    
    // increment the metric UID by one so we can scan all of the rows for the
    // given metric
    final long stop = UniqueId.uidToLong(metric, TSDB.metrics_width()) + 1;
    scanner.setStopKey(UniqueId.longToUID(stop, TSDB.metrics_width()));
    scanner.setFamily(TSMeta.FAMILY());
    
    // set the filter if we have tags
    if (!tags.isEmpty()) {
      final short name_width = TSDB.tagk_width();
      final short value_width = TSDB.tagv_width();
      final short tagsize = (short) (name_width + value_width);
      // Generate a regexp for our tags.  Say we have 2 tags: { 0 0 1 0 0 2 }
      // and { 4 5 6 9 8 7 }, the regexp will be:
      // "^.{7}(?:.{6})*\\Q\000\000\001\000\000\002\\E(?:.{6})*\\Q\004\005\006\011\010\007\\E(?:.{6})*$"
      final StringBuilder buf = new StringBuilder(
          15  // "^.{N}" + "(?:.{M})*" + "$"
          + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
             * (tags.size())));

      // Alright, let's build this regexp.  From the beginning...
      buf.append("(?s)"  // Ensure we use the DOTALL flag.
                 + "^.{")
         // ... start by skipping the metric ID.
         .append(TSDB.metrics_width())
         .append("}");
      final Iterator<byte[]> tags = this.tags.iterator();
      byte[] tag = tags.hasNext() ? tags.next() : null;
      // Tags and group_bys are already sorted.  We need to put them in the
      // regexp in order by ID, which means we just merge two sorted lists.
      do {
        // Skip any number of tags.
        buf.append("(?:.{").append(tagsize).append("})*\\Q");
        UniqueId.addIdToRegexp(buf, tag);
        tag = tags.hasNext() ? tags.next() : null;
      } while (tag != null);  // Stop when they both become null.
      // Skip any number of tags before the end.
      buf.append("(?:.{").append(tagsize).append("})*$");
      scanner.setKeyRegexp(buf.toString(), CHARSET);
    }
    
    return scanner;
  }
}
