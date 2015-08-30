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
import java.util.Map;

import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.Internal;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.DateTime;

import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * Methods for querying the tsdb-meta table or finding the last data point of
 * a particular time series. This can be used to figure out what
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

  /** The TSUID that can be set by the caller or after processing the metric */
  private byte[] tsuid;
  
  /** The metric set by the caller */
  private String metric;
  
  /** The metric UID after lookup */
  private byte[] metric_uid;
  
  /** The tags set by the caller */
  private Map<String, String> tags;
  
  /** The tag UID list after lookup */
  private ArrayList<byte[]> tag_uids;
  
  /** Whether or not to resolve names for last data point queries */
  private boolean resolve_names;
  
  /** How far back, in hours, to scan for last data point queries */
  private int back_scan;
  
  /** The last timestamp scanned for last data point queries */
  private long last_timestamp;
  
  /** The TSDB we belong to. */
  private final TSDB tsdb;
  
  /**
   * Default CTor just sets the TSDB reference
   * @param tsdb The TSDB to use for storage access
   * @throws IllegalArgumentException if the TSDB reference is null
   * @deprecated Please use one of the other constructors. Will be removed in 2.3
   */
  public TSUIDQuery(final TSDB tsdb) {
    if (tsdb == null) {
      throw new IllegalArgumentException("TSDB reference cannot be null");
    }
    this.tsdb =tsdb;
  }
  
  /**
   * CTor used for a TSUID based query when we know exactly what we want
   * @param tsdb The TSDB to use for storage access
   * @param tsuid A TSUID to use for querying
   * @throws IllegalArgumentException if the TSUID is invalid
   */
  public TSUIDQuery(final TSDB tsdb, final byte[] tsuid) {
    if (tsdb == null) {
      throw new IllegalArgumentException("TSDB reference cannot be null");
    }
    if (tsuid == null || tsuid.length < 
        TSDB.metrics_width() + TSDB.tagk_width() + TSDB.tagv_width()) {
      throw new IllegalArgumentException("TSUID must not be null and must "
          + "have a metric and at least one tag pair");
    }
    this.tsdb = tsdb;
    this.tsuid = tsuid;
  }
  
  /**
   * CTor used for a metric style query
   * @param tsdb The TSDB to use for storage access
   * @param metric The metric to look up
   * @param tags The tags to lookup. This may be an empty map if you're scanning
   * meta.
   * @throws IllegalArgumentException if the metric is null, empty or the tag
   * map is null. 
   */
  public TSUIDQuery(final TSDB tsdb, final String metric, 
      final Map<String, String> tags) {
    if (tsdb == null) {
      throw new IllegalArgumentException("TSDB reference cannot be null");
    }
    if (metric == null || metric.isEmpty()) {
      throw new IllegalArgumentException("Metric cannot be null or empty");
    }
    if (tags == null) {
      throw new IllegalArgumentException("Tag map cannot be null. Empty is ok");
    }
    this.tsdb = tsdb;
    this.metric = metric;
    this.tags = tags;
  }
  
  /**
   * Attempts to fetch the last data point for the given metric or TSUID.
   * If back_scan == 0 and meta is enabled via 
   * "tsd.core.meta.enable_tsuid_tracking" or 
   * "tsd.core.meta.enable_tsuid_incrementing" then we will look up the metric
   * or TSUID in the meta table first and use the counter there to get the
   * last write time.
   * <p>
   * However if backscan is set, then we'll start with the current time and 
   * iterate back "back_scan" number of hours until we find a value.
   * <p>
   * @param resolve_names Whether or not to resolve the UIDs back to their
   * names when we find a value.
   * @param back_scan The number of hours back in time to scan
   * @return A data point if found, null if not. Or an exception if something
   * went pear shaped.
   */
  public Deferred<IncomingDataPoint> getLastPoint(final boolean resolve_names, 
      final int back_scan) {
    if (back_scan < 0) {
      throw new IllegalArgumentException(
          "Backscan must be zero or a positive number");
    }
    
    this.resolve_names = resolve_names;
    this.back_scan = back_scan;
    
    final boolean meta_enabled = tsdb.getConfig().enable_tsuid_tracking() ||
        tsdb.getConfig().enable_tsuid_incrementing();
    
    class TSUIDCB implements Callback<Deferred<IncomingDataPoint>, byte[]> {
      @Override
      public Deferred<IncomingDataPoint> call(final byte[] incoming_tsuid) 
          throws Exception {
        if (tsuid == null && incoming_tsuid == null) {
          return Deferred.fromError(new RuntimeException("Both incoming and "
              + "supplied TSUIDs were null for " + TSUIDQuery.this));
        } else if (incoming_tsuid != null) {
          setTSUID(incoming_tsuid);
        }
        if (back_scan < 1 && meta_enabled) {
          final GetRequest get = new GetRequest(tsdb.metaTable(), tsuid);
          get.family(TSMeta.FAMILY());
          get.qualifier(TSMeta.COUNTER_QUALIFIER());
          return tsdb.getClient().get(get).addCallbackDeferring(new MetaCB());
        }
        
        if (last_timestamp > 0) {
          last_timestamp = Internal.baseTime(last_timestamp);
        } else {
          last_timestamp = Internal.baseTime(DateTime.currentTimeMillis());
        }
        final byte[] key = RowKey.rowKeyFromTSUID(tsdb, tsuid, last_timestamp);
        final GetRequest get = new GetRequest(tsdb.dataTable(), key);
        get.family(TSDB.FAMILY());
        return tsdb.getClient().get(get).addCallbackDeferring(new LastPointCB());
      }
      @Override
      public String toString() {
        return "TSUID callback";
      }
    }
    
    if (tsuid == null) {
      return tsuidFromMetric(tsdb, metric, tags)
          .addCallbackDeferring(new TSUIDCB());
    }
    try {
      // damn typed exceptions....
      return new TSUIDCB().call(null);
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }
  
  /**
   * Sets the query to perform
   * @param metric Name of the metric to search for
   * @param tags A map of tag value pairs or simply an empty map
   * @throws NoSuchUniqueName if the metric or any of the tag names/values did
   * not exist
   * @deprecated Please use one of the constructors instead. Will be removed in 2.3
   */
  public void setQuery(final String metric, final Map<String, String> tags) {
    this.metric = metric;
    this.tags = tags;
    metric_uid = tsdb.getUID(UniqueIdType.METRIC, metric);
    tag_uids = Tags.resolveAll(tsdb, tags);
  }
  
  /**
   * Fetches a list of TSUIDs given the metric and optional tag pairs. The query
   * format is similar to TsdbQuery but doesn't support grouping operators for 
   * tags. Only TSUIDs that had "ts_counter" qualifiers will be returned.
   * <p>
   * NOTE: If you called {@link #setQuery(String, Map)} successfully this will
   * immediately scan the meta table. But if you used the CTOR to set the
   * metric and tags it will attempt to resolve those and may return an exception.
   * @return A map of TSUIDs to the last timestamp (in milliseconds) when the
   * "ts_counter" was updated. Note that the timestamp will be the time stored
   * by HBase, not the actual timestamp of the data point. If nothing was
   * found, the map will be empty but not null.
   * @throws IllegalArgumentException if the metric was not set or the tag map
   * was null
   */
  public Deferred<ByteMap<Long>> getLastWriteTimes() {
    class ResolutionCB implements Callback<Deferred<ByteMap<Long>>, Object> {
      @Override
      public Deferred<ByteMap<Long>> call(Object arg0) throws Exception {
        final Scanner scanner = getScanner();
        scanner.setQualifier(TSMeta.COUNTER_QUALIFIER());
        final Deferred<ByteMap<Long>> results = new Deferred<ByteMap<Long>>();
        final ByteMap<Long> tsuids = new ByteMap<Long>();
        
        final class ErrBack implements Callback<Object, Exception> {
          @Override
          public Object call(final Exception e) throws Exception {
            results.callback(e);
            return null;
          }
          @Override
          public String toString() {
            return "Error callback";
          }
        }
        
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
            return scanner.nextRows().addCallback(this).addErrback(new ErrBack());
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
      @Override
      public String toString() {
        return "Last counter time callback";
      }
    }
    
    if (metric_uid == null) {
      return resolveMetric().addCallbackDeferring(new ResolutionCB());
    }
    try {
      return new ResolutionCB().call(null);
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }
  
  /**
   * Returns all TSMeta objects stored for timeseries defined by this query. The 
   * query is similar to TsdbQuery without any aggregations. Returns an empty 
   * list, when no TSMetas are found. Only returns stored TSMetas.
   * <p>
   * NOTE: If you called {@link #setQuery(String, Map)} successfully this will
   * immediately scan the meta table. But if you used the CTOR to set the
   * metric and tags it will attempt to resolve those and may return an exception.
   * @return A list of existing TSMetas for the timeseries covered by the query.
   * @throws IllegalArgumentException When either no metric was specified or the
   * tag map was null (Empty map is OK).
   */
  public Deferred<List<TSMeta>> getTSMetas() {
    class ResolutionCB implements Callback<Deferred<List<TSMeta>>, Object> {
      @Override
      public Deferred<List<TSMeta>> call(final Object done) throws Exception {
        final Scanner scanner = getScanner();
        scanner.setQualifier(TSMeta.META_QUALIFIER());
        final Deferred<List<TSMeta>> results = new Deferred<List<TSMeta>>();
        final List<TSMeta> tsmetas = new ArrayList<TSMeta>();
        final List<Deferred<TSMeta>> tsmeta_group = new ArrayList<Deferred<TSMeta>>();
        
        final class TSMetaGroupCB implements Callback<Object,
          ArrayList<TSMeta>> {
          @Override
          public List<TSMeta> call(ArrayList<TSMeta> ts) throws Exception {
            for (TSMeta tsm: ts) {
              if (tsm != null) {
                tsmetas.add(tsm);
              }
            }
            results.callback(tsmetas);
            return null;
          }
          @Override
          public String toString() {
            return "TSMeta callback";
          }
        }
        
        final class ErrBack implements Callback<Object, Exception> {
          @Override
          public Object call(final Exception e) throws Exception {
            results.callback(e);
            return null;
          }
          @Override
          public String toString() {
            return "Error callback";
          }
        }

        /**
         * Scanner callback that will call itself while iterating through the 
         * tsdb-meta table.
         * 
         * Keeps track of a Set of Deferred TSMeta calls. When all rows are scanned,
         * will wait for all TSMeta calls to be completed and then create the result 
         * list.
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
            return scanner.nextRows().addCallback(this).addErrback(new ErrBack());
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
                Deferred.group(tsmeta_group)
                  .addCallback(new TSMetaGroupCB()).addErrback(new ErrBack());
                return null;
              }
              for (final ArrayList<KeyValue> row : rows) {
                tsmeta_group.add(TSMeta.parseFromColumn(tsdb, row.get(0), true));
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
      @Override
      public String toString() {
        return "TSMeta scan callback";
      }
    }
    
    if (metric_uid == null) {
      return resolveMetric().addCallbackDeferring(new ResolutionCB());
    }
    try {
      return new ResolutionCB().call(null);
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }
  
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("TSUIDQuery(metric=").append(metric)
       .append(", tags=").append(tags)
       .append(", tsuid=")
       .append(tsuid != null ? UniqueId.uidToString(tsuid) : "null")
       .append(", last_timestamp=").append(last_timestamp)
       .append(", back_scan=").append(back_scan)
       .append(", resolve_names=").append(resolve_names)
       .append(")");
    return buf.toString();
  }
  
  /**
   * Converts the given metric and tags to a TSUID by resolving the strings to
   * their UIDs. Note that the resulting TSUID may not exist if the combination
   * was not written to TSDB
   * @param tsdb The TSDB to use for storage access
   * @param metric The metric name to resolve
   * @param tags The tags to resolve. May not be empty.
   * @return A deferred containing the TSUID when ready or an error such as
   * a NoSuchUniqueName exception if the metric didn't exist or a
   * DeferredGroupException if one of the tag keys or values did not exist.
   * @throws IllegalArgumentException if the metric or tags were null or
   * empty.
   */
  public static Deferred<byte[]> tsuidFromMetric(final TSDB tsdb, 
      final String metric, final Map<String, String> tags) {
    if (metric == null || metric.isEmpty()) {
      throw new IllegalArgumentException("The metric cannot be empty");
    }
    if (tags == null || tags.isEmpty()) {
      throw new IllegalArgumentException("Tags cannot be null or empty "
          + "when getting a TSUID");
    }
    
    final byte[] metric_uid = new byte[TSDB.metrics_width()];
    
    class TagsCB implements Callback<byte[], ArrayList<byte[]>> {
      @Override
      public byte[] call(final ArrayList<byte[]> tag_list) throws Exception {
        final byte[] tsuid = new byte[metric_uid.length + 
                                      ((TSDB.tagk_width() + TSDB.tagv_width()) 
                                          * tag_list.size())];
        int idx = 0;
        System.arraycopy(metric_uid, 0, tsuid, 0, metric_uid.length);
        idx += metric_uid.length;
        for (final byte[] t : tag_list) {
          System.arraycopy(t, 0, tsuid, idx, t.length);
          idx += t.length;
        }
        return tsuid;
      }
      @Override
      public String toString() {
        return "Tag resolution callback";
      }
    }
    
    class MetricCB implements Callback<Deferred<byte[]>, byte[]> {
      @Override
      public Deferred<byte[]> call(final byte[] uid) 
          throws Exception {
        System.arraycopy(uid, 0, metric_uid, 0, uid.length);
        return Tags.resolveAllAsync(tsdb, tags).addCallback(new TagsCB());
      }
      @Override
      public String toString() {
        return "Metric resolution callback";
      }
    }
    
    return tsdb.getUIDAsync(UniqueIdType.METRIC, metric)
        .addCallbackDeferring(new MetricCB());
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
   * @deprecated Please use {@link #getLastPoint}
   */
  public static Deferred<IncomingDataPoint> getLastPoint(final TSDB tsdb, 
      final byte[] tsuid, final boolean resolve_names, final int max_lookups, 
      final long last_timestamp) {
    final TSUIDQuery query = new TSUIDQuery(tsdb, tsuid);
    query.last_timestamp = last_timestamp;
    return query.getLastPoint(resolve_names, max_lookups);
  }
  
  /**
   * Resolve the UIDs to names. If the query was for a metric and tags then we
   * can just use those.
   * @param dp The data point to fill in values for
   * @return A deferred with the data point or an exception if something went
   * wrong.
   */
  private Deferred<IncomingDataPoint> resolveNames(final IncomingDataPoint dp) {
    // If the caller gave us a metric and tags, save some time by NOT hitting
    // our UID tables or storage.
    if (metric != null) {
      dp.setMetric(metric);
      dp.setTags((HashMap<String, String>)tags);
      return Deferred.fromResult(dp);
    }
    
    class TagsCB implements Callback<IncomingDataPoint, HashMap<String, String>> {
      public IncomingDataPoint call(final HashMap<String, String> tags) 
        throws Exception {
        dp.setTags(tags);
        return dp;
      }
      @Override
      public String toString() {
        return "Tags resolution CB";
      }
    }
    
    class MetricCB implements Callback<Deferred<IncomingDataPoint>, String> {
      public Deferred<IncomingDataPoint> call(final String name) 
          throws Exception {
        dp.setMetric(name);
        final List<byte[]> tags = UniqueId.getTagPairsFromTSUID(tsuid);
        return Tags.resolveIdsAsync(tsdb, tags).addCallback(new TagsCB());
      }
      @Override
      public String toString() {
        return "Metric resolution CB";
      }
    }
    
    final byte[] metric_uid = Arrays.copyOfRange(tsuid, 0, TSDB.metrics_width());     
    return tsdb.getUidName(UniqueIdType.METRIC, metric_uid)
        .addCallbackDeferring(new MetricCB());
  }
  
  /**
   * Handles getting the results of the first GetRequest and keeps iterating
   * back in time until we find a point or run out of back scans.
   */
  private class LastPointCB implements Callback<Deferred<IncomingDataPoint>, 
    ArrayList<KeyValue>> {
    int iteration = 0;
    
    @Override
    public Deferred<IncomingDataPoint> call(final ArrayList<KeyValue> row)
        throws Exception {      
      if (row == null || row.isEmpty()) {
        if (iteration >= back_scan) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("No data points found within the time span for TSUID query " 
                + TSUIDQuery.this);
          }
          return Deferred.fromResult(null);
        }
        
        last_timestamp -= 3600;
        ++iteration;
        final byte[] key = RowKey.rowKeyFromTSUID(tsdb, tsuid, last_timestamp);
        final GetRequest get = new GetRequest(tsdb.dataTable(), key);
        get.family(TSDB.FAMILY());
        return tsdb.getClient().get(get).addCallbackDeferring(this);
      }
      
      final IncomingDataPoint dp = 
          new Internal.GetLastDataPointCB(tsdb).call(row);
      dp.setTSUID(UniqueId.uidToString(tsuid));
      if (!resolve_names) {
        return Deferred.fromResult(dp);
      }
      
      return resolveNames(dp);
    }
    @Override
    public String toString() {
      return "LastDataPoint callback";
    }
  }
  
  /**
   * Callback that receives the result of a single meta table lookup to see if
   * the last write counter was there or not. The input should contain just
   * the counter column
   */
  private class MetaCB implements Callback<Deferred<IncomingDataPoint>, 
    ArrayList<KeyValue>> {
    @Override
    public Deferred<IncomingDataPoint> call(final ArrayList<KeyValue> row) 
        throws Exception {
      if (row == null) {
        return Deferred.fromResult(null);
      }
      last_timestamp = Internal.baseTime(row.get(0).timestamp());
      final byte[] key = RowKey.rowKeyFromTSUID(tsdb, tsuid, last_timestamp);
      final GetRequest get = new GetRequest(tsdb.dataTable(), key);
      get.family(TSDB.FAMILY());
      return tsdb.getClient().get(get).addCallbackDeferring(new LastPointCB());
    }
    @Override
    public String toString() {
      return "Meta TSCounter lookup";
    }
  }
  
  /**
   * Resolves the metric and tags (if set) to their UIDs, setting the local
   * arrays.
   * @return A deferred to wait on or catch exceptions in
   * @throws IllegalArgumentException if the metric is empty || null or the 
   * tag list is null.
   */
  private Deferred<Object> resolveMetric() {
    if (metric == null || metric.isEmpty()) {
      throw new IllegalArgumentException("The metric cannot be empty");
    }
    if (tags == null) {
      throw new IllegalArgumentException("Tags cannot be null or empty "
          + "when getting a TSUID");
    }
    
    class TagsCB implements Callback<Object, ArrayList<byte[]>> {
      @Override
      public Object call(final ArrayList<byte[]> tag_list) throws Exception {
        setTagUIDs(tag_list);
        return null;
      }
      @Override
      public String toString() {
        return "Tag resolution callback";
      }
    }
    
    class MetricCB implements Callback<Deferred<Object>, byte[]> {
      @Override
      public Deferred<Object> call(final byte[] uid) 
          throws Exception {
        setMetricUID(uid);
        if (tags.isEmpty()) {
          setTagUIDs(new ArrayList<byte[]>(0));
          return null;
        }
        return Tags.resolveAllAsync(tsdb, tags).addCallback(new TagsCB());
      }
      @Override
      public String toString() {
        return "Metric resolution callback";
      }
    }
    
    return tsdb.getUIDAsync(UniqueIdType.METRIC, metric)
        .addCallbackDeferring(new MetricCB());
  }
  
  /** @param tsuid The TSUID to store */
  private void setTSUID(final byte[] tsuid) {
    this.tsuid = tsuid;
  }
  
  /** @param uid The metric UID to set */
  private void setMetricUID(final byte[] uid) {
    metric_uid = uid;
  }
  
  /** @param uids The tag UIDs to set */
  private void setTagUIDs(final ArrayList<byte[]> uids) {
    tag_uids = uids;
  }
  
  /**
   * Configures the scanner for a specific metric and optional tags
   * @return A configured scanner
   */
  private Scanner getScanner() {
    final Scanner scanner = tsdb.getClient().newScanner(tsdb.metaTable());
    scanner.setStartKey(metric_uid);
    
    // increment the metric UID by one so we can scan all of the rows for the
    // given metric
    final long stop = UniqueId.uidToLong(metric_uid, TSDB.metrics_width()) + 1;
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
      final Iterator<byte[]> tags = this.tag_uids.iterator();
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