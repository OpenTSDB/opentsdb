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
package net.opentsdb.core;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hbase.async.Bytes;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.hbase.async.Bytes.ByteMap;

import com.google.common.annotations.VisibleForTesting;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.stats.Histogram;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.DateTime;

/**
 * Non-synchronized implementation of {@link Query}.
 */
final class TsdbQuery implements Query {

  private static final Logger LOG = LoggerFactory.getLogger(TsdbQuery.class);

  /** Used whenever there are no results. */
  private static final DataPoints[] NO_RESULT = new DataPoints[0];

  /**
   * Keep track of the latency we perceive when doing Scans on HBase.
   * We want buckets up to 16s, with 2 ms interval between each bucket up to
   * 100 ms after we which we switch to exponential buckets.
   */
  static final Histogram scanlatency = new Histogram(16000, (short) 2, 100);

  /**
   * Charset to use with our server-side row-filter.
   * We use this one because it preserves every possible byte unchanged.
   */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");

  /** The TSDB we belong to. */
  private final TSDB tsdb;

  /** Value used for timestamps that are uninitialized.  */
  private static final int UNSET = -1;

  /** Start time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  private long start_time = UNSET;

  /** End time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  private long end_time = UNSET;

  /** ID of the metric being looked up. */
  private byte[] metric;

  /**
   * Tags by which we must group the results.
   * Each element is a tag ID.
   * Invariant: an element cannot be both in this array and in {@code tags}.
   */
  private ArrayList<byte[]> group_bys;

  /**
   * Tag key and values to use in the row key filter, all pre-sorted
   */
  private ByteMap<byte[][]> row_key_literals;

  /** If true, use rate of change instead of actual values. */
  private boolean rate;

  /** Specifies the various options for rate calculations */
  private RateOptions rate_options;
  
  /** Aggregator function to use. */
  private Aggregator aggregator;

  /**
   * Downsampling function to use, if any (can be {@code null}).
   * If this is non-null, {@code sample_interval_ms} must be strictly positive.
   */
  private Aggregator downsampler;

  /** Minimum time interval (in milliseconds) wanted between each data point. */
  private long sample_interval_ms;
  
  /** Downsampling fill policy. */
  private FillPolicy fill_policy;

  /** Optional list of TSUIDs to fetch and aggregate instead of a metric */
  private List<String> tsuids;
  
  /** An index that links this query to the original sub query */
  private int query_index;
  
  /** Tag value filters to apply post scan */
  private List<TagVFilter> filters;
  
  /** Constructor. */
  public TsdbQuery(final TSDB tsdb) {
    this.tsdb = tsdb;
    
    // By default, we should interpolate.
    fill_policy = DownsamplingSpecification.DEFAULT_FILL_POLICY;
  }

  /**
   * Sets the start time for the query
   * @param timestamp Unix epoch timestamp in seconds or milliseconds
   * @throws IllegalArgumentException if the timestamp is invalid or greater
   * than the end time (if set)
   */
  @Override
  public void setStartTime(final long timestamp) {
    if (timestamp < 0 || ((timestamp & Const.SECOND_MASK) != 0 && 
        timestamp > 9999999999999L)) {
      throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
    } else if (end_time != UNSET && timestamp >= getEndTime()) {
      throw new IllegalArgumentException("new start time (" + timestamp
          + ") is greater than or equal to end time: " + getEndTime());
    }
    start_time = timestamp;
  }

  /**
   * @return the start time for the query
   * @throws IllegalStateException if the start time hasn't been set yet
   */
  @Override
  public long getStartTime() {
    if (start_time == UNSET) {
      throw new IllegalStateException("setStartTime was never called!");
    }
    return start_time;
  }

  /**
   * Sets the end time for the query. If this isn't set, the system time will be
   * used when the query is executed or {@link #getEndTime} is called
   * @param timestamp Unix epoch timestamp in seconds or milliseconds
   * @throws IllegalArgumentException if the timestamp is invalid or less
   * than the start time (if set)
   */
  @Override
  public void setEndTime(final long timestamp) {
    if (timestamp < 0 || ((timestamp & Const.SECOND_MASK) != 0 && 
        timestamp > 9999999999999L)) {
      throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
    } else if (start_time != UNSET && timestamp <= getStartTime()) {
      throw new IllegalArgumentException("new end time (" + timestamp
          + ") is less than or equal to start time: " + getStartTime());
    }
    end_time = timestamp;
  }

  /** @return the configured end time. If the end time hasn't been set, the
   * current system time will be stored and returned.
   */
  @Override
  public long getEndTime() {
    if (end_time == UNSET) {
      setEndTime(DateTime.currentTimeMillis());
    }
    return end_time;
  }

  @Override
  public void setTimeSeries(final String metric,
      final Map<String, String> tags,
      final Aggregator function,
      final boolean rate) throws NoSuchUniqueName {
    setTimeSeries(metric, tags, function, rate, new RateOptions());
  }
  
  @Override
  public void setTimeSeries(final String metric,
        final Map<String, String> tags,
        final Aggregator function,
        final boolean rate,
        final RateOptions rate_options)
  throws NoSuchUniqueName {
    if (filters == null) {
      filters = new ArrayList<TagVFilter>(tags.size());
    }
    TagVFilter.tagsToFilters(tags, filters);
    
    try {
      for (final TagVFilter filter : this.filters) {
        filter.resolveTagkName(tsdb).join();
      }
    } catch (final InterruptedException e) {
      LOG.warn("Interrupted", e);
      Thread.currentThread().interrupt();
    } catch (final NoSuchUniqueName e) {
      throw e;
    } catch (final Exception e) {
      if (e instanceof DeferredGroupException) {
        // rollback to the actual case. The DGE missdirects
        Throwable ex = e.getCause();
        while(ex != null && ex instanceof DeferredGroupException) {
          ex = ex.getCause();
        }
        if (ex != null) {
          throw (RuntimeException)ex;
        }
      }
      LOG.error("Unexpected exception processing group bys", e);
      throw new RuntimeException(e);
    }
    
    findGroupBys();
    this.metric = tsdb.metrics.getId(metric);
    aggregator = function;
    this.rate = rate;
    this.rate_options = rate_options;
  }

  @Override
  public void setTimeSeries(final List<String> tsuids,
      final Aggregator function, final boolean rate) {
    setTimeSeries(tsuids, function, rate, new RateOptions());
  }
  
  @Override
  public void setTimeSeries(final List<String> tsuids,
      final Aggregator function, final boolean rate, 
      final RateOptions rate_options) {
    if (tsuids == null || tsuids.isEmpty()) {
      throw new IllegalArgumentException(
          "Empty or missing TSUID list not allowed");
    }
    
    String first_metric = "";
    for (final String tsuid : tsuids) {
      if (first_metric.isEmpty()) {
        first_metric = tsuid.substring(0, TSDB.metrics_width() * 2)
          .toUpperCase();
        continue;
      }
      
      final String metric = tsuid.substring(0, TSDB.metrics_width() * 2)
        .toUpperCase();
      if (!first_metric.equals(metric)) {
        throw new IllegalArgumentException(
          "One or more TSUIDs did not share the same metric");
      }
    }
    
    // the metric will be set with the scanner is configured 
    this.tsuids = tsuids;
    aggregator = function;
    this.rate = rate;
    this.rate_options = rate_options;
  }
  
  public Deferred<Object> configureFromQuery(final TSQuery query, 
      final int index) {
    if (query.getQueries() == null || query.getQueries().isEmpty()) {
      throw new IllegalArgumentException("Missing sub queries");
    }
    if (index < 0 || index > query.getQueries().size()) {
      throw new IllegalArgumentException("Query index was out of range");
    }
    
    final TSSubQuery sub_query = query.getQueries().get(index);
    setStartTime(query.startTime());
    setEndTime(query.endTime());
    query_index = index;
    
    // set common options
    aggregator = sub_query.aggregator();
    rate = sub_query.getRate();
    rate_options = sub_query.getRateOptions();
    if (rate_options == null) {
      rate_options = new RateOptions();
    }
    downsampler = sub_query.downsampler();
    sample_interval_ms = sub_query.downsampleInterval();
    fill_policy = sub_query.fillPolicy();
    filters = sub_query.getFilters();
    
    // if we have tsuids set, that takes precedence
    if (sub_query.getTsuids() != null && !sub_query.getTsuids().isEmpty()) {
      tsuids = new ArrayList<String>(sub_query.getTsuids());
      String first_metric = "";
      for (final String tsuid : tsuids) {
        if (first_metric.isEmpty()) {
          first_metric = tsuid.substring(0, TSDB.metrics_width() * 2)
            .toUpperCase();
          continue;
        }
        
        final String metric = tsuid.substring(0, TSDB.metrics_width() * 2)
          .toUpperCase();
        if (!first_metric.equals(metric)) {
          throw new IllegalArgumentException(
            "One or more TSUIDs did not share the same metric [" + first_metric + 
            "] [" + metric + "]");
        }
      }
      return Deferred.fromResult(null);
    } else {
      /** Triggers the group by resolution if we had filters to resolve */
      class FilterCB implements Callback<Object, ArrayList<byte[]>> {
        @Override
        public Object call(final ArrayList<byte[]> results) throws Exception {
          findGroupBys();
          return Deferred.fromResult(null);
        }
      }

      /** Resolve and group by tags after resolving the metric */
      class MetricCB implements Callback<Object, byte[]> {
        @Override
        public Object call(final byte[] uid) throws Exception {
          metric = uid;
          if (filters != null) {
            final List<Deferred<byte[]>> deferreds = 
                new ArrayList<Deferred<byte[]>>(filters.size());
            for (final TagVFilter filter : filters) {
              deferreds.add(filter.resolveTagkName(tsdb));
            }
            return Deferred.group(deferreds).addCallback(new FilterCB());
          } else {
            return Deferred.fromResult(null);
          }
        }
      }
      
      // fire off the callback chain by resolving the metric first
      return tsdb.metrics.getIdAsync(sub_query.getMetric())
          .addCallback(new MetricCB());
    }
  }
  
  
  @Override
  public void downsample(final long interval, final Aggregator downsampler,
      final FillPolicy fill_policy) {
    if (downsampler == null) {
      throw new NullPointerException("downsampler");
    } else if (interval <= 0) {
      throw new IllegalArgumentException("interval not > 0: " + interval);
    }
    this.downsampler = downsampler;
    this.sample_interval_ms = interval;
    this.fill_policy = fill_policy;
  }

  /**
   * Sets an optional downsampling function with interpolation on this query.
   * @param interval The interval, in milliseconds to rollup data points
   * @param downsampler An aggregation function to use when rolling up data points
   * @throws NullPointerException if the aggregation function is null
   * @throws IllegalArgumentException if the interval is not greater than 0
   */
  @Override
  public void downsample(final long interval, final Aggregator downsampler) {
    downsample(interval, downsampler, FillPolicy.NONE);
  }

  /**
   * Populates the {@link #group_bys} and {@link #row_key_literals}'s with 
   * values pulled from the filters. 
   */
  private void findGroupBys() {
    if (filters == null || filters.isEmpty()) {
      return;
    }
    
    row_key_literals = new ByteMap<byte[][]>();
    
    Collections.sort(filters);
    final Iterator<TagVFilter> current_iterator = filters.iterator();
    final Iterator<TagVFilter> look_ahead = filters.iterator();
    byte[] tagk = null;
    TagVFilter next = look_ahead.hasNext() ? look_ahead.next() : null;
    int row_key_literals_count = 0;
    while (current_iterator.hasNext()) {
      next = look_ahead.hasNext() ? look_ahead.next() : null;
      int gbs = 0;
      // sorted!
      final ByteMap<Void> literals = new ByteMap<Void>();
      final List<TagVFilter> literal_filters = new ArrayList<TagVFilter>();
      TagVFilter current = null;
      do { // yeah, I'm breakin out the do!!!
        current = current_iterator.next();
        if (tagk == null) {
          tagk = new byte[TSDB.tagk_width()];
          System.arraycopy(current.getTagkBytes(), 0, tagk, 0, TSDB.tagk_width());
        }
        
        if (current.isGroupBy()) {
          gbs++;
        }
        if (!current.getTagVUids().isEmpty()) {
          for (final byte[] uid : current.getTagVUids()) {
            literals.put(uid, null);
          }
          literal_filters.add(current);
        }

        if (next != null && Bytes.memcmp(tagk, next.getTagkBytes()) != 0) {
          break;
        }
        next = look_ahead.hasNext() ? look_ahead.next() : null;
      } while (current_iterator.hasNext() && 
          Bytes.memcmp(tagk, current.getTagkBytes()) == 0);

      if (gbs > 0) {
        if (group_bys == null) {
          group_bys = new ArrayList<byte[]>();
        }
        group_bys.add(current.getTagkBytes());
      }
      
      if (literals.size() > 0) {
        if (literals.size() + row_key_literals_count > 
            tsdb.getConfig().getInt("tsd.query.filter.expansion_limit")) {
          LOG.debug("Skipping literals for " + current.getTagk() + 
              " as it exceedes the limit");
        } else {
          final byte[][] values = new byte[literals.size()][];
          literals.keySet().toArray(values);
          row_key_literals.put(current.getTagkBytes(), values);
          row_key_literals_count += values.length;
          
          for (final TagVFilter filter : literal_filters) {
            filter.setPostScan(false);
          }
        }
      } else {
        row_key_literals.put(current.getTagkBytes(), null);
      }
    }
  }
  /**
   * Executes the query.
   * NOTE: Do not run the same query multiple times. Construct a new query with
   * the same parameters again if needed
   * TODO(cl) There are some strange occurrences when unit testing where the end
   * time, if not set, can change between calls to run()
   * @return An array of data points with one time series per array value
   */
  @Override
  public DataPoints[] run() throws HBaseException {
    try {
      return runAsync().joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  @Override
  public Deferred<DataPoints[]> runAsync() throws HBaseException {
    return findSpans().addCallback(new GroupByAndAggregateCB());
  }

  /**
   * Finds all the {@link Span}s that match this query.
   * This is what actually scans the HBase table and loads the data into
   * {@link Span}s.
   * @return A map from HBase row key to the {@link Span} for that row key.
   * Since a {@link Span} actually contains multiple HBase rows, the row key
   * stored in the map has its timestamp zero'ed out.
   * @throws HBaseException if there was a problem communicating with HBase to
   * perform the search.
   * @throws IllegalArgumentException if bad data was retrieved from HBase.
   */
  private Deferred<TreeMap<byte[], Span>> findSpans() throws HBaseException {
    final short metric_width = tsdb.metrics.width();
    final TreeMap<byte[], Span> spans = // The key is a row key from HBase.
      new TreeMap<byte[], Span>(new SpanCmp(
          (short)(Const.SALT_WIDTH() + metric_width)));
    
    // Copy only the filters that should trigger a tag resolution. If this list
    // is empty due to literals or a wildcard star, then we'll save a TON of
    // UID lookups
    final List<TagVFilter> scanner_filters;
    if (filters != null) {   
      scanner_filters = new ArrayList<TagVFilter>(filters.size());
      for (final TagVFilter filter : filters) {
        if (filter.postScan()) {
          scanner_filters.add(filter);
        }
      }
    } else {
      scanner_filters = null;
    }
    
    if (Const.SALT_WIDTH() > 0) {
      final List<Scanner> scanners = new ArrayList<Scanner>(Const.SALT_BUCKETS());
      for (int i = 0; i < Const.SALT_BUCKETS(); i++) {
        scanners.add(getScanner(i));
      }
      return new SaltScanner(tsdb, metric, scanners, spans, scanner_filters)
        .scan();
    }
    
    final Scanner scanner = getScanner();
    final Deferred<TreeMap<byte[], Span>> results =
      new Deferred<TreeMap<byte[], Span>>();
    
    /**
    * Scanner callback executed recursively each time we get a set of data
    * from storage. This is responsible for determining what columns are
    * returned and issuing requests to load leaf objects.
    * When the scanner returns a null set of rows, the method initiates the
    * final callback.
    */
    final class ScannerCB implements Callback<Object,
      ArrayList<ArrayList<KeyValue>>> {
      
      int nrows = 0;
      boolean seenAnnotation = false;
      int hbase_time = 0; // milliseconds.
      long starttime = System.nanoTime();
      long timeout = tsdb.getConfig().getLong("tsd.query.timeout");
      private final Set<String> skips = new HashSet<String>();
      private final Set<String> keepers = new HashSet<String>();
      /**
      * Starts the scanner and is called recursively to fetch the next set of
      * rows from the scanner.
      * @return The map of spans if loaded successfully, null if no data was
      * found
      */
       public Object scan() {
         starttime = System.nanoTime();
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
         hbase_time += (System.nanoTime() - starttime) / 1000000;
         try {
           if (rows == null) {
             hbase_time += (System.nanoTime() - starttime) / 1000000;
             scanlatency.add(hbase_time);
             LOG.info(TsdbQuery.this + " matched " + nrows + " rows in " +
                 spans.size() + " spans in " + hbase_time + "ms");
             if (nrows < 1 && !seenAnnotation) {
               results.callback(null);
             } else {
               results.callback(spans);
             }
             scanner.close();
             return null;
           }
           
           if (timeout > 0 && hbase_time > timeout) {
             throw new InterruptedException("Query timeout exceeded!");
           }
           
           // used for UID resolution if a filter is involved
           final List<Deferred<Object>> lookups = 
               filters != null && !filters.isEmpty() ? 
                   new ArrayList<Deferred<Object>>(rows.size()) : null;
               
           for (final ArrayList<KeyValue> row : rows) {
             final byte[] key = row.get(0).key();
             if (Bytes.memcmp(metric, key, 0, metric_width) != 0) {
               scanner.close();
               throw new IllegalDataException(
                   "HBase returned a row that doesn't match"
                   + " our scanner (" + scanner + ")! " + row + " does not start"
                   + " with " + Arrays.toString(metric));
             }
             
             // If any filters have made it this far then we need to resolve
             // the row key UIDs to their names for string comparison. We'll
             // try to avoid the resolution with some sets but we may dupe
             // resolve a few times.
             // TODO - more efficient resolution
             // TODO - byte set instead of a string for the uid may be faster
             if (scanner_filters != null && !scanner_filters.isEmpty()) {
               lookups.clear();
               final String tsuid = 
                   UniqueId.uidToString(UniqueId.getTSUIDFromKey(key, 
                   TSDB.metrics_width(), Const.TIMESTAMP_BYTES));
               if (skips.contains(tsuid)) {
                 continue;
               }
               if (!keepers.contains(tsuid)) {
                 /** CB to called after all of the UIDs have been resolved */
                 class MatchCB implements Callback<Object, ArrayList<Boolean>> {
                   @Override
                   public Object call(final ArrayList<Boolean> matches) 
                       throws Exception {
                     for (final boolean matched : matches) {
                       if (!matched) {
                         skips.add(tsuid);
                         return null;
                       }
                     }
                     // matched all, good data
                     keepers.add(tsuid);
                     processRow(key, row);
                     return null;
                   }
                 }

                 /** Resolves all of the row key UIDs to their strings for filtering */
                 class GetTagsCB implements
                     Callback<Deferred<ArrayList<Boolean>>, Map<String, String>> {
                   @Override
                   public Deferred<ArrayList<Boolean>> call(
                       final Map<String, String> tags) throws Exception {
                     final List<Deferred<Boolean>> matches =
                         new ArrayList<Deferred<Boolean>>(scanner_filters.size());

                     for (final TagVFilter filter : scanner_filters) {
                       matches.add(filter.match(tags));
                     }
                     
                     return Deferred.group(matches);
                   }
                 }
    
                 lookups.add(Tags.getTagsAsync(tsdb, key)
                     .addCallbackDeferring(new GetTagsCB())
                     .addBoth(new MatchCB()));
               } else {
                 processRow(key, row);
               }
             } else {
               processRow(key, row);
             }
           }

           // either we need to wait on the UID resolutions or we can go ahead
           // if we don't have filters.
           if (lookups != null && lookups.size() > 0) {
             class GroupCB implements Callback<Object, ArrayList<Object>> {
               @Override
               public Object call(final ArrayList<Object> group) throws Exception {
                 return scan();
               }
             }
             return Deferred.group(lookups).addCallback(new GroupCB());
           } else {
             return scan();
           }
         } catch (Exception e) {
           scanner.close();
           results.callback(e);
           return null;
         }
       }
       
       /**
        * Finds or creates the span for this row, compacts it and stores it.
        * @param key The row key to use for fetching the span
        * @param row The row to add
        */
       void processRow(final byte[] key, final ArrayList<KeyValue> row) {
         Span datapoints = spans.get(key);
         if (datapoints == null) {
           datapoints = new Span(tsdb);
           spans.put(key, datapoints);
         }
         final KeyValue compacted = 
           tsdb.compact(row, datapoints.getAnnotations());
         seenAnnotation |= !datapoints.getAnnotations().isEmpty();
         if (compacted != null) { // Can be null if we ignored all KVs.
           datapoints.addRow(compacted);
           ++nrows;
         }
       }
     }

     new ScannerCB().scan();
     return results;
  }

  /**
  * Callback that should be attached the the output of
  * {@link TsdbQuery#findSpans} to group and sort the results.
  */
  private class GroupByAndAggregateCB implements 
    Callback<DataPoints[], TreeMap<byte[], Span>>{
    
    /**
    * Creates the {@link SpanGroup}s to form the final results of this query.
    * @param spans The {@link Span}s found for this query ({@link #findSpans}).
    * Can be {@code null}, in which case the array returned will be empty.
    * @return A possibly empty array of {@link SpanGroup}s built according to
    * any 'GROUP BY' formulated in this query.
    */
    @Override
    public DataPoints[] call(final TreeMap<byte[], Span> spans) throws Exception {
      if (spans == null || spans.size() <= 0) {
        return NO_RESULT;
      }
      if (group_bys == null) {
        // We haven't been asked to find groups, so let's put all the spans
        // together in the same group.
        final SpanGroup group = new SpanGroup(tsdb,
                                              getScanStartTimeSeconds(),
                                              getScanEndTimeSeconds(),
                                              spans.values(),
                                              rate, rate_options,
                                              aggregator,
                                              sample_interval_ms, downsampler,
                                              query_index, fill_policy);
        return new SpanGroup[] { group };
      }
  
      // Maps group value IDs to the SpanGroup for those values. Say we've
      // been asked to group by two things: foo=* bar=* Then the keys in this
      // map will contain all the value IDs combinations we've seen. If the
      // name IDs for `foo' and `bar' are respectively [0, 0, 7] and [0, 0, 2]
      // then we'll have group_bys=[[0, 0, 2], [0, 0, 7]] (notice it's sorted
      // by ID, so bar is first) and say we find foo=LOL bar=OMG as well as
      // foo=LOL bar=WTF and that the IDs of the tag values are:
      // LOL=[0, 0, 1] OMG=[0, 0, 4] WTF=[0, 0, 3]
      // then the map will have two keys:
      // - one for the LOL-OMG combination: [0, 0, 1, 0, 0, 4] and,
      // - one for the LOL-WTF combination: [0, 0, 1, 0, 0, 3].
      final ByteMap<SpanGroup> groups = new ByteMap<SpanGroup>();
      final short value_width = tsdb.tag_values.width();
      final byte[] group = new byte[group_bys.size() * value_width];
      for (final Map.Entry<byte[], Span> entry : spans.entrySet()) {
        final byte[] row = entry.getKey();
        byte[] value_id = null;
        int i = 0;
        // TODO(tsuna): The following loop has a quadratic behavior. We can
        // make it much better since both the row key and group_bys are sorted.
        for (final byte[] tag_id : group_bys) {
          value_id = Tags.getValueId(tsdb, row, tag_id);
          if (value_id == null) {
            break;
          }
          System.arraycopy(value_id, 0, group, i, value_width);
          i += value_width;
        }
        if (value_id == null) {
          LOG.error("WTF? Dropping span for row " + Arrays.toString(row)
                   + " as it had no matching tag from the requested groups,"
                   + " which is unexpected. Query=" + this);
          continue;
        }
        //LOG.info("Span belongs to group " + Arrays.toString(group) + ": " + Arrays.toString(row));
        SpanGroup thegroup = groups.get(group);
        if (thegroup == null) {
          thegroup = new SpanGroup(tsdb, getScanStartTimeSeconds(),
                                   getScanEndTimeSeconds(),
                                   null, rate, rate_options, aggregator,
                                   sample_interval_ms, downsampler, query_index, 
                                   fill_policy);
          // Copy the array because we're going to keep `group' and overwrite
          // its contents. So we want the collection to have an immutable copy.
          final byte[] group_copy = new byte[group.length];
          System.arraycopy(group, 0, group_copy, 0, group.length);
          groups.put(group_copy, thegroup);
        }
        thegroup.add(entry.getValue());
      }
      //for (final Map.Entry<byte[], SpanGroup> entry : groups) {
      // LOG.info("group for " + Arrays.toString(entry.getKey()) + ": " + entry.getValue());
      //}
      return groups.values().toArray(new SpanGroup[groups.size()]);
    }
  }

  /**
   * Returns a scanner set for the given metric (from {@link #metric} or from
   * the first TSUID in the {@link #tsuids}s list. If one or more tags are 
   * provided, it calls into {@link #createAndSetFilter} to setup a row key 
   * filter. If one or more TSUIDs have been provided, it calls into
   * {@link #createAndSetTSUIDFilter} to setup a row key filter.
   * @return A scanner to use for fetching data points
   */
  protected Scanner getScanner() throws HBaseException {
    return getScanner(0);
  }
  
  /**
   * Returns a scanner set for the given metric (from {@link #metric} or from
   * the first TSUID in the {@link #tsuids}s list. If one or more tags are 
   * provided, it calls into {@link #createAndSetFilter} to setup a row key 
   * filter. If one or more TSUIDs have been provided, it calls into
   * {@link #createAndSetTSUIDFilter} to setup a row key filter.
   * @param salt_bucket The salt bucket to scan over when salting is enabled.
   * @return A scanner to use for fetching data points
   */
  protected Scanner getScanner(final int salt_bucket) throws HBaseException {
    final short metric_width = tsdb.metrics.width();
    final int metric_salt_width = metric_width + Const.SALT_WIDTH();
    final byte[] start_row = new byte[metric_salt_width + Const.TIMESTAMP_BYTES];
    final byte[] end_row = new byte[metric_salt_width + Const.TIMESTAMP_BYTES];
    
    if (Const.SALT_WIDTH() > 0) {
      final byte[] salt = RowKey.getSaltBytes(salt_bucket);
      System.arraycopy(salt, 0, start_row, 0, Const.SALT_WIDTH());
      System.arraycopy(salt, 0, end_row, 0, Const.SALT_WIDTH());
    }
    
    // We search at least one row before and one row after the start & end
    // time we've been given as it's quite likely that the exact timestamp
    // we're looking for is in the middle of a row.  Plus, a number of things
    // rely on having a few extra data points before & after the exact start
    // & end dates in order to do proper rate calculation or downsampling near
    // the "edges" of the graph.
    Bytes.setInt(start_row, (int) getScanStartTimeSeconds(), metric_salt_width);
    Bytes.setInt(end_row, (end_time == UNSET
                           ? -1  // Will scan until the end (0xFFF...).
                           : (int) getScanEndTimeSeconds()),
                           metric_salt_width);
    
    // set the metric UID based on the TSUIDs if given, or the metric UID
    if (tsuids != null && !tsuids.isEmpty()) {
      final String tsuid = tsuids.get(0);
      final String metric_uid = tsuid.substring(0, metric_width * 2);
      metric = UniqueId.stringToUid(metric_uid);
      System.arraycopy(metric, 0, start_row, Const.SALT_WIDTH(), metric_width);
      System.arraycopy(metric, 0, end_row, Const.SALT_WIDTH(), metric_width); 
    } else {
      System.arraycopy(metric, 0, start_row, Const.SALT_WIDTH(), metric_width);
      System.arraycopy(metric, 0, end_row, Const.SALT_WIDTH(), metric_width);
    }
    
    final Scanner scanner = tsdb.client.newScanner(tsdb.table);
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    if (tsuids != null && !tsuids.isEmpty()) {
      createAndSetTSUIDFilter(scanner);
    } else if (filters.size() > 0) {
      createAndSetFilter(scanner);
    }
    scanner.setFamily(TSDB.FAMILY);
    return scanner;
  }

  /** Returns the UNIX timestamp from which we must start scanning.  */
  private long getScanStartTimeSeconds() {
    // Begin with the raw query start time.
    long start = getStartTime();

    // Convert to seconds if we have a query in ms.
    if ((start & Const.SECOND_MASK) != 0L) {
      start /= 1000L;
    }

    // First, we align the start timestamp to its representative value for the
    // interval in which it appears, if downsampling.
    long interval_aligned_ts = start;
    if (0L != sample_interval_ms) {
      // Downsampling enabled.
      final long interval_offset = (1000L * start) % sample_interval_ms;
      interval_aligned_ts -= interval_offset / 1000L;
    }

    // Then snap that timestamp back to its representative value for the
    // timespan in which it appears.
    final long timespan_offset = interval_aligned_ts % Const.MAX_TIMESPAN;
    final long timespan_aligned_ts = interval_aligned_ts - timespan_offset;

    // Don't return negative numbers.
    return timespan_aligned_ts > 0L ? timespan_aligned_ts : 0L;
  }

  /** Returns the UNIX timestamp at which we must stop scanning.  */
  private long getScanEndTimeSeconds() {
    // Begin with the raw query end time.
    long end = getEndTime();

    // Convert to seconds if we have a query in ms.
    if ((end & Const.SECOND_MASK) != 0L) {
      end /= 1000L;
    }

    // The calculation depends on whether we're downsampling.
    if (0L != sample_interval_ms) {
      // Downsampling enabled.
      //
      // First, we align the end timestamp to its representative value for the
      // interval FOLLOWING the one in which it appears.
      //
      // OpenTSDB's query bounds are inclusive, but HBase scan bounds are half-
      // open. The user may have provided an end bound that is already
      // interval-aligned (i.e., its interval offset is zero). If so, the user
      // wishes for that interval to appear in the output. In that case, we
      // skip forward an entire extra interval.
      //
      // This can be accomplished by simply not testing for zero offset.
      final long interval_offset = (1000L * end) % sample_interval_ms;
      final long interval_aligned_ts = end +
        (sample_interval_ms - interval_offset) / 1000L;

      // Then, if we're now aligned on a timespan boundary, then we need no
      // further adjustment: we are guaranteed to have always moved the end time
      // forward, so the scan will find the data we need.
      //
      // Otherwise, we need to align to the NEXT timespan to ensure that we scan
      // the needed data.
      final long timespan_offset = interval_aligned_ts % Const.MAX_TIMESPAN;
      return (0L == timespan_offset) ?
        interval_aligned_ts :
        interval_aligned_ts + (Const.MAX_TIMESPAN - timespan_offset);
    } else {
      // Not downsampling.
      //
      // Regardless of the end timestamp's position within the current timespan,
      // we must always align to the beginning of the next timespan. This is
      // true even if it's already aligned on a timespan boundary. Again, the
      // reason for this is OpenTSDB's closed interval vs. HBase's half-open.
      final long timespan_offset = end % Const.MAX_TIMESPAN;
      return end + (Const.MAX_TIMESPAN - timespan_offset);
    }
  }

  /**
   * Sets the server-side regexp filter on the scanner.
   * In order to find the rows with the relevant tags, we use a
   * server-side filter that matches a regular expression on the row key.
   * @param scanner The scanner on which to add the filter.
   */
  private void createAndSetFilter(final Scanner scanner) {
    if (group_bys != null) {
      Collections.sort(group_bys, Bytes.MEMCMP);
    }
    final short name_width = tsdb.tag_names.width();
    final short value_width = tsdb.tag_values.width();
    final short tagsize = (short) (name_width + value_width);
    // Generate a regexp for our tags.  Say we have 2 tags: { 0 0 1 0 0 2 }
    // and { 4 5 6 9 8 7 }, the regexp will be:
    // "^.{7}(?:.{6})*\\Q\000\000\001\000\000\002\\E(?:.{6})*\\Q\004\005\006\011\010\007\\E(?:.{6})*$"
    final StringBuilder buf = new StringBuilder(
        15  // "^.{N}" + "(?:.{M})*" + "$"
        + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
           * ((row_key_literals == null ? 0 : row_key_literals.size()) + 
               (group_bys == null ? 0 : group_bys.size() * 3))));
    // In order to avoid re-allocations, reserve a bit more w/ groups ^^^

    // Alright, let's build this regexp.  From the beginning...
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
               + "^.{")
       // ... start by skipping the salt, metric ID and timestamp.
       .append(Const.SALT_WIDTH() + tsdb.metrics.width() + Const.TIMESTAMP_BYTES)
       .append("}");

    final Iterator<Entry<byte[], byte[][]>> it = row_key_literals == null ? 
        new ByteMap<byte[][]>().iterator() : row_key_literals.iterator();

    while(it.hasNext()) {
      Entry<byte[], byte[][]> entry = it.hasNext() ? it.next() : null;
      // TODO - This look ahead may be expensive. We need to get some data around
      // whether it's faster for HBase to scan with a look ahead or simply pass
      // the rows back to the TSD for filtering.
      final boolean not_key = 
          entry.getValue() != null && entry.getValue().length == 0;
      
      // Skip any number of tags.
      buf.append("(?:.{").append(tagsize).append("})*");
      if (not_key) {
        // start the lookahead as we have a key we expliclty do not want in the
        // results
        buf.append("(?!");
      }
      buf.append("\\Q");
      
      addId(buf, entry.getKey());
      if (entry.getValue() != null && entry.getValue().length > 0) {  // Add a group_by.
        // We want specific IDs.  List them: /(AAA|BBB|CCC|..)/
        buf.append("(?:");
        for (final byte[] value_id : entry.getValue()) {
          if (value_id == null) {
            continue;
          }
          buf.append("\\Q");
          addId(buf, value_id);
          buf.append('|');
        }
        // Replace the pipe of the last iteration.
        buf.setCharAt(buf.length() - 1, ')');
      } else {
        buf.append(".{").append(value_width).append('}');  // Any value ID.
      }
      
      if (not_key) {
        // be sure to close off the look ahead
        buf.append(")");
      }
    }
    // Skip any number of tags before the end.
    buf.append("(?:.{").append(tagsize).append("})*$");
    scanner.setKeyRegexp(buf.toString(), CHARSET);
    if (LOG.isDebugEnabled()) {
      logRegexScanner(buf.toString());
    }
  }

  /**
   * Little helper to print out the regular expression by converting the UID
   * bytes to an array.
   * @param regexp The regex string to print to the debug log
   * @since 2.2
   */
  void logRegexScanner(final String regexp) {
    final StringBuilder buf = new StringBuilder();
    for (int i = 0; i < regexp.length(); i++) {
      if (i > 0 && regexp.charAt(i - 1) == 'Q') {
        if (regexp.charAt(i - 3) == '*') {
          // tagk
          byte[] tagk = new byte[TSDB.tagk_width()];
          for (int x = 0; x < TSDB.tagk_width(); x++) {
            tagk[x] = (byte)regexp.charAt(i + x);
          }
          i += TSDB.tagk_width();
          buf.append(Arrays.toString(tagk));
        } else {
          // tagv
          byte[] tagv = new byte[TSDB.tagv_width()];
          for (int x = 0; x < TSDB.tagv_width(); x++) {
            tagv[x] = (byte)regexp.charAt(i + x);
          }
          i += TSDB.tagv_width();
          buf.append(Arrays.toString(tagv));
        }
      } else {
        buf.append(regexp.charAt(i));
      }
    }
    LOG.debug("Scanner regex: " + buf.toString());
  }
  
  /**
   * Sets the server-side regexp filter on the scanner.
   * This will compile a list of the tagk/v pairs for the TSUIDs to prevent
   * storage from returning irrelevant rows.
   * @param scanner The scanner on which to add the filter.
   * @since 2.0
   */
  private void createAndSetTSUIDFilter(final Scanner scanner) {
    Collections.sort(tsuids);
    
    // first, convert the tags to byte arrays and count up the total length
    // so we can allocate the string builder
    final short metric_width = tsdb.metrics.width();
    int tags_length = 0;
    final ArrayList<byte[]> uids = new ArrayList<byte[]>(tsuids.size());
    for (final String tsuid : tsuids) {
      final String tags = tsuid.substring(metric_width * 2);
      final byte[] tag_bytes = UniqueId.stringToUid(tags);
      tags_length += tag_bytes.length;
      uids.add(tag_bytes);
    }
    
    // Generate a regexp for our tags based on any metric and timestamp (since
    // those are handled by the row start/stop) and the list of TSUID tagk/v
    // pairs. The generated regex will look like: ^.{7}(tags|tags|tags)$
    // where each "tags" is similar to \\Q\000\000\001\000\000\002\\E
    final StringBuilder buf = new StringBuilder(
        13  // "(?s)^.{N}(" + ")$"
        + (tsuids.size() * 11) // "\\Q" + "\\E|"
        + tags_length); // total # of bytes in tsuids tagk/v pairs
    
    // Alright, let's build this regexp.  From the beginning...
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
               + "^.{")
       // ... start by skipping the metric ID and timestamp.
       .append(Const.SALT_WIDTH() + tsdb.metrics.width() + Const.TIMESTAMP_BYTES)
       .append("}(");
    
    for (final byte[] tags : uids) {
       // quote the bytes
      buf.append("\\Q");
      addId(buf, tags);
      buf.append('|');
    }
    
    // Replace the pipe of the last iteration, close and set
    buf.setCharAt(buf.length() - 1, ')');
    buf.append("$");
    scanner.setKeyRegexp(buf.toString(), CHARSET);
  }

  /**
   * Appends the given ID to the given buffer, followed by "\\E".
   */
  private static void addId(final StringBuilder buf, final byte[] id) {
    boolean backslash = false;
    for (final byte b : id) {
      buf.append((char) (b & 0xFF));
      if (b == 'E' && backslash) {  // If we saw a `\' and now we have a `E'.
        // So we just terminated the quoted section because we just added \E
        // to `buf'.  So let's put a litteral \E now and start quoting again.
        buf.append("\\\\E\\Q");
      } else {
        backslash = b == '\\';
      }
    }
    buf.append("\\E");
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("TsdbQuery(start_time=")
       .append(getStartTime())
       .append(", end_time=")
       .append(getEndTime());
    if (tsuids != null && !tsuids.isEmpty()) {
      buf.append(", tsuids=");
      for (final String tsuid : tsuids) {
        buf.append(tsuid).append(",");
      }
    } else {
      buf.append(", metric=").append(Arrays.toString(metric));
      buf.append(", filters=[");
      for (final Iterator<TagVFilter> it = filters.iterator(); it.hasNext(); ) {
        buf.append(it.next());
        if (it.hasNext()) {
          buf.append(',');
        }
      }
      buf.append("], rate=").append(rate)
        .append(", aggregator=").append(aggregator)
        .append(", group_bys=(");
      if (group_bys != null) {
        for (final byte[] tag_id : group_bys) {
          try {
            buf.append(tsdb.tag_names.getName(tag_id));
          } catch (NoSuchUniqueId e) {
            buf.append('<').append(e.getMessage()).append('>');
          }
          buf.append(' ')
             .append(Arrays.toString(tag_id));
          if (row_key_literals != null) {
            final byte[][] value_ids = row_key_literals.get(tag_id);
            if (value_ids == null) {
              continue;
            }
            buf.append("={");
            for (final byte[] value_id : value_ids) {
              try {
                if (value_id != null) {
                  buf.append(tsdb.tag_values.getName(value_id));
                } else {
                  buf.append("null");
                }
              } catch (NoSuchUniqueId e) {
                buf.append('<').append(e.getMessage()).append('>');
              }
              buf.append(' ')
                 .append(Arrays.toString(value_id))
                 .append(", ");
            }
            buf.append('}');
          }
          buf.append(", ");
        }
      }
    }
    buf.append("))");
    return buf.toString();
  }

  /**
   * Comparator that ignores timestamps in row keys.
   */
  private static final class SpanCmp implements Comparator<byte[]> {

    private final short metric_width;

    public SpanCmp(final short metric_width) {
      this.metric_width = metric_width;
    }

    @Override
    public int compare(final byte[] a, final byte[] b) {
      final int length = Math.min(a.length, b.length);
      if (a == b) {  // Do this after accessing a.length and b.length
        return 0;    // in order to NPE if either a or b is null.
      }
      int i;
      // First compare the metric ID.
      for (i = 0; i < metric_width; i++) {
        if (a[i] != b[i]) {
          return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
        }
      }
      // Then skip the timestamp and compare the rest.
      for (i += Const.TIMESTAMP_BYTES; i < length; i++) {
        if (a[i] != b[i]) {
          return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
        }
      }
      return a.length - b.length;
    }

  }

  /** Helps unit tests inspect private methods. */
  @VisibleForTesting
  static class ForTesting {

    /** @return the start time of the HBase scan for unit tests. */
    static long getScanStartTimeSeconds(final TsdbQuery query) {
      return query.getScanStartTimeSeconds();
    }

    /** @return the end time of the HBase scan for unit tests. */
    static long getScanEndTimeSeconds(final TsdbQuery query) {
      return query.getScanEndTimeSeconds();
    }

    /** @return the downsampling interval for unit tests. */
    static long getDownsampleIntervalMs(final TsdbQuery query) {
      return query.sample_interval_ms;
    }
  
    static byte[] getMetric(final TsdbQuery query) {
      return query.metric;
    }
    
    static RateOptions getRateOptions(final TsdbQuery query) {
      return query.rate_options;
    }
    
    static List<TagVFilter> getFilters(final TsdbQuery query) {
      return query.filters;
    }
    
    static ArrayList<byte[]> getGroupBys(final TsdbQuery query) {
      return query.group_bys;
    }
    
    static ByteMap<byte[][]> getRowKeyLiterals(final TsdbQuery query) {
      return query.row_key_literals;
    }
  
  }
}
