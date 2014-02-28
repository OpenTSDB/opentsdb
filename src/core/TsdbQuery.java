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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import static org.hbase.async.Bytes.ByteMap;

import net.opentsdb.stats.Histogram;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;

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
   * Tags of the metrics being looked up.
   * Each tag is a byte array holding the ID of both the name and value
   * of the tag.
   * Invariant: an element cannot be both in this array and in group_bys.
   */
  private ArrayList<byte[]> tags;

  /**
   * Tags by which we must group the results.
   * Each element is a tag ID.
   * Invariant: an element cannot be both in this array and in {@code tags}.
   */
  private ArrayList<byte[]> group_bys;

  /**
   * Values we may be grouping on.
   * For certain elements in {@code group_bys}, we may have a specific list of
   * values IDs we're looking for.  Those IDs are stored in this map.  The key
   * is an element of {@code group_bys} (so a tag name ID) and the values are
   * tag value IDs (at least two).
   */
  private ByteMap<byte[][]> group_by_values;

  /** If true, use rate of change instead of actual values. */
  private boolean rate;

  /** Specifies the various options for rate calculations */
  private RateOptions rate_options;
  
  /** Aggregator function to use. */
  private Aggregator aggregator;

  /**
   * Downsampling function to use, if any (can be {@code null}).
   * If this is non-null, {@code sample_interval} must be strictly positive.
   */
  private Aggregator downsampler;

  /** Minimum time interval (in seconds) wanted between each data point. */
  private int sample_interval;

  /** Optional list of TSUIDs to fetch and aggregate instead of a metric */
  private List<String> tsuids;
  
  /** Constructor. */
  public TsdbQuery(final TSDB tsdb) {
    this.tsdb = tsdb;
  }

  /**
   * Sets the start time for the query
   * @param timestamp Unix epoch timestamp in seconds or milliseconds
   * @throws IllegalArgumentException if the timestamp is invalid or greater
   * than the end time (if set)
   */
  public void setStartTime(final long timestamp) {
    if ((timestamp & Const.SECOND_MASK) != 0 && 
        (timestamp < 1000000000000L || timestamp > 9999999999999L)) {
      throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
    } else if (end_time != UNSET && timestamp >= getEndTime()) {
      throw new IllegalArgumentException("new start time (" + timestamp
          + ") is greater than or equal to end time: " + getEndTime());
    }
    start_time = timestamp;
  }

  /**
   * @returns the start time for the query
   * @throws IllegalStateException if the start time hasn't been set yet
   */
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
  public void setEndTime(final long timestamp) {
    if ((timestamp & Const.SECOND_MASK) != 0 && 
        (timestamp < 1000000000000L || timestamp > 9999999999999L)) {
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
  public long getEndTime() {
    if (end_time == UNSET) {
      setEndTime(System.currentTimeMillis());
    }
    return end_time;
  }

  public void setTimeSeries(final String metric,
      final Map<String, String> tags,
      final Aggregator function,
      final boolean rate) throws NoSuchUniqueName {
    setTimeSeries(metric, tags, function, rate, new RateOptions());
  }
  
  public void setTimeSeries(final String metric,
        final Map<String, String> tags,
        final Aggregator function,
        final boolean rate,
        final RateOptions rate_options)
  throws NoSuchUniqueName {
    findGroupBys(tags);
    this.metric = tsdb.metrics.getId(metric);
    this.tags = Tags.resolveAll(tsdb, tags);
    aggregator = function;
    this.rate = rate;
    this.rate_options = rate_options;
  }

  public void setTimeSeries(final List<String> tsuids,
      final Aggregator function, final boolean rate) {
    setTimeSeries(tsuids, function, rate, new RateOptions());
  }
  
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
  
  /**
   * Sets an optional downsampling function on this query
   * @param interval The interval, in milliseconds to rollup data points
   * @param downsampler An aggregation function to use when rolling up data points
   * @throws NullPointerException if the aggregation function is null
   * @throws IllegalArgumentException if the interval is not greater than 0
   */
  public void downsample(final int interval, final Aggregator downsampler) {
    if (downsampler == null) {
      throw new NullPointerException("downsampler");
    } else if (interval <= 0) {
      throw new IllegalArgumentException("interval not > 0: " + interval);
    }
    this.downsampler = downsampler;
    this.sample_interval = interval;
  }

  /**
   * Extracts all the tags we must use to group results.
   * <ul>
   * <li>If a tag has the form {@code name=*} then we'll create one
   *     group per value we find for that tag.</li>
   * <li>If a tag has the form {@code name={v1,v2,..,vN}} then we'll
   *     create {@code N} groups.</li>
   * </ul>
   * In the both cases above, {@code name} will be stored in the
   * {@code group_bys} attribute.  In the second case specifically,
   * the {@code N} values would be stored in {@code group_by_values},
   * the key in this map being {@code name}.
   * @param tags The tags from which to extract the 'GROUP BY's.
   * Each tag that represents a 'GROUP BY' will be removed from the map
   * passed in argument.
   */
  private void findGroupBys(final Map<String, String> tags) {
    final Iterator<Map.Entry<String, String>> i = tags.entrySet().iterator();
    while (i.hasNext()) {
      final Map.Entry<String, String> tag = i.next();
      final String tagvalue = tag.getValue();
      if (tagvalue.equals("*")  // 'GROUP BY' with any value.
          || tagvalue.indexOf('|', 1) >= 0) {  // Multiple possible values.
        if (group_bys == null) {
          group_bys = new ArrayList<byte[]>();
        }
        group_bys.add(tsdb.tag_names.getId(tag.getKey()));
        i.remove();
        if (tagvalue.charAt(0) == '*') {
          continue;  // For a 'GROUP BY' with any value, we're done.
        }
        // 'GROUP BY' with specific values.  Need to split the values
        // to group on and store their IDs in group_by_values.
        final String[] values = Tags.splitString(tagvalue, '|');
        if (group_by_values == null) {
          group_by_values = new ByteMap<byte[][]>();
        }
        final short value_width = tsdb.tag_values.width();
        final byte[][] value_ids = new byte[values.length][value_width];
        group_by_values.put(tsdb.tag_names.getId(tag.getKey()),
                            value_ids);
        for (int j = 0; j < values.length; j++) {
          final byte[] value_id = tsdb.tag_values.getId(values[j]);
          System.arraycopy(value_id, 0, value_ids[j], 0, value_width);
        }
      }
    }
  }

  /**
   * Executes the query
   * @return An array of data points with one time series per array value
   */
  public DataPoints[] run() throws HBaseException {
    try {
      return runAsync().joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
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
   * @throws IllegalArgumentException if bad data was retreived from HBase.
   */
  private Deferred<TreeMap<byte[], Span>> findSpans() throws HBaseException {
    final short metric_width = tsdb.metrics.width();
    final TreeMap<byte[], Span> spans = // The key is a row key from HBase.
      new TreeMap<byte[], Span>(new SpanCmp(metric_width));
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
      int hbase_time = 0; // milliseconds.
      long starttime = System.nanoTime();
      
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
                 spans.size() + " spans");
             if (nrows < 1) {
               results.callback(null);
             } else {
               results.callback(spans);
             }
             scanner.close();
             return null;
           }
           
           for (final ArrayList<KeyValue> row : rows) {
             final byte[] key = row.get(0).key();
             if (Bytes.memcmp(metric, key, 0, metric_width) != 0) {
               scanner.close();
               throw new IllegalDataException(
                   "HBase returned a row that doesn't match"
                   + " our scanner (" + scanner + ")! " + row + " does not start"
                   + " with " + Arrays.toString(metric));
             }
             Span datapoints = spans.get(key);
             if (datapoints == null) {
               datapoints = new Span(tsdb);
               spans.put(key, datapoints);
             }
             final KeyValue compacted = 
               tsdb.compact(row, datapoints.getAnnotations());
             if (compacted != null) { // Can be null if we ignored all KVs.
               datapoints.addRow(compacted);
               nrows++;
             }
           }
           
           return scan();
         } catch (Exception e) {
           scanner.close();
           results.callback(e);
           return null;
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
    public DataPoints[] call(final TreeMap<byte[], Span> spans) throws Exception {
      if (spans == null || spans.size() <= 0) {
        return NO_RESULT;
      }
      if (group_bys == null) {
        // We haven't been asked to find groups, so let's put all the spans
        // together in the same group.
        final SpanGroup group = new SpanGroup(tsdb,
                                              getScanStartTime(),
                                              getScanEndTime(),
                                              spans.values(),
                                              rate, rate_options,
                                              aggregator,
                                              sample_interval, downsampler);
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
          thegroup = new SpanGroup(tsdb, getScanStartTime(), getScanEndTime(),
                                   null, rate, rate_options, aggregator,
                                   sample_interval, downsampler);
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
    final short metric_width = tsdb.metrics.width();
    final byte[] start_row = new byte[metric_width + Const.TIMESTAMP_BYTES];
    final byte[] end_row = new byte[metric_width + Const.TIMESTAMP_BYTES];
    // We search at least one row before and one row after the start & end
    // time we've been given as it's quite likely that the exact timestamp
    // we're looking for is in the middle of a row.  Plus, a number of things
    // rely on having a few extra data points before & after the exact start
    // & end dates in order to do proper rate calculation or downsampling near
    // the "edges" of the graph.
    Bytes.setInt(start_row, (int) getScanStartTime(), metric_width);
    Bytes.setInt(end_row, (end_time == UNSET
                           ? -1  // Will scan until the end (0xFFF...).
                           : (int) getScanEndTime()),
                 metric_width);
    
    // set the metric UID based on the TSUIDs if given, or the metric UID
    if (tsuids != null && !tsuids.isEmpty()) {
      final String tsuid = tsuids.get(0);
      final String metric_uid = tsuid.substring(0, TSDB.metrics_width() * 2);
      metric = UniqueId.stringToUid(metric_uid);
      System.arraycopy(metric, 0, start_row, 0, metric_width);
      System.arraycopy(metric, 0, end_row, 0, metric_width); 
    } else {
      System.arraycopy(metric, 0, start_row, 0, metric_width);
      System.arraycopy(metric, 0, end_row, 0, metric_width);
    }

    final Scanner scanner = tsdb.client.newScanner(tsdb.table);
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    if (tsuids != null && !tsuids.isEmpty()) {
      createAndSetTSUIDFilter(scanner);
    } else if (tags.size() > 0 || group_bys != null) {
      createAndSetFilter(scanner);
    }
    scanner.setFamily(TSDB.FAMILY);
    return scanner;
  }

  /** Returns the UNIX timestamp from which we must start scanning.  */
  private long getScanStartTime() {
    // The reason we look before by `MAX_TIMESPAN * 2' seconds is because of
    // the following.  Let's assume MAX_TIMESPAN = 600 (10 minutes) and the
    // start_time = ... 12:31:00.  If we initialize the scanner to look
    // only 10 minutes before, we'll start scanning at time=12:21, which will
    // give us the row that starts at 12:30 (remember: rows are always aligned
    // on MAX_TIMESPAN boundaries -- so in this example, on 10m boundaries).
    // But we need to start scanning at least 1 row before, so we actually
    // look back by twice MAX_TIMESPAN.  Only when start_time is aligned on a
    // MAX_TIMESPAN boundary then we'll mistakenly scan back by an extra row,
    // but this doesn't really matter.
    // Additionally, in case our sample_interval is large, we need to look
    // even further before/after, so use that too.
    long start = getStartTime();
    // down cast to seconds if we have a query in ms
    if ((start & Const.SECOND_MASK) != 0) {
      start /= 1000;
    }
    final long ts = start - Const.MAX_TIMESPAN * 2 - sample_interval;
    return ts > 0 ? ts : 0;
  }

  /** Returns the UNIX timestamp at which we must stop scanning.  */
  private long getScanEndTime() {
    // For the end_time, we have a different problem.  For instance if our
    // end_time = ... 12:30:00, we'll stop scanning when we get to 12:40, but
    // once again we wanna try to look ahead one more row, so to avoid this
    // problem we always add 1 second to the end_time.  Only when the end_time
    // is of the form HH:59:59 then we will scan ahead an extra row, but once
    // again that doesn't really matter.
    // Additionally, in case our sample_interval is large, we need to look
    // even further before/after, so use that too.
    long end = getEndTime();
    if ((end & Const.SECOND_MASK) != 0) {
      end /= 1000;
    }
    return end + Const.MAX_TIMESPAN + 1 + sample_interval;
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
           * (tags.size() + (group_bys == null ? 0 : group_bys.size() * 3))));
    // In order to avoid re-allocations, reserve a bit more w/ groups ^^^

    // Alright, let's build this regexp.  From the beginning...
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
               + "^.{")
       // ... start by skipping the metric ID and timestamp.
       .append(tsdb.metrics.width() + Const.TIMESTAMP_BYTES)
       .append("}");
    final Iterator<byte[]> tags = this.tags.iterator();
    final Iterator<byte[]> group_bys = (this.group_bys == null
                                        ? new ArrayList<byte[]>(0).iterator()
                                        : this.group_bys.iterator());
    byte[] tag = tags.hasNext() ? tags.next() : null;
    byte[] group_by = group_bys.hasNext() ? group_bys.next() : null;
    // Tags and group_bys are already sorted.  We need to put them in the
    // regexp in order by ID, which means we just merge two sorted lists.
    do {
      // Skip any number of tags.
      buf.append("(?:.{").append(tagsize).append("})*\\Q");
      if (isTagNext(name_width, tag, group_by)) {
        addId(buf, tag);
        tag = tags.hasNext() ? tags.next() : null;
      } else {  // Add a group_by.
        addId(buf, group_by);
        final byte[][] value_ids = (group_by_values == null
                                    ? null
                                    : group_by_values.get(group_by));
        if (value_ids == null) {  // We don't want any specific ID...
          buf.append(".{").append(value_width).append('}');  // Any value ID.
        } else {  // We want specific IDs.  List them: /(AAA|BBB|CCC|..)/
          buf.append("(?:");
          for (final byte[] value_id : value_ids) {
            buf.append("\\Q");
            addId(buf, value_id);
            buf.append('|');
          }
          // Replace the pipe of the last iteration.
          buf.setCharAt(buf.length() - 1, ')');
        }
        group_by = group_bys.hasNext() ? group_bys.next() : null;
      }
    } while (tag != group_by);  // Stop when they both become null.
    // Skip any number of tags before the end.
    buf.append("(?:.{").append(tagsize).append("})*$");
    scanner.setKeyRegexp(buf.toString(), CHARSET);
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
       .append(tsdb.metrics.width() + Const.TIMESTAMP_BYTES)
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
   * Helper comparison function to compare tag name IDs.
   * @param name_width Number of bytes used by a tag name ID.
   * @param tag A tag (array containing a tag name ID and a tag value ID).
   * @param group_by A tag name ID.
   * @return {@code true} number if {@code tag} should be used next (because
   * it contains a smaller ID), {@code false} otherwise.
   */
  private boolean isTagNext(final short name_width,
                            final byte[] tag,
                            final byte[] group_by) {
    if (tag == null) {
      return false;
    } else if (group_by == null) {
      return true;
    }
    final int cmp = Bytes.memcmp(tag, group_by, 0, name_width);
    if (cmp == 0) {
      throw new AssertionError("invariant violation: tag ID "
          + Arrays.toString(group_by) + " is both in 'tags' and"
          + " 'group_bys' in " + this);
    }
    return cmp < 0;
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
      try {
        buf.append(" (").append(tsdb.metrics.getName(metric));
      } catch (NoSuchUniqueId e) {
        buf.append(" (<").append(e.getMessage()).append('>');
      }
      try {
        buf.append("), tags=").append(Tags.resolveIds(tsdb, tags));
      } catch (NoSuchUniqueId e) {
        buf.append("), tags=<").append(e.getMessage()).append('>');
      }
    }
    buf.append(", rate=").append(rate)
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
        if (group_by_values != null) {
          final byte[][] value_ids = group_by_values.get(tag_id);
          if (value_ids == null) {
            continue;
          }
          buf.append("={");
          for (final byte[] value_id : value_ids) {
            try {
              buf.append(tsdb.tag_values.getName(value_id));
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

}
