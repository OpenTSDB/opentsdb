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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;

import static net.opentsdb.core.TSDB.checkTimestamp;
import static org.hbase.async.Bytes.ByteMap;
import net.opentsdb.stats.Histogram;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;

/**
 * Non-synchronized implementation of {@link Query}.
 */
public final class TsdbQuery implements Query {

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
  public final TSDB tsdb;

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
   * If this is non-null, {@code sample_interval_ms} must be strictly positive.
   */
  private Aggregator downsampler;

  /** Minimum time interval (in milliseconds) wanted between each data point. */
  private long sample_interval_ms;

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
  @Override
  public void setStartTime(final long timestamp) {
    checkTimestamp(timestamp);

    if (end_time != UNSET && timestamp >= getEndTime()) {
      throw new IllegalArgumentException("new start time (" + timestamp
          + ") is greater than or equal to end time: " + getEndTime());
    }

    start_time = timestamp;
  }

  /**
   * @returns the start time for the query
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
    checkTimestamp(timestamp);

    if (start_time != UNSET && timestamp <= getStartTime()) {
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
      setEndTime(System.currentTimeMillis());
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
    findGroupBys(tags);

    try {
      this.metric = tsdb.metrics.getIdAsync(metric).joinUninterruptibly();
      this.tags = Tags.resolveAllAsync(tsdb, tags).joinUninterruptibly();
    } catch (Exception e) {
      Throwables.propagate(e);
    }

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
        first_metric = tsuid.substring(0, Const.METRICS_WIDTH * 2)
          .toUpperCase();
        continue;
      }

      final String metric = tsuid.substring(0, Const.METRICS_WIDTH * 2)
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
  @Override
  public void downsample(final long interval, final Aggregator downsampler) {
    if (downsampler == null) {
      throw new NullPointerException("downsampler");
    } else if (interval <= 0) {
      throw new IllegalArgumentException("interval not > 0: " + interval);
    }
    this.downsampler = downsampler;
    this.sample_interval_ms = interval;
  }

  public long getSampleInterval() {
    return sample_interval_ms;
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
    try {
      final Iterator<Map.Entry<String, String>> i = tags.entrySet().iterator();
      while (i.hasNext()) {
        final Map.Entry<String, String> tag = i.next();
        final String tagvalue = tag.getValue();
        if ("*".equals(tagvalue)  // 'GROUP BY' with any value.
                || tagvalue.indexOf('|', 1) >= 0) {  // Multiple possible values.
          if (group_bys == null) {
            group_bys = new ArrayList<byte[]>();
          }
          group_bys.add(tsdb.tag_names.getIdAsync(tag.getKey()).joinUninterruptibly());
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
          group_by_values.put(tsdb.tag_names.getIdAsync(tag.getKey()).joinUninterruptibly(),
                  value_ids);
          for (int j = 0; j < values.length; j++) {
            final byte[] value_id = tsdb.tag_values.getIdAsync(values[j]).joinUninterruptibly();
            System.arraycopy(value_id, 0, value_ids[j], 0, value_width);
          }
        }
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  /**
  * Callback that should be attached the the output of
  * {@link net.opentsdb.storage.TsdbStore#executeQuery} to group and sort the results.
  */
  private class GroupByAndAggregateCB implements 
    Callback<DataPoints[], TreeMap<byte[], Span>>{
    
    /**
    * Creates the {@link SpanGroup}s to form the final results of this query.
    * @param spans The {@link Span}s found for this query ({@link TSDB#findSpans}).
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
        final SpanGroup group = new SpanGroup(
                tsdb.getScanStartTimeSeconds(this),
                                              tsdb.getScanEndTimeSeconds(this),
                                              spans.values(),
                                              rate, rate_options,
                                              aggregator,
                                              sample_interval_ms, downsampler);
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
          thegroup = new SpanGroup(tsdb.getScanStartTimeSeconds(this),
                                   tsdb.getScanEndTimeSeconds(this),
                                   null, rate, rate_options, aggregator,
                                   sample_interval_ms, downsampler);
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
   * Converts the start time to seconds if needed and then returns it.
   */
  public long getStartTimeSeconds() {
    long start = getStartTime();
    // down cast to seconds if we have a query in ms
    if ((start & Const.SECOND_MASK) != 0) {
      start /= 1000;
    }

    return start;
  }

  public long getEndTimeSeconds() {
    long end = getEndTime();
    if ((end & Const.SECOND_MASK) != 0) {
      end /= 1000;
    }

    return end;
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
      try {
        buf.append(" (").append(tsdb.metrics.getNameAsync(metric).joinUninterruptibly());
      } catch (NoSuchUniqueId e) {
        buf.append(" (<").append(e.getMessage()).append('>');
      } catch (Exception e) {
        Throwables.propagate(e);
      }
     try {
        try {
          buf.append("), tags=")
                  .append(Tags.resolveIdsAsync(tsdb, tags).joinUninterruptibly());
        } catch (Exception e) {
          Throwables.propagate(e);
        }
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
          buf.append(tsdb.tag_names.getNameAsync(tag_id).joinUninterruptibly());
        } catch (NoSuchUniqueId e) {
          buf.append('<').append(e.getMessage()).append('>');
        } catch (Exception e) {
          Throwables.propagate(e);
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
              buf.append(tsdb.tag_values.getNameAsync(value_id).joinUninterruptibly());
            } catch (NoSuchUniqueId e) {
              buf.append('<').append(e.getMessage()).append('>');
            } catch (Exception e) {
              Throwables.propagate(e);
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
}
