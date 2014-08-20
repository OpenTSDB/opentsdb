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

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.hbase.async.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;

import static com.google.common.base.Preconditions.checkNotNull;
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

  /**
   * Used whenever there are no results.
   */
  private static final DataPoints[] NO_RESULT = new DataPoints[0];

  /**
   * Keep track of the latency we perceive when doing Scans on HBase.
   * We want buckets up to 16s, with 2 ms interval between each bucket up to
   * 100 ms after we which we switch to exponential buckets.
   */
  static final Histogram scanlatency = new Histogram(16000, (short) 2, 100);

  /**
   * ID of the metric being looked up.
   */
  private final byte[] metric;

  /**
   * Optional list of TSUIDs to fetch and aggregate instead of a metric
   */
  private final List<String> tsuids;

  /**
   * Tags of the metrics being looked up.
   * Each tag is a byte array holding the ID of both the name and value
   * of the tag.
   * Invariant: an element cannot be both in this array and in group_bys.
   */
  private final ArrayList<byte[]> tags;

  /**
   * Start time (UNIX timestamp in seconds) on 32 bits ("unsigned" int).
   */
  private final long start_time;

  /**
   * End time (UNIX timestamp in seconds) on 32 bits ("unsigned" int).
   */
  private final Optional<Long> end_time;

  /**
   * Tags by which we must group the results.
   * Each element is a tag ID.
   * Invariant: an element cannot be both in this array and in {@code tags}.
   */
  private final List<byte[]> group_bys;

  /**
   * Values we may be grouping on.
   * For certain elements in {@code group_bys}, we may have a specific list of
   * values IDs we're looking for.  Those IDs are stored in this map.  The key
   * is an element of {@code group_bys} (so a tag name ID) and the values are
   * tag value IDs (at least two).
   */
  private final Map<byte[], ArrayList<byte[]>> group_by_values;

  /**
   * If true, use rate of change instead of actual values.
   */
  private final boolean rate;

  /**
   * Specifies the various options for rate calculations
   */
  private final RateOptions rate_options;

  /**
   * Aggregator function to use.
   */
  private final Aggregator aggregator;

  /**
   * Downsampling function to use, if any (can be {@code null}).
   * If this is non-null, {@code sample_interval_ms} must be strictly positive.
   */
  private final Aggregator downsampler;

  /**
   * Minimum time interval (in milliseconds) wanted between each data point.
   */
  private final long sample_interval_ms;

  /**
   * Constructor.
   */
  TsdbQuery(final byte[] metric_id,
            final ArrayList<byte[]> tags,
            final long start_time,
            final Optional<Long> end_time,
            final Aggregator aggregator,
            final Aggregator downsampler,
            final long interval,
            final boolean rate,
            final RateOptions rate_options,
            final List<byte[]> group_bys,
            final Map<byte[], ArrayList<byte[]>> group_by_values) {
    this.metric = checkNotNull(metric_id);
    this.tags = checkNotNull(tags);
    this.start_time = checkTimestamp(start_time);
    this.end_time = checkTimestamp(end_time);

    this.aggregator = aggregator;
    this.downsampler = downsampler;
    this.sample_interval_ms = interval;

    this.rate = rate;
    this.rate_options = rate_options;

    this.group_bys = group_bys;
    this.group_by_values = group_by_values;

    this.tsuids = null;
  }

  TsdbQuery(final byte[] metric_id,
            final List<String> tsuids,
            final long start_time,
            final Optional<Long> end_time,
            final Aggregator aggregator,
            final Aggregator downsampler,
            final long interval,
            final boolean rate,
            final RateOptions rate_options) {
    this.metric = checkNotNull(metric_id);
    this.tsuids = checkNotNull(tsuids);
    this.start_time = checkTimestamp(start_time);
    this.end_time = checkTimestamp(end_time);

    this.aggregator = aggregator;
    this.downsampler = downsampler;
    this.sample_interval_ms = interval;

    this.rate = rate;
    this.rate_options = rate_options;

    this.group_bys = null;
    this.group_by_values = null;

    this.tags = null;
  }

  /**
   * @return the start time for the query
   * @throws IllegalStateException if the start time hasn't been set yet
   */
  @Override
  public long getStartTime() {
    return start_time;
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

  /**
   * @return the configured end time. If the end time hasn't been set, the
   * current system time will be stored and returned.
   */
  @Override
  public Optional<Long> getEndTime() {
    return end_time;
  }

  /**
   * Converts the end time to seconds if needed and then returns it.
   */
  public Optional<Long> getEndTimeSeconds() {
    if (end_time.isPresent()) {
      long end = end_time.get();
      if ((end & Const.SECOND_MASK) != 0) {
        end /= 1000;
      }

      return Optional.of(end);
    }

    return end_time;
  }

  public long getSampleInterval() {
    return sample_interval_ms;
  }

  public byte[] getMetric() {
    return metric;
  }

  /**
   * Callback that should be attached the the output of
   * {@link net.opentsdb.storage.TsdbStore#executeQuery} to group and sort the results.
   */
  private class GroupByAndAggregateCB implements
          Callback<DataPoints[], TreeMap<byte[], Span>> {

    /**
     * Creates the {@link SpanGroup}s to form the final results of this query.
     *
     * @param spans The {@link Span}s found for this query ({@link TSDB#findSpans}).
     *              Can be {@code null}, in which case the array returned will be empty.
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
        return new SpanGroup[]{group};
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

  @Override
  public String toString() {
    Objects.ToStringHelper str_helper = Objects.toStringHelper(this);

    str_helper.add("start_time", start_time)
            .add("end_time", end_time);

    if (tsuids != null && !tsuids.isEmpty()) {
      str_helper.add("tsuids", Joiner.on(", ")
              .skipNulls()
              .join(tsuids));
    } else {
      str_helper.add("metric", Arrays.toString(metric));
      str_helper.add("tags", Joiner.on(", ")
              .skipNulls()
              .join(tags));
    }

    str_helper.add("rate", rate)
            .add("aggregator", aggregator);

    if (group_bys != null) {
      str_helper.add("group_bys", Joiner.on(", ")
              .skipNulls()
              .join(group_bys));
    }

    if (group_by_values != null) {
      str_helper.add("group_by_values", Joiner.on(", ")
              .skipNulls()
              .join(group_by_values));
    }

    return str_helper.toString();
  }
}
