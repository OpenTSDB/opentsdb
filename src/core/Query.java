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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.opentsdb.core.TSDB.checkTimestamp;

import net.opentsdb.stats.Histogram;

/**
 * Non-synchronized implementation of {@link Query}.
 */
public final class Query {
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
  Query(final byte[] metric_id,
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
    this.end_time = checkEndTime(end_time);

    this.aggregator = aggregator;
    this.downsampler = downsampler;
    this.sample_interval_ms = interval;

    this.rate = rate;
    this.rate_options = rate_options;

    this.group_bys = group_bys;
    this.group_by_values = group_by_values;

    this.tsuids = null;
  }

  Query(final byte[] metric_id,
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
    this.end_time = checkEndTime(end_time);

    this.aggregator = aggregator;
    this.downsampler = downsampler;
    this.sample_interval_ms = interval;

    this.rate = rate;
    this.rate_options = rate_options;

    this.group_bys = null;
    this.group_by_values = null;

    this.tags = null;
  }

  private Optional<Long> checkEndTime(final Optional<Long> end_time) {
    if (end_time.isPresent()) {
      checkTimestamp(end_time.get());
    }

    return end_time;
  }

  /**
   * The start time for the query
   * @return A strictly positive integer.
   */
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
   * Returns the end time of the query.
   * @return A strictly positive integer.
   */
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

  public List<String> getTSUIDS() {
    return tsuids;
  }

  public ArrayList<byte[]> getTags() {
    return tags;
  }

  public List<byte[]> getGroupBys() {
    return group_bys;
  }

  public Map<byte[], ArrayList<byte[]>> getGroupByValues() {
    return group_by_values;
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
      String gbv_str = Joiner.on(',').withKeyValueSeparator("=").join(group_by_values);
      str_helper.add("group_by_values", gbv_str);
    }

    return str_helper.toString();
  }

  public boolean getRate() {
    return rate;
  }

  public RateOptions getRateOptions() {
    return rate_options;
  }

  public Aggregator getAggregator() {
    return aggregator;
  }

  public Aggregator getDownsampler() {
    return downsampler;
  }
}
