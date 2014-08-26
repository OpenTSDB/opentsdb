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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.hbase.async.Bytes;

import net.opentsdb.meta.Annotation;

import static net.opentsdb.core.Timestamp.inMilliseconds;

/**
 * Groups multiple spans together and offers a dynamic "view" on them.
 * <p>
 * This is used for queries to the TSDB, where we might group multiple
 * {@link Span}s that are for the same time series but different tags
 * together.  We need to "hide" data points that are outside of the
 * time period of the query and do on-the-fly aggregation of the data
 * points coming from the different Spans, using an {@link Aggregator}.
 * Since not all the Spans will have their data points at exactly the
 * same time, we also do on-the-fly linear interpolation.  If needed,
 * this view can also return the rate of change instead of the actual
 * data points.
 * <p>
 * This is one of the rare (if not the only) implementations of
 * {@link DataPoints} for which {@link #getTags} can potentially return
 * an empty map.
 * <p>
 * The implementation can also dynamically downsample the data when a
 * sampling interval a downsampling function (in the form of an
 * {@link Aggregator}) are given.  This is done by using a special
 * iterator when using the {@link Span.DownsamplingIterator}.
 */
final class SpanGroup implements DataPoints {
  /** Annotations */
  private final ArrayList<Annotation> annotations;

  /** Start time (UNIX timestamp in seconds or ms) on 32 bits ("unsigned" int). */
  private final long start_time;

  /** End time (UNIX timestamp in seconds or ms) on 32 bits ("unsigned" int). */
  private final long end_time;

  /**
   * The tags of this group.
   * This is the intersection set between the tags of all the Spans
   * in this group.
   * @see #computeTags
   */
  private Map<byte[], byte[]> tags;

  /**
   * The names of the tags that aren't shared by every single data point.
   * This is the symmetric difference between the tags of all the Spans
   * in this group.
   * @see #computeTags
   */
  private List<byte[]> aggregated_tags;

  /** Spans in this group.  They must all be for the same metric. */
  private final ArrayList<Span> spans = new ArrayList<Span>();

  /** If true, use rate of change instead of actual values. */
  private final boolean rate;
  
  /** Specifies the various options for rate calculations */
  private RateOptions rate_options; 

  /** Aggregator to use to aggregate data points from different Spans. */
  private final Aggregator aggregator;

  /**
   * Downsampling function to use, if any (can be {@code null}).
   * If this is non-null, {@code sample_interval} must be strictly positive.
   */
  private final Aggregator downsampler;

  /** Minimum time interval (in seconds) wanted between each data point. */
  private final long sample_interval;

  /**
   * Ctor.
   * @param start_time Any data point strictly before this timestamp will be
   * ignored.
   * @param end_time Any data point strictly after this timestamp will be
   * ignored.
   * @param spans A sequence of initial {@link Spans} to add to this group.
   * Ignored if {@code null}.  Additional spans can be added with {@link #add}.
   * @param rate If {@code true}, the rate of the series will be used instead
   * of the actual values.
   * @param aggregator The aggregation function to use.
   * @param interval Number of milliseconds wanted between each data point.
   * @param downsampler Aggregation function to use to group data points
   */
  SpanGroup(final long start_time, final long end_time,
            final Iterable<Span> spans,
            final boolean rate,
            final Aggregator aggregator,
            final long interval, final Aggregator downsampler) {
    this(start_time, end_time, spans, rate, new RateOptions(false,
        Long.MAX_VALUE, RateOptions.DEFAULT_RESET_VALUE), aggregator, interval,
        downsampler);
  }

  /**
   * Ctor.
   * @param start_time Any data point strictly before this timestamp will be
   * ignored.
   * @param end_time Any data point strictly after this timestamp will be
   * ignored.
   * @param spans A sequence of initial {@link Spans} to add to this group.
   * Ignored if {@code null}. Additional spans can be added with {@link #add}.
   * @param rate If {@code true}, the rate of the series will be used instead
   * of the actual values.
   * @param rate_options Specifies the optional additional rate calculation options.
   * @param aggregator The aggregation function to use.
   * @param interval Number of milliseconds wanted between each data point.
   * @param downsampler Aggregation function to use to group data points
   * within an interval.
   * @since 2.0
   */
  SpanGroup(final long start_time, final long end_time,
            final Iterable<Span> spans,
            final boolean rate, final RateOptions rate_options,
            final Aggregator aggregator,
            final long interval, final Aggregator downsampler) {
    annotations = new ArrayList<Annotation>();
    this.start_time = inMilliseconds(start_time);
    this.end_time = inMilliseconds(end_time);
    if (spans != null) {
      for (final Span span : spans) {
        add(span);
      }
    }
    this.rate = rate;
    this.rate_options = rate_options;
    this.aggregator = aggregator;
    this.downsampler = downsampler;
    this.sample_interval = interval;
  }

  /**
   * Adds a span to this group, provided that it's in the right time range.
   * <b>Must not</b> be called once {@link #getTags} or
   * {@link #getAggregatedTags} has been called on this instance.
   * @param span The span to add to this group.  If none of the data points
   * fall within our time range, this method will silently ignore that span.
   */
  void add(final Span span) {
    if (tags != null) {
      throw new AssertionError("The set of tags has already been computed"
                               + ", you can't add more Spans to " + this);
    }

    // normalize timestamps to milliseconds for proper comparison
    final long start = inMilliseconds(start_time);
    final long end = inMilliseconds(end_time);

    if (span.size() == 0) {
      // copy annotations that are in the time range
      for (Annotation annot : span.getAnnotations()) {
        long annot_start = inMilliseconds(annot.getStartTime());
        long annot_end = inMilliseconds(annot.getStartTime());

        if (annot_end >= start && annot_start <= end) {
          annotations.add(annot);
        }
      }
    } else {
      long first_dp = inMilliseconds(span.timestamp(0));

      // The following call to timestamp() will throw an
      // IndexOutOfBoundsException if size == 0, which is OK since it would
      // be a programming error.
      long last_dp = inMilliseconds(span.timestamp(span.size() - 1));

      if (first_dp <= end && last_dp >= start) {
        this.spans.add(span);
        annotations.addAll(span.getAnnotations());
      }
    }
  }

  /**
   * Computes the intersection set + symmetric difference of tags in all spans.
   * @param spans A collection of spans for which to find the common tags.
   * @return A (possibly empty) map of the tags common to all the spans given.
   */
  private void computeTags() {
    if (spans.isEmpty()) {
      tags = Maps.newHashMapWithExpectedSize(0);
      aggregated_tags = Lists.newArrayListWithCapacity(0);
      return;
    }

    final Set<byte[]> discarded_tags = Sets.newTreeSet(Bytes.MEMCMP);

    final Iterator<Span> it = spans.iterator();
    Map<byte[], byte[]> tags2 = it.next().tags();

    // This algorithm removes tags from the <code>tags</code> instance variable
    // when that specific tag name doesn't exist in <code>span_tags</code> or
    // the value of that tag in <code>span_tags</code> isn't equal to the tag
    // value in tags.
    while (it.hasNext()) {
      final Map<byte[], byte[]> span_tags = it.next().tags();

      for (Map.Entry<byte[], byte[]> entry : tags2.entrySet()) {
        final byte[] value = span_tags.get(entry.getKey());

        if (value == null || !Arrays.equals(value, entry.getValue())) {
          tags2.remove(entry.getKey());
          discarded_tags.add(entry.getKey());
        }
      }
    }

    tags = ImmutableMap.copyOf(tags2);
    aggregated_tags = ImmutableList.copyOf(discarded_tags);
  }

  /**
   * @see DataPoints#metric()
   */
  @Override
  public byte[] metric() {
    // TODO(luuse): Should not return a 0 byte like this. An optional would be better
    return spans.isEmpty() ? new byte[] {0} : spans.get(0).metric();
  }

  /**
   * @see DataPoints#tags()
   */
  @Override
  public Map<byte[], byte[]> tags() {
    computeTags();
    return tags;
  }

  /**
   * @see DataPoints#aggregatedTags()
   */
  @Override
  public List<byte[]> aggregatedTags() {
    computeTags();
    return aggregated_tags;
  }

  public List<String> getTSUIDs() {
    List<String> tsuids = new ArrayList<String>(spans.size());
    for (Span sp : spans) {
      tsuids.addAll(sp.getTSUIDs());
    }
    return tsuids;
  }
  
  /**
   * Compiles the annotations for each span into a new array list
   * @return Null if none of the spans had any annotations, a list if one or
   * more were found
   */
  public List<Annotation> getAnnotations() {
    return annotations.isEmpty() ? null : annotations;
  }

  public int size() {
    // TODO(tsuna): There is a way of doing this way more efficiently by
    // inspecting the Spans and counting only data points that fall in
    // our time range.
    final SeekableView it = iterator();
    int size = 0;
    while (it.hasNext()) {
      it.next();
      size++;
    }
    return size;
  }

  public int aggregatedSize() {
    int size = 0;
    for (final Span span : spans) {
      size += span.size();
    }
    return size;
  }

  public SeekableView iterator() {
    return AggregationIterator.create(spans, start_time, end_time, aggregator,
                                  aggregator.interpolationMethod(),
                                  downsampler, sample_interval,
                                  rate, rate_options);
  }

  /**
   * Finds the {@code i}th data point of this group in {@code O(n)}.
   * Where {@code n} is the number of data points in this group.
   */
  private DataPoint getDataPoint(int i) {
    return Iterators.get(iterator(), i);
  }

  public long timestamp(final int i) {
    return getDataPoint(i).timestamp();
  }

  public boolean isInteger(final int i) {
    return getDataPoint(i).isInteger();
  }

  public double doubleValue(final int i) {
    return getDataPoint(i).doubleValue();
  }

  public long longValue(final int i) {
    return getDataPoint(i).longValue();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
            .add("start_time", start_time)
            .add("end_time", end_time)
            .add("tags", tags)
            .add("aggregated_tags", aggregated_tags)
            .add("rate", rate)
            .add("aggregator", aggregator)
            .add("downsampler", downsampler)
            .add("sample_interval", sample_interval)
            .add("spans", spans)
            .toString();
  }
}
