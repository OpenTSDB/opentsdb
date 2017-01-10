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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hbase.async.Bytes;
import org.hbase.async.Bytes.ByteMap;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.meta.Annotation;

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
  private Map<String, String> tags;
  private ByteMap<byte[]> tag_uids;

  /**
   * The names of the tags that aren't shared by every single data point.
   * This is the symmetric difference between the tags of all the Spans
   * in this group.
   * @see #computeTags
   */
  private List<String> aggregated_tags;
  private Set<byte[]> aggregated_tag_uids;

  /** Spans in this group.  They must all be for the same metric. */
  private final ArrayList<Span> spans = new ArrayList<Span>();

  /** If true, use rate of change instead of actual values. */
  private final boolean rate;
  
  /** Specifies the various options for rate calculations */
  private RateOptions rate_options; 

  /** Aggregator to use to aggregate data points from different Spans. */
  private final Aggregator aggregator;

  /** Downsampling specification to use, if any (can be {@code null}). */
  private DownsamplingSpecification downsampler;
  
  /** Start timestamp of the query for filtering */  
  private final long query_start;
  
  /** End timestamp of the query for filtering */
  private final long query_end;  

  /** Index of the query in the TSQuery class */
  private final int query_index;
  
  /** Whether or not the query is for rolled up data */
  private final boolean is_rollup;
  
  /** The TSDB to which we belong, used for resolution */
  private final TSDB tsdb;
  
  /**
   * Ctor.
   * @param tsdb The TSDB we belong to.
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
   * within an interval.
   */
  SpanGroup(final TSDB tsdb,
            final long start_time, final long end_time,
            final Iterable<Span> spans,
            final boolean rate,
            final Aggregator aggregator,
            final long interval, final Aggregator downsampler) {
    this(tsdb, start_time, end_time, spans, rate, new RateOptions(false,
        Long.MAX_VALUE, RateOptions.DEFAULT_RESET_VALUE), aggregator, interval,
        downsampler);
  }

  /**
   * Ctor.
   * @param tsdb The TSDB we belong to.
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
  SpanGroup(final TSDB tsdb,
            final long start_time, final long end_time,
            final Iterable<Span> spans,
            final boolean rate, final RateOptions rate_options,
            final Aggregator aggregator,
            final long interval, final Aggregator downsampler) {
    this(tsdb, start_time, end_time, spans, rate, rate_options, aggregator, 
        interval, downsampler, -1, FillPolicy.NONE);
  }

  /**
   * Ctor.
   * @param tsdb The TSDB we belong to.
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
   * @param query_index index of the original query
   * @param fill_policy Policy specifying whether to interpolate or to fill
   * missing intervals with special values.
   * @since 2.2
   */
  SpanGroup(final TSDB tsdb,
            final long start_time, final long end_time,
            final Iterable<Span> spans,
            final boolean rate, final RateOptions rate_options,
            final Aggregator aggregator,
            final long interval, final Aggregator downsampler, final int query_index,
            final FillPolicy fill_policy) {
     this(tsdb, start_time, end_time, spans, rate, rate_options, aggregator,
         downsampler != null ? 
             new DownsamplingSpecification(interval, downsampler, fill_policy) : 
           null,
         0, 0, query_index);
  }
  
  /**
   * Ctor.
   * @param tsdb The TSDB we belong to.
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
   * @param downsampler The specification to use for downsampling, may be null.
   * @param query_start Start of the actual query
   * @param query_end End of the actual query
   * @param query_index index of the original query
   * @since 2.3
   */
  SpanGroup(final TSDB tsdb,
            final long start_time, 
            final long end_time,
            final Iterable<Span> spans,
            final boolean rate, 
            final RateOptions rate_options,
            final Aggregator aggregator,
            final DownsamplingSpecification downsampler, 
            final long query_start,
            final long query_end,
            final int query_index) {
     this(tsdb, start_time, end_time, spans, rate, rate_options, aggregator, 
         downsampler, query_start, query_end, query_index, false);
  }
  
  /**
   * Ctor.
   * @param tsdb The TSDB we belong to.
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
   * @param downsampler The specification to use for downsampling, may be null.
   * @param query_start Start of the actual query
   * @param query_end End of the actual query
   * @param query_index index of the original query
   * @param is_rollup Whether or not this query is handling rolled up data
   * @since 2.4
   */
  SpanGroup(final TSDB tsdb,
            final long start_time, 
            final long end_time,
            final Iterable<Span> spans,
            final boolean rate, 
            final RateOptions rate_options,
            final Aggregator aggregator,
            final DownsamplingSpecification downsampler, 
            final long query_start,
            final long query_end,
            final int query_index,
            final boolean is_rollup) {
     annotations = new ArrayList<Annotation>();
     this.start_time = (start_time & Const.SECOND_MASK) == 0 ? 
         start_time * 1000 : start_time;
     this.end_time = (end_time & Const.SECOND_MASK) == 0 ? 
         end_time * 1000 : end_time;
     if (spans != null) {
       for (final Span span : spans) {
         add(span);
       }
     }
     this.rate = rate;
     this.rate_options = rate_options;
     this.aggregator = aggregator;
     this.downsampler = downsampler;
     this.query_start = query_start;
     this.query_end = query_end;
     this.query_index = query_index;
     this.is_rollup = is_rollup;
     this.tsdb = tsdb;
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
    final long start = (start_time & Const.SECOND_MASK) == 0 ? 
        start_time * 1000 : start_time;
    final long end = (end_time & Const.SECOND_MASK) == 0 ? 
        end_time * 1000 : end_time;

    if (span.size() == 0) {
      // copy annotations that are in the time range
      for (Annotation annot : span.getAnnotations()) {
        long annot_start = annot.getStartTime();
        if ((annot_start & Const.SECOND_MASK) == 0) {
          annot_start *= 1000;
        }
        long annot_end = annot.getStartTime();
        if ((annot_end & Const.SECOND_MASK) == 0) {
          annot_end *= 1000;
        }
        if (annot_end >= start && annot_start <= end) {
          annotations.add(annot);
        }
      }
    } else {
      long first_dp = span.timestamp(0);
      if ((first_dp & Const.SECOND_MASK) == 0) {
        first_dp *= 1000;
      }
      // The following call to timestamp() will throw an
      // IndexOutOfBoundsException if size == 0, which is OK since it would
      // be a programming error.
      long last_dp = span.timestamp(span.size() - 1);
      if ((last_dp & Const.SECOND_MASK) == 0) {
        last_dp *= 1000;
      }
      if (first_dp <= end && last_dp >= start) {
        this.spans.add(span);
        annotations.addAll(span.getAnnotations());
      }
    }
  }

  /**
   * Computes the intersection set + symmetric difference of tags in all spans.
   * This method loads the UID aggregated list and tag pair maps with byte arrays
   * but does not actually resolve the UIDs to strings. 
   * On the first run, it will initialize the UID collections (which may be empty)
   * and subsequent calls will skip processing.
   */
  private void computeTags() {
    if (tag_uids != null && aggregated_tag_uids != null) {
      return;
    }
    if (spans.isEmpty()) {
      tag_uids = new ByteMap<byte[]>();
      aggregated_tag_uids = new HashSet<byte[]>();
      return;
    }
    
    // local tag uids
    final ByteMap<byte[]> tag_set = new ByteMap<byte[]>();
    
    // value is always null, we just want the set of unique keys
    final ByteMap<byte[]> discards = new ByteMap<byte[]>();
    final Iterator<Span> it = spans.iterator();
    while (it.hasNext()) {
      final Span span = it.next();
      final ByteMap<byte[]> uids = span.getTagUids();
      
      for (final Map.Entry<byte[], byte[]> tag_pair : uids.entrySet()) {
        // we already know it's an aggregated tag
        if (discards.containsKey(tag_pair.getKey())) {
          continue;
        }
        
        final byte[] tag_value = tag_set.get(tag_pair.getKey());
        if (tag_value == null) {
          tag_set.put(tag_pair.getKey(), tag_pair.getValue());
        } else if (Bytes.memcmp(tag_value, tag_pair.getValue()) != 0) {
          // bump to aggregated tags
          discards.put(tag_pair.getKey(), null);
          tag_set.remove(tag_pair.getKey());
        }
      }
    }
    
    aggregated_tag_uids = discards.keySet();
    tag_uids = tag_set;
  }

  public String metricName() {
    try {
      return metricNameAsync().joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  public Deferred<String> metricNameAsync() {
    return spans.isEmpty() ? Deferred.fromResult("") : 
      spans.get(0).metricNameAsync();
  }

  @Override
  public byte[] metricUID() {
    return spans.isEmpty() ? new byte[] {} : spans.get(0).metricUID();
  }
  
  public Map<String, String> getTags() {
    try {
      return getTagsAsync().joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  public Deferred<Map<String, String>> getTagsAsync() {
    if (tags != null) {
      return Deferred.fromResult(tags);
    }
    
    if (spans.isEmpty()) {
      tags = new HashMap<String, String>(0);
      return Deferred.fromResult(tags);
    }
    
    if (tag_uids == null) {
      computeTags();
    }
    
    return resolveTags(tag_uids);
  }

  @Override
  public ByteMap<byte[]> getTagUids() {
    if (tag_uids == null) {
      computeTags();
    }
    return tag_uids;
  }
  
  public List<String> getAggregatedTags() {
    try {
      return getAggregatedTagsAsync().joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  public Deferred<List<String>> getAggregatedTagsAsync() {
    if (aggregated_tags != null) {
      return Deferred.fromResult(aggregated_tags);
    }
    
    if (spans.isEmpty()) {
      aggregated_tags = new ArrayList<String>(0);
      return Deferred.fromResult(aggregated_tags);
    }
    
    if (aggregated_tag_uids == null) {
      computeTags();
    }
    
    return resolveAggTags(aggregated_tag_uids);
  }
  
  @Override
  public List<byte[]> getAggregatedTagUids() {
    if (aggregated_tag_uids != null) {
      return new ArrayList<byte[]>(aggregated_tag_uids);
    }
    
    if (spans.isEmpty()) {
      return Collections.emptyList();
    }
    
    if (aggregated_tag_uids == null) {
      computeTags();
    }
    return new ArrayList<byte[]>(aggregated_tag_uids);
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
                                  downsampler, query_start, query_end,
                                  rate, rate_options, is_rollup);
  }

  /**
   * Finds the {@code i}th data point of this group in {@code O(n)}.
   * Where {@code n} is the number of data points in this group.
   */
  private DataPoint getDataPoint(int i) {
    if (i < 0) {
      throw new IndexOutOfBoundsException("negative index: " + i);
    }
    final int saved_i = i;
    final SeekableView it = iterator();
    DataPoint dp = null;
    while (it.hasNext() && i >= 0) {
      dp = it.next();
      i--;
    }
    if (i != -1 || dp == null) {
      throw new IndexOutOfBoundsException("index " + saved_i
          + " too large (it's >= " + size() + ") for " + this);
    }
    return dp;
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
    return "SpanGroup(" + toStringSharedAttributes()
      + ", spans=" + spans
      + ')';
  }

  private String toStringSharedAttributes() {
    return "start_time=" + start_time
      + ", end_time=" + end_time
      + ", tags=" + tags
      + ", aggregated_tags=" + aggregated_tags
      + ", rate=" + rate
      + ", aggregator=" + aggregator
      + ", downsampler=" + downsampler
      + ", query_start=" + query_start
      + ", query_end" + query_end
      + ')';
  }

  public int getQueryIndex() {
    return query_index;
  }

  /**
   * Resolves the set of tag keys to their string names.
   * @param tagks The set of unique tag names
   * @return a deferred to wait on for all of the tag keys to be resolved. The
   * result should be null.
   */
  private Deferred<List<String>> resolveAggTags(final Set<byte[]> tagks) {
    if (aggregated_tags != null) {
      return Deferred.fromResult(null);
    }
    aggregated_tags = new ArrayList<String>(tagks.size());
    
    final List<Deferred<String>> names = 
        new ArrayList<Deferred<String>>(tagks.size());
    for (final byte[] tagk : tagks) {
      names.add(tsdb.tag_names.getNameAsync(tagk));
    }
    
    /** Adds the names to the aggregated_tags list */
    final class ResolveCB implements Callback<List<String>, ArrayList<String>> {
      @Override
      public List<String> call(final ArrayList<String> names) throws Exception {
        for (final String name : names) {
          aggregated_tags.add(name);
        }
        return aggregated_tags;
      }
    }
    
    return Deferred.group(names).addCallback(new ResolveCB());
  }
  
  /**
   * Resolves the tags to their names, loading them into {@link tags} after
   * initializing that map.
   * @param tag_uids The tag UIDs
   * @return A defeferred to wait on for resolution to complete, the result
   * should be null.
   */
  private Deferred<Map<String, String>> resolveTags(final ByteMap<byte[]> tag_uids) {
    if (tags != null) {
      return Deferred.fromResult(null);
    }
    tags = new HashMap<String, String>(tag_uids.size());
    
    final List<Deferred<Object>> deferreds = 
        new ArrayList<Deferred<Object>>(tag_uids.size());
    
    /** Dumps the pairs into the map in the correct order */
    final class PairCB implements Callback<Object, ArrayList<String>> {
      @Override
      public Object call(final ArrayList<String> pair) throws Exception {
        tags.put(pair.get(0), pair.get(1));
        return null;
      }
    }
    
    /** Callback executed once all of the pairs are resolved and stored in the map */
    final class GroupCB implements Callback<Map<String, String>, ArrayList<Object>> {
      @Override
      public Map<String, String> call(final ArrayList<Object> group) 
          throws Exception {
        return tags;
      }
    }
    
    for (Map.Entry<byte[], byte[]> tag_pair : tag_uids.entrySet()) {
      final List<Deferred<String>> resolve_pair = 
          new ArrayList<Deferred<String>>(2);
      resolve_pair.add(tsdb.tag_names.getNameAsync(tag_pair.getKey()));
      resolve_pair.add(tsdb.tag_values.getNameAsync(tag_pair.getValue()));
      deferreds.add(Deferred.groupInOrder(resolve_pair).addCallback(new PairCB()));
    }
    
    return Deferred.group(deferreds).addCallback(new GroupCB());
  }
}
