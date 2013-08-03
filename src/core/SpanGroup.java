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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Aggregators.Interpolation;
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
  private HashMap<String, String> tags;

  /**
   * The names of the tags that aren't shared by every single data point.
   * This is the symmetric difference between the tags of all the Spans
   * in this group.
   * @see #computeTags
   */
  private ArrayList<String> aggregated_tags;

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
  private final int sample_interval;

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
            final int interval, final Aggregator downsampler) {
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
            final int interval, final Aggregator downsampler) {
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
    final long start = (start_time & Const.SECOND_MASK) == 0 ? 
        start_time * 1000 : start_time;
    final long end = (end_time & Const.SECOND_MASK) == 0 ? 
        end_time * 1000 : end_time;
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
    }
  }

  /**
   * Computes the intersection set + symmetric difference of tags in all spans.
   * @param spans A collection of spans for which to find the common tags.
   * @return A (possibly empty) map of the tags common to all the spans given.
   */
  private Deferred<Object> computeTags() {
    if (spans.isEmpty()) {
      tags = new HashMap<String, String>(0);
      aggregated_tags = new ArrayList<String>(0);
      return Deferred.fromResult(null);
    }

    final Iterator<Span> it = spans.iterator();
    
    /**
     * This is the last callback that will determine what tags are aggregated in
     * the results.
     */
    class SpanTagsCB implements Callback<Object, ArrayList<Map<String, String>>> {
      public Object call(final ArrayList<Map<String, String>> lookups) 
        throws Exception {
        final HashSet<String> discarded_tags = new HashSet<String>(tags.size());
        for (Map<String, String> lookup : lookups) {
          final Iterator<Map.Entry<String, String>> i = tags.entrySet().iterator();
          while (i.hasNext()) {
            final Map.Entry<String, String> entry = i.next();
            final String name = entry.getKey();
            final String value = lookup.get(name);
            if (value == null || !value.equals(entry.getValue())) {
              i.remove();
              discarded_tags.add(name);
            }
          }
        }
        SpanGroup.this.aggregated_tags = new ArrayList<String>(discarded_tags);
        return null;
      }
    }
    
    /**
     * We have to wait for the first set of tags to be resolved so we can 
     * create a map with the proper size. Then we iterate through the rest of
     * the tags for the different spans and work on each set.
     */
    class FirstTagSetCB implements Callback<Object, Map<String, String>> {      
      public Object call(final Map<String, String> first_tags) throws Exception {
        tags = new HashMap<String, String>(first_tags);
        final ArrayList<Deferred<Map<String, String>>> deferreds = 
          new ArrayList<Deferred<Map<String, String>>>(tags.size());
        
        while (it.hasNext()) {
          deferreds.add(it.next().getTagsAsync());
        }
        
        return Deferred.groupInOrder(deferreds).addCallback(new SpanTagsCB());
      }
    }

    return it.next().getTagsAsync().addCallback(new FirstTagSetCB());
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
      final Map<String, String> local_tags = tags;
      return Deferred.fromResult(local_tags);
    }
    
    class ComputeCB implements Callback<Map<String, String>, Object> {
      public Map<String, String> call(final Object obj) {
        return tags;
      }
    }
    
    return computeTags().addCallback(new ComputeCB());
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
      final List<String> agg_tags = aggregated_tags;
      return Deferred.fromResult(agg_tags);
    }
    
    class ComputeCB implements Callback<List<String>, Object> {
      public List<String> call(final Object obj) {
        return aggregated_tags;
      }
    }
    
    return computeTags().addCallback(new ComputeCB());
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
    ArrayList<Annotation> annotations = new ArrayList<Annotation>();
    for (Span sp : spans) {
      if (sp.getAnnotations().size() > 0) {
        annotations.addAll(sp.getAnnotations());
      }
    }
    
    if (annotations.size() > 0) {
      return annotations;
    }
    return null;
  }
  
  public int size() {
    // TODO(tsuna): There is a way of doing this way more efficiently by
    // inspecting the Spans and counting only data points that fall in
    // our time range.
    final SGIterator it = new SGIterator(aggregator.interpolationMethod());
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
    return new SGIterator(aggregator.interpolationMethod());
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
    final SGIterator it = new SGIterator(aggregator.interpolationMethod());
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

  /**
   * Iterator that does aggregation and linear interpolation (lerp).
   * <p>
   * This where the real business of {@link SpanGroup} is.  This iterator
   * provides a merged, aggregated view of multiple {@link Span}s.  The data
   * points in all the Spans are returned in chronological order.  Each time
   * we return a data point from a span, we aggregate it with the current
   * value from all the other Spans.  If other Spans don't have a value at
   * that specific timestamp, we do a linear interpolation in order to
   * estimate what the value of that Span should be at that time.
   * <p>
   * All this merging, linear interpolation and aggregation happens in
   * {@code O(1)} space and {@code O(N)} time.  All we need is to keep an
   * iterator on each Span, and {@code 4*k} {@code long}s in memory, where
   * {@code k} is the number of Spans in the group.  When computing a rate,
   * we need an extra {@code 2*k} {@code long}s in memory (see below).
   * <p>
   * In order to do linear interpolation, we need to know two data points:
   * the current one and the next one.  So for each Span in the group, we need
   * 4 longs: the current value, the current timestamp, the next value and the
   * next timestamp.  We maintain two arrays for timestamps and values.  Those
   * arrays have {@code 2 * iterators.length} elements.  The first half
   * contains the current values and second half the next values.  When a Span
   * gets used, its next data point becomes the current one (so its value and
   * timestamp are moved from the 2nd half of their respective array to the
   * first half) and the new-next data point is fetched from the underlying
   * iterator of that Span.
   * <p>
   * Here is an example when the SpanGroup contains 2 Spans:
   * <pre>              current    |     next
   *               +-------+-------+-------+-------+
   *   timestamps: |  T1   |  T2   |  T3   |  T4   |
   *               +-------+-------+-------+-------+
   *                    current    |     next
   *               +-------+-------+-------+-------+
   *       values: |  V1   |  V2   |  V3   |  V4   |
   *               +-------+-------+-------+-------+
   *                               |
   *   current: 0
   *   pos: 0
   *   iterators: [ it0, it1 ]
   * </pre>
   * Since {@code current == 0}, the current data point has the value V1
   * and time T1.  Let's note that (V1, T1).  Now this group has 2 Spans,
   * which means we're trying to aggregate 2 different series (same metric ID
   * but different tags).  So The next value that this iterator returns needs
   * to be a combination of V1 and V2 (assuming that T2 is less than T1).
   * If our aggregation function is "sum", we sort of want to sum up V1 and
   * V2.  But those two data points may not necessarily be at the same time.
   * T2 can be less than or equal to T1.  If T2 is greater than T1, we ignore
   * V2 and return just V1, since we haven't reached the time yet where V2
   * exist, so it's essentially as if it wasn't there.
   * Say T2 is less than T1.  Summing up V1 and V2 doesn't make sense, since
   * they represent two measurements made at different times.  So instead,
   * we need to find what the value V2 would have been, had it been measured
   * at time T1 instead of T2.  We do this using linear interpolation between
   * the data point (V2, T2) and the following one for that series, (V4, T4).
   * The result is thus the sum of V1 and the interpolated value between V2
   * and V4.
   * <p>
   * Now let's move onto the next data point.  Assuming that T3 is less than
   * T4, it means we need to advance to the next point on the 1st series.  To
   * do this we use the iterator it0 to get the next data point for that
   * series and we end up with the following state:
   * <pre>              current    |     next
   *               +-------+-------+-------+-------+
   *   timestamps: |  T3   |  T2   |  T5   |  T4   |
   *               +-------+-------+-------+-------+
   *                    current    |     next
   *               +-------+-------+-------+-------+
   *       values: |  V3   |  V2   |  V5   |  V4   |
   *               +-------+-------+-------+-------+
   *                               |
   *   current: 0
   *   pos: 0
   *   iterators: [ it0, it1 ]
   * </pre>
   * Then all you need is to "rinse and repeat".
   * <p>
   * More details: Since each value above can be either an integer or a
   * floating point, we have to keep track of the type of each value.  Values
   * are always stored in a {@code long}.  When a value is a floating point
   * value, the bits of the longs just need to be interpreted to get back the
   * floating point value.  The way we keep track of the type is by using the
   * most significant bit of the timestamp (to avoid an extra array).  This is
   * fine since our timestamps only really use 32 of the 64 bits of the long
   * in which they're stored.  When there is no "current" value (1st half of
   * the arrays depicted above), the timestamp will be set to 0.  When there
   * is no "next" value (2nd half of the arrays), the timestamp will be set
   * to a special, really large value (too large to be a valid timestamp).
   * <p>
   * Now when computing a rate, things are a little bit different.  This is
   * because a rate, by definition, involves 2 values.  In addition to the
   * 2 values, we need a 3rd one to "look ahead" and find when this time
   * series reaches its last value.   So for a rate, we'd have this:
   * <pre>              current    |      next     |     prev
   *               +-------+-------+-------+-------+-------+-------+
   *   timestamps: |  T3   |  T4   |  T5   |  T6   |  T1   |  T2   |
   *               +-------+-------+-------+-------+-------+-------+
   *                    current    |      next     |     prev
   *               +-------+-------+-------+-------+-------+-------+
   *       values: |  V3   |  V4   |  V5   |  V6   |  V1   |  V2   |
   *               +-------+-------+-------+-------+-------+-------+
   *                               |               |
   *   current: 0
   *   pos: 0
   *   iterators: [ it0, it1 ]
   * </pre>
   * Notice we just extend the table a little bit to be able to save one extra
   * value per time series.  When returning a value, we use "prev" and
   * "current" to compute the rate.  Once a value has been used, instead of
   * throwing it away like we do when rates aren't involved, we "migrate" it
   * to the 3rd part of the array ("prev") so we can use it for the next rate.
   */
  private final class SGIterator
    implements SeekableView, DataPoint,
               Aggregator.Longs, Aggregator.Doubles {

    /** Extra bit we set on the timestamp of floating point values. */
    private static final long FLAG_FLOAT = 0x8000000000000000L;

    /** Mask to use in order to get rid of the flag above.
     * This value also conveniently represents the largest timestamp we can
     * possibly store, provided that the most significant bit is reserved by
     * FLAG_FLOAT.
     */
    private static final long TIME_MASK  = 0x7FFFFFFFFFFFFFFFL;

    /** Interpolation method to use when aggregating time series */
    private final Interpolation method;
    
    /**
     * Where we are in each {@link Span} in the group.
     * The iterators in this array always points to 2 values ahead of the
     * current value, as we pre-load the current and the next values into the
     * {@link #timestamps} and {@link #values} member.
     * Once we reach the end of a Span, we'll null out its iterator from this
     * array.
     */
    private final SeekableView[] iterators;

    /**
     * The current and previous timestamps for the data points being used.
     * <p>
     * Are we computing a rate?
     * <ul>
     * <li>No: for {@code iterators[i]} the timestamp of the current data
     *     point is {@code timestamps[i]} and the timestamp of the next data
     *     point is {@code timestamps[iterators.length + i]}.</li>
     * <li>Yes: In addition to the above, the previous data point is saved
     *     in {@code timestamps[iterators.length * 2 + i]}.
     * </li></ul>
     * <p>
     * Each timestamp can have the {@code FLAG_FLOAT} applied so it's important
     * to use the {@code TIME_MASK} when getting the actual timestamp value
     * out of it.
     * There are two special values for timestamps:
     * <ul>
     * <li>{@code 0} when in the first half of the array: this iterator has
     * run out of data points and must not be used anymore.</li>
     * <li>{@code TIME_MASK} when in the second half of the array: this
     * iterator has reached its last data point and must not be used for
     * linear interpolation anymore.</li>
     * </ul>
     */
    private final long[] timestamps; // 32 bit unsigned + flag

    /**
     * The current and next values for the data points being used.
     * This array works exactly in the same fashion as the 'timestamps' array.
     * This array is also used to store floating point values, in which case
     * their binary representation just happens to be stored in a {@code long}.
     */
    private final long[] values;

    /** The index in {@link #iterators} of the current Span being used. */
    private int current;

    /** The index in {@link #values} of the current value being aggregated. */
    private int pos;

    /** Creates a new iterator for this {@link SpanGroup}. */
    public SGIterator(final Interpolation method) {
      this.method = method;
      final int size = spans.size();
      iterators = new SeekableView[size];
      timestamps = new long[size * (rate ? 3 : 2)];
      values = new long[size * (rate ? 3 : 2)];
      // Initialize every Iterator, fetch their first values that fall
      // within our time range.
      for (int i = 0; i < size; i++) {
        final SeekableView it =
          (downsampler == null
           ? spans.get(i).spanIterator()
           : spans.get(i).downsampler(sample_interval, downsampler));
        iterators[i] = it;
        it.seek(start_time);
        final DataPoint dp;
        try {
          dp = it.next();
        } catch (NoSuchElementException e) {
          throw new AssertionError("Span #" + i + " is empty! span="
                                   + spans.get(i));
        }
        //LOG.debug("Creating iterator #" + i);
        if (dp.timestamp() >= start_time) {
          //LOG.debug("First DP in range for #" + i + ": "
          //          + dp.timestamp() + " >= " + start_time);
          putDataPoint(size + i, dp);
        } else {
          //LOG.debug("No DP in range for #" + i + ": "
          //          + dp.timestamp() + " < " + start_time);
          endReached(i);
          continue;
        }
        if (rate) {  // Need two values to compute a rate.  Load one more.
          if (it.hasNext()) {
            moveToNext(i);
          } else {
            endReached(i);
          }
        }
      }
    }

    /**
     * Indicates that an iterator in {@link #iterators} has reached the end.
     * @param i The index in {@link #iterators} of the iterator.
     */
    private void endReached(final int i) {
      //LOG.debug("No more DP for #" + i);
      timestamps[iterators.length + i] = TIME_MASK;
      iterators[i] = null;  // We won't use it anymore, so free() it.
    }

    /**
     * Puts the next data point of an iterator in the internal buffer.
     * @param i The index in {@link #iterators} of the iterator.
     * @param dp The last data point returned by that iterator.
     */
    private void putDataPoint(final int i, final DataPoint dp) {
      timestamps[i] = dp.timestamp();
      if (dp.isInteger()) {
        //LOG.debug("Putting #" + i + " (long) " + dp.longValue()
        //          + " @ time " + dp.timestamp());
        values[i] = dp.longValue();
      } else {
        //LOG.debug("Putting #" + i + " (double) " + dp.doubleValue()
        //          + " @ time " + dp.timestamp());
        values[i] = Double.doubleToRawLongBits(dp.doubleValue());
        timestamps[i] |= FLAG_FLOAT;
      }
    }

    // ------------------ //
    // Iterator interface //
    // ------------------ //

    public boolean hasNext() {
      final int size = iterators.length;
      for (int i = 0; i < size; i++) {
        // As long as any of the iterators has a data point with a timestamp
        // that falls within our interval, we know we have at least one next.
        if ((timestamps[size + i] & TIME_MASK) <= end_time) {
          //LOG.debug("hasNext #" + (size + i));
          return true;
        }
      }
      //LOG.debug("No hasNext (return false)");
      return false;
    }

    public DataPoint next() {
      final int size = iterators.length;
      long min_ts = Long.MAX_VALUE;

      // In case we reached the end of one or more Spans, we need to make sure
      // we mark them as such by zeroing their current timestamp.  There may
      // be multiple Spans that reached their end at once, so check them all.
      for (int i = current; i < size; i++) {
        if (timestamps[i + size] == TIME_MASK) {
          //LOG.debug("Expiring last DP for #" + current);
          timestamps[i] = 0;
        }
      }

      // Now we need to find which Span we'll consume next.  We'll pick the
      // one that has the data point with the smallest timestamp since we want to
      // return them in chronological order.
      current = -1;
      // If there's more than one Span with the same smallest timestamp, we'll
      // set this to true so we can fetch the next data point in all of them at
      // the same time.
      boolean multiple = false;
      for (int i = 0; i < size; i++) {
        final long timestamp = timestamps[size + i] & TIME_MASK;
        if (timestamp <= end_time) {
          if (timestamp < min_ts) {
            min_ts = timestamp;
            current = i;
            // We just found a new minimum so right now we can't possibly have
            // multiple Spans with the same minimum.
            multiple = false;
          } else if (timestamp == min_ts) {
            multiple = true;
          }
        }
      }
      if (current < 0) {
        throw new NoSuchElementException("no more elements");
      }
      moveToNext(current);
      if (multiple) {
        //LOG.debug("Moving multiple DPs at time " + min_ts);
        // We know we saw at least one other data point with the same minimum
        // timestamp after `current', so let's move those ones too.
        for (int i = current + 1; i < size; i++) {
          final long timestamp = timestamps[size + i] & TIME_MASK;
          if (timestamp == min_ts) {
            moveToNext(i);
          }
        }
      }

      return this;
    }

    /**
     * Makes iterator number {@code i} move forward to the next data point.
     * @param i The index in {@link #iterators} of the iterator.
     */
    private void moveToNext(final int i) {
      final int size = iterators.length;
      final int next = iterators.length + i;
      if (rate) {  // move "current" in "prev".
        timestamps[next + size] = timestamps[i];
        values[next + size] = values[i];
        //LOG.debug("Saving #" + i + " -> #" + (next + size)
        //          + ((timestamps[i] & FLAG_FLOAT) == FLAG_FLOAT
        //             ? " float " + Double.longBitsToDouble(values[i])
        //             : " long " + values[i])
        //          + " @ time " + (timestamps[i] & TIME_MASK));
      }
      timestamps[i] = timestamps[next];
      values[i] = values[next];
      //LOG.debug("Moving #" + next + " -> #" + i
      //          + ((timestamps[i] & FLAG_FLOAT) == FLAG_FLOAT
      //             ? " float " + Double.longBitsToDouble(values[i])
      //             : " long " + values[i])
      //          + " @ time " + (timestamps[i] & TIME_MASK));
      final SeekableView it = iterators[i];
      if (it.hasNext()) {
        putDataPoint(next, it.next());
      } else {
        endReached(i);
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    // ---------------------- //
    // SeekableView interface //
    // ---------------------- //

    public void seek(final long timestamp) {
      for (final SeekableView it : iterators) {
        it.seek(timestamp);
      }
    }

    // ------------------- //
    // DataPoint interface //
    // ------------------- //

    public long timestamp() {
      return timestamps[current] & TIME_MASK;
    }

    public boolean isInteger() {
      if (rate) {
        // An rate can never be precisely represented without floating point.
        return false;
      }
      // If at least one of the values we're going to aggregate or interpolate
      // with is a float, we have to convert everything to a float.
      for (int i = timestamps.length - 1; i >= 0; i--) {
        if ((timestamps[i] & FLAG_FLOAT) == FLAG_FLOAT) {
          return false;
        }
      }
      return true;
    }

    public long longValue() {
      if (isInteger()) {
        pos = -1;
        return aggregator.runLong(this);
      }
      throw new ClassCastException("current value is a double: " + this);
    }

    public double doubleValue() {
      if (!isInteger()) {
        pos = -1;
        final double value = aggregator.runDouble(this);
        //LOG.debug("aggregator returned " + value);
        if (value != value || Double.isInfinite(value)) {
          throw new IllegalStateException("Got NaN or Infinity: "
             + value + " in this " + this);
        }
        return value;
      }
      throw new ClassCastException("current value is a long: " + this);
    }

    public double toDouble() {
      return isInteger() ? longValue() : doubleValue();
    }

    // -------------------------- //
    // Aggregator.Longs interface //
    // -------------------------- //

    public boolean hasNextValue() {
      return hasNextValue(false);
    }

    /**
     * Returns whether or not there are more values to aggregate.
     * @param update_pos Whether or not to also move the internal pointer
     * {@link #pos} to the index of the next value to aggregate.
     * @return true if there are more values to aggregate, false otherwise.
     */
    private boolean hasNextValue(boolean update_pos) {
      final int size = iterators.length;
      for (int i = pos + 1; i < size; i++) {
        if (timestamps[i] != 0) {
          //LOG.debug("hasNextValue -> true #" + i);
          if (update_pos) {
            pos = i;
          }
          return true;
        }
      }
      //LOG.debug("hasNextValue -> false (ran out)");
      return false;
    }

    public long nextLongValue() {
      if (hasNextValue(true)) {
        final long y0 = values[pos];
        if (rate) {
          throw new AssertionError("Should not be here, impossible! " + this);
        }
        if (current == pos) {
          return y0;
        }
        final long x = timestamps[current] & TIME_MASK;
        final long x0 = timestamps[pos] & TIME_MASK;
        if (x == x0) {
          return y0;
        }
        final long y1 = values[pos + iterators.length];
        final long x1 = timestamps[pos + iterators.length] & TIME_MASK;
        if (x == x1) {
          return y1;
        }
        if ((x1 & Const.MILLISECOND_MASK) != 0) {
          throw new AssertionError("x1=" + x1 + " in " + this);
        }
        final long r;
        switch (method) {
          case LERP: 
            r = y0 + (x - x0) * (y1 - y0) / (x1 - x0);
            //LOG.debug("Lerping to time " + x + ": " + y0 + " @ " + x0
            //          + " -> " + y1 + " @ " + x1 + " => " + r);
            break;
          case ZIM:
            r = 0;
            break;
          case MAX:
            r = Long.MAX_VALUE;
            break;
          case MIN:
            r = Long.MIN_VALUE;
            break;
          default:
            throw new IllegalDataException("Invalid interploation somehow??");
        }
        return r;
      }
      throw new NoSuchElementException("no more longs in " + this);
    }

    // ---------------------------- //
    // Aggregator.Doubles interface //
    // ---------------------------- //

    public double nextDoubleValue() {
      if (hasNextValue(true)) {
        final double y0 = ((timestamps[pos] & FLAG_FLOAT) == FLAG_FLOAT
                           ? Double.longBitsToDouble(values[pos])
                           : values[pos]);
        if (rate) {
          final long x0 = timestamps[pos] & TIME_MASK;
          final int prev = pos + iterators.length * 2;
          final double y1 = ((timestamps[prev] & FLAG_FLOAT) == FLAG_FLOAT
                             ? Double.longBitsToDouble(values[prev])
                             : values[prev]);
          final long x1 = timestamps[prev] & TIME_MASK;
          assert x0 > x1: ("Next timestamp (" + x0 + ") is supposed to be "
            + " strictly greater than the previous one (" + x1 + "), but it's"
            + " not.  this=" + this);
          
          // we need to account for LONGs that are being converted to a double
          // to do so, we can see if it's greater than the most precise integer
          // a double can store. Then we calc the diff on the Longs before
          // casting to a double. 
          // TODO(cl) If the diff between data points is > 2^53 we're still in 
          // trouble though that's less likely than giant integer counters.
          final boolean double_overflow = 
              (timestamps[pos] & FLAG_FLOAT) != FLAG_FLOAT && 
              (timestamps[prev] & FLAG_FLOAT) != FLAG_FLOAT &&
              ((values[prev] & Const.MAX_INT_IN_DOUBLE) != 0 || 
                  (values[pos] & Const.MAX_INT_IN_DOUBLE) != 0);
          //LOG.debug("Double overflow detected");
          
          final double difference;
          if (double_overflow) {
            final long diff = values[pos] - values[prev];
            difference = (double)(diff);
          } else {
            difference = y0 - y1;
          }
          //LOG.debug("Difference is: " + difference);
          
          // If we have a counter rate of change calculation, y0 and y1
          // have values such that the rate would be < 0 then calculate the
          // new rate value assuming a roll over
          if (rate_options.isCounter() && difference < 0) {
            final double r;
            if (double_overflow) {
              long diff = rate_options.getCounterMax() - values[prev];
              diff += values[pos];
              // TODO - for backwards compatibility we'll convert the ms to seconds
              // but in the future we should add a ratems flag that will calculate
              // the rate as is.
              r = (double)diff / ((double)(x0 - x1) / (double)1000);
            } else {
              // TODO - for backwards compatibility we'll convert the ms to seconds
              // but in the future we should add a ratems flag that will calculate
              // the rate as is.
              r = (rate_options.getCounterMax() - y1 + y0) / 
                                        ((double)(x0 - x1) / (double)1000);
            }
            if (rate_options.getResetValue() > RateOptions.DEFAULT_RESET_VALUE
                && r > rate_options.getResetValue()) {
              return 0.0;
            }
            //LOG.debug("Rolled Rate for " + y1 + " @ " + x1
            // + " -> " + y0 + " @ " + x0 + " => " + r);
            return r;
          }
          
          // TODO - for backwards compatibility we'll convert the ms to seconds
          // but in the future we should add a ratems flag that will calculate
          // the rate as is.
          final double r = difference / ((double)(x0 - x1) / (double)1000);
          //LOG.debug("Rate for " + y1 + " @ " + x1
          //          + " -> " + y0 + " @ " + x0 + " => " + r);
          return r;
        }
        if (current == pos) {
          //LOG.debug("Exact match, no lerp needed");
          return y0;
        }
        final long x = timestamps[current] & TIME_MASK;
        final long x0 = timestamps[pos] & TIME_MASK;
        if (x == x0) {
          //LOG.debug("No lerp needed x == x0 (" + x + " == "+x0+") => " + y0);
          return y0;
        }
        final int next = pos + iterators.length;
        final double y1 = ((timestamps[next] & FLAG_FLOAT) == FLAG_FLOAT
                           ? Double.longBitsToDouble(values[next])
                           : values[next]);
        final long x1 = timestamps[next] & TIME_MASK;
        if (x == x1) {
          //LOG.debug("No lerp needed x == x1 (" + x + " == "+x1+") => " + y1);
          return y1;
        }
        if ((x1 & Const.MILLISECOND_MASK) != 0) {
          throw new AssertionError("x1=" + x1 + " in " + this);
        }
        final double r;
        switch (method) {
        case LERP: 
          r = y0 + (x - x0) * (y1 - y0) / (x1 - x0);
          //LOG.debug("Lerping to time " + x + ": " + y0 + " @ " + x0
          //          + " -> " + y1 + " @ " + x1 + " => " + r);
          break;
        case ZIM:
          r = 0;
          break;
        case MAX:
          r = Double.MAX_VALUE;
          break;
        case MIN:
          r = Double.MIN_VALUE;
          break;
        default:
          throw new IllegalDataException("Invalid interploation somehow??");
      }
        return r;
      }
      throw new NoSuchElementException("no more doubles in " + this);
    }

    public String toString() {
      return "SpanGroup.Iterator(timestamps=" + Arrays.toString(timestamps)
        + ", values=" + Arrays.toString(values)
        + ", current=" + current
        + ", pos=" + pos
        + ", (SpanGroup: " + toStringSharedAttributes()
        + "), iterators=" + Arrays.toString(iterators)
        + ')';
    }

  }

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
      + ", sample_interval=" + sample_interval
      + ')';
  }

}
