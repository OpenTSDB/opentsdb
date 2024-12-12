// This file is part of OpenTSDB.
// Copyright (C) 2012-2021  The OpenTSDB Authors.
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

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.meta.Annotation;
import org.hbase.async.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitRollupSpanGroup extends AbstractSpanGroup {
    private final List<SpanGroup> spanGroups = new ArrayList<SpanGroup>();

    public SplitRollupSpanGroup(SpanGroup... groups) {
        for (SpanGroup group : groups) {
            if (group != null) {
                spanGroups.add(group);
            }
        }

        if (spanGroups.isEmpty()) {
            throw new IllegalArgumentException("At least one SpanGroup must be non-null");
        }
    }

    /**
     * Returns the name of the series.
     */
    @Override
    public String metricName() {
        return spanGroups.get(0).metricName();
    }

    /**
     * Returns the name of the series.
     *
     * @since 1.2
     */
    @Override
    public Deferred<String> metricNameAsync() {
        return spanGroups.get(0).metricNameAsync();
    }

    /**
     * @return the metric UID
     * @since 2.3
     */
    @Override
    public byte[] metricUID() {
        return spanGroups.get(0).metricUID();
    }

    /**
     * Returns the tags associated with these data points.
     *
     * @return A non-{@code null} map of tag names (keys), tag values (values).
     */
    @Override
    public Map<String, String> getTags() {
        Map<String, String> tags = new HashMap<String, String>();

        for (SpanGroup group : spanGroups) {
            tags.putAll(group.getTags());
        }

        return tags;
    }

    /**
     * Returns the tags associated with these data points.
     *
     * @return A non-{@code null} map of tag names (keys), tag values (values).
     * @since 1.2
     */
    @Override
    public Deferred<Map<String, String>> getTagsAsync() {
        class GetTagsCB implements Callback<Map<String, String>, ArrayList<Map<String, String>>> {
            @Override
            public Map<String, String> call(ArrayList<Map<String, String>> resolvedTags) throws Exception {
                Map<String, String> tags = new HashMap<String, String>();
                for (Map<String, String> groupTags : resolvedTags) {
                    tags.putAll(groupTags);
                }
                return tags;
            }
        }

        List<Deferred<Map<String, String>>> deferreds = new ArrayList<Deferred<Map<String, String>>>(spanGroups.size());

        for (SpanGroup group : spanGroups) {
            deferreds.add(group.getTagsAsync());
        }

        return Deferred.groupInOrder(deferreds).addCallback(new GetTagsCB());
    }

    /**
     * Returns a map of tag pairs as UIDs.
     * When used on a span or row, it returns the tag set. When used on a span
     * group it will return only the tag pairs that are common across all
     * time series in the group.
     *
     * @return A potentially empty map of tagk to tagv pairs as UIDs
     * @since 2.2
     */
    @Override
    public Bytes.ByteMap<byte[]> getTagUids() {
        Bytes.ByteMap<byte[]> tagUids = new Bytes.ByteMap<byte[]>();

        for (SpanGroup group : spanGroups) {
            tagUids.putAll(group.getTagUids());
        }

        return tagUids;
    }

    /**
     * Returns the tags associated with some but not all of the data points.
     * <p>
     * When this instance represents the aggregation of multiple time series
     * (same metric but different tags), {@link #getTags} returns the tags that
     * are common to all data points (intersection set) whereas this method
     * returns all the tags names that are not common to all data points (union
     * set minus the intersection set, also called the symmetric difference).
     * <p>
     * If this instance does not represent an aggregation of multiple time
     * series, the list returned is empty.
     *
     * @return A non-{@code null} list of tag names.
     */
    @Override
    public List<String> getAggregatedTags() {
        List<String> aggregatedTags = new ArrayList<String>();

        for (SpanGroup group : spanGroups) {
            aggregatedTags.addAll(group.getAggregatedTags());
        }

        return aggregatedTags;
    }

    /**
     * Returns the tags associated with some but not all of the data points.
     * <p>
     * When this instance represents the aggregation of multiple time series
     * (same metric but different tags), {@link #getTags} returns the tags that
     * are common to all data points (intersection set) whereas this method
     * returns all the tags names that are not common to all data points (union
     * set minus the intersection set, also called the symmetric difference).
     * <p>
     * If this instance does not represent an aggregation of multiple time
     * series, the list returned is empty.
     *
     * @return A non-{@code null} list of tag names.
     * @since 1.2
     */
    @Override
    public Deferred<List<String>> getAggregatedTagsAsync() {
        class GetAggregatedTagsCB implements Callback<List<String>, ArrayList<List<String>>> {
            @Override
            public List<String> call(ArrayList<List<String>> resolvedTags) throws Exception {
                List<String> aggregatedTags = new ArrayList<String>();
                for (List<String> groupTags : resolvedTags) {
                    aggregatedTags.addAll(groupTags);
                }
                return aggregatedTags;
            }
        }

        List<Deferred<List<String>>> deferreds =
                new ArrayList<Deferred<List<String>>>(spanGroups.size());
        for (SpanGroup group : spanGroups) {
            deferreds.add(group.getAggregatedTagsAsync());
        }

        return Deferred.groupInOrder(deferreds).addCallback(new GetAggregatedTagsCB());
    }

    /**
     * Returns the tagk UIDs associated with some but not all of the data points.
     *
     * @return a non-{@code null} list of tagk UIDs.
     * @since 2.3
     */
    @Override
    public List<byte[]> getAggregatedTagUids() {
        List<byte[]> aggTagUids = new ArrayList<byte[]>();

        for (SpanGroup group : spanGroups) {
            aggTagUids.addAll(group.getAggregatedTagUids());
        }

        return aggTagUids;
    }

    /**
     * Returns a list of unique TSUIDs contained in the results
     *
     * @return an empty list if there were no results, otherwise a list of TSUIDs
     */
    @Override
    public List<String> getTSUIDs() {
        List<String> tsuids = new ArrayList<String>();

        for (SpanGroup group : spanGroups) {
            tsuids.addAll(group.getTSUIDs());
        }

        return tsuids;
    }

    /**
     * Compiles the annotations for each span into a new array list
     *
     * @return Null if none of the spans had any annotations, a list if one or
     * more were found
     */
    @Override
    public List<Annotation> getAnnotations() {
        List<Annotation> annotations = new ArrayList<Annotation>();

        for (SpanGroup group : spanGroups) {
            List<Annotation> groupAnnotations = group.getAnnotations();
            if (groupAnnotations != null) {
                annotations.addAll(group.getAnnotations());
            }
        }

        return annotations;
    }

    /**
     * Returns the number of data points.
     * <p>
     * This method must be implemented in {@code O(1)} or {@code O(n)}
     * where <code>n = {@link #aggregatedSize} &gt; 0</code>.
     *
     * @return A positive integer.
     */
    @Override
    public int size() {
        int size = 0;
        for (SpanGroup group : spanGroups) {
            size += group.size();
        }
        return size;
    }

    /**
     * Returns the number of data points aggregated in this instance.
     * <p>
     * When this instance represents the aggregation of multiple time series
     * (same metric but different tags), {@link #size} returns the number of data
     * points after aggregation, whereas this method returns the number of data
     * points before aggregation.
     * <p>
     * If this instance does not represent an aggregation of multiple time
     * series, then 0 is returned.
     *
     * @return A positive integer.
     */
    @Override
    public int aggregatedSize() {
        int aggregatedSize = 0;
        for (SpanGroup group : spanGroups) {
            aggregatedSize += group.aggregatedSize();
        }
        return aggregatedSize;
    }

    /**
     * Returns a <em>zero-copy view</em> to go through {@code size()} data points.
     * <p>
     * The iterator returned must return each {@link DataPoint} in {@code O(1)}.
     * <b>The {@link DataPoint} returned must not be stored</b> and gets
     * invalidated as soon as {@code next} is called on the iterator.  If you
     * want to store individual data points, you need to copy the timestamp
     * and value out of each {@link DataPoint} into your own data structures.
     */
    @Override
    public SeekableView iterator() {
        List<SeekableView> iterators = new ArrayList<SeekableView>();
        for (SpanGroup group : spanGroups) {
            iterators.add(group.iterator());
        }
        return new SeekableViewChain(iterators);
    }

    /**
     * Returns the timestamp associated with the {@code i}th data point.
     * The first data point has index 0.
     * <p>
     * This method must be implemented in
     * <code>O({@link #aggregatedSize})</code> or better.
     * <p>
     * It is guaranteed that <pre>timestamp(i) &lt; timestamp(i+1)</pre>
     *
     * @param i
     * @return A strictly positive integer.
     * @throws IndexOutOfBoundsException if {@code i} is not in the range
     *                                   <code>[0, {@link #size} - 1]</code>
     */
    @Override
    public long timestamp(int i) {
        return getDataPoint(i).timestamp();
    }

    /**
     * Tells whether or not the {@code i}th value is of integer type.
     * The first data point has index 0.
     * <p>
     * This method must be implemented in
     * <code>O({@link #aggregatedSize})</code> or better.
     *
     * @param i
     * @return {@code true} if the {@code i}th value is of integer type,
     * {@code false} if it's of floating point type.
     * @throws IndexOutOfBoundsException if {@code i} is not in the range
     *                                   <code>[0, {@link #size} - 1]</code>
     */
    @Override
    public boolean isInteger(int i) {
        return getDataPoint(i).isInteger();
    }

    /**
     * Returns the value of the {@code i}th data point as a long.
     * The first data point has index 0.
     * <p>
     * This method must be implemented in
     * <code>O({@link #aggregatedSize})</code> or better.
     * Use {@link #iterator} to get successive {@code O(1)} accesses.
     *
     * @param i
     * @throws IndexOutOfBoundsException if {@code i} is not in the range
     *                                   <code>[0, {@link #size} - 1]</code>
     * @throws ClassCastException        if the
     *                                   <code>{@link #isInteger isInteger(i)} == false</code>.
     * @see #iterator
     */
    @Override
    public long longValue(int i) {
        return getDataPoint(i).longValue();
    }

    /**
     * Returns the value of the {@code i}th data point as a float.
     * The first data point has index 0.
     * <p>
     * This method must be implemented in
     * <code>O({@link #aggregatedSize})</code> or better.
     * Use {@link #iterator} to get successive {@code O(1)} accesses.
     *
     * @param i
     * @throws IndexOutOfBoundsException if {@code i} is not in the range
     *                                   <code>[0, {@link #size} - 1]</code>
     * @throws ClassCastException        if the
     *                                   <code>{@link #isInteger isInteger(i)} == true</code>.
     * @see #iterator
     */
    @Override
    public double doubleValue(int i) {
        return getDataPoint(i).doubleValue();
    }

    /**
     * Return the query index that maps this datapoints to the original TSSubQuery.
     *
     * @return index of the query in the TSQuery class
     * @throws UnsupportedOperationException if the implementing class can't map
     *                                       to a sub query.
     * @since 2.2
     */
    @Override
    public int getQueryIndex() {
        return spanGroups.get(0).getQueryIndex();
    }

    /**
     * Return whether these data points are the result of the percentile calculation
     * on the histogram data points. The client can call {@code getPercentile} to get
     * the percentile calculation parameter.
     *
     * @return true or false
     * @since 2.4
     */
    @Override
    public boolean isPercentile() {
        return spanGroups.get(0).isPercentile();
    }

    /**
     * Return the percentile calculation parameter. This interface and {@code isPercentile} are used
     * to convert {@code HistogramDataPoints} to {@code DataPoints}
     *
     * @return the percentile parameter
     * @since 2.4
     */
    @Override
    public float getPercentile() {
        return spanGroups.get(0).getPercentile();
    }

    /**
     * Returns the group the spans in here belong to.
     * <p>
     * Returns null if the NONE aggregator was requested in the query
     * Returns an empty array if there were no group bys and they're all in the same group
     * Returns the group otherwise
     *
     * @return The group
     */
    public byte[] group() {
        return spanGroups.get(0).group();
    }
}
