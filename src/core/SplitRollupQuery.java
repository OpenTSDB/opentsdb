// This file is part of OpenTSDB.
// Copyright (C) 2010-2021  The OpenTSDB Authors.
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
import net.opentsdb.rollup.RollupQuery;
import net.opentsdb.uid.NoSuchUniqueName;
import org.hbase.async.Bytes;
import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.HBaseException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class SplitRollupQuery extends AbstractQuery {

    private TSDB tsdb;

    private TsdbQuery rollupQuery;
    private TsdbQuery rawQuery;

    private Deferred<Object> rollupResolution;
    private Deferred<Object> rawResolution;

    SplitRollupQuery(final TSDB tsdb, TsdbQuery rollupQuery, Deferred<Object> rollupResolution) {
        if (rollupQuery == null) {
            throw new IllegalArgumentException("Rollup query cannot be null");
        }

        this.tsdb = tsdb;

        this.rollupQuery = rollupQuery;
        this.rollupResolution = rollupResolution;
    }

    /**
     * Returns the start time of the graph.
     *
     * @return A strictly positive integer.
     * @throws IllegalStateException if {@link #setStartTime(long)} was never
     *                               called on this instance before.
     */
    @Override
    public long getStartTime() {
        return rollupQuery != null ? rollupQuery.getStartTime() : rawQuery.getStartTime();
    }

    /**
     * Sets the start time of the graph. Converts the timestamp to milliseconds if necessary.
     *
     * @param timestamp The start time, all the data points returned will have a
     *                  timestamp greater than or equal to this one.
     * @throws IllegalArgumentException if timestamp is less than or equal to 0,
     *                                  or if it can't fit on 32 bits.
     * @throws IllegalArgumentException if
     *                                  {@code timestamp >= }{@link #getEndTime getEndTime}.
     */
    @Override
    public void setStartTime(long timestamp) {
        if ((timestamp & Const.SECOND_MASK) == 0) { timestamp *= 1000L; }

        if (rollupQuery == null) {
            rawQuery.setStartTime(timestamp);
            return;
        }

        if (rollupQuery.getEndTime() <= timestamp) {
            rollupQuery = null;
            rawQuery.setStartTime(timestamp);
            return;
        }

        rollupQuery.setStartTime(timestamp);
    }

    /**
     * Returns the end time of the graph.
     * <p>
     * If {@link #setEndTime} was never called before, this method will
     * automatically execute
     * {@link #setEndTime setEndTime}{@code (System.currentTimeMillis() / 1000)}
     * to set the end time.
     *
     * @return A strictly positive integer.
     */
    @Override
    public long getEndTime() {
        return rawQuery != null ? rawQuery.getEndTime() : rollupQuery.getEndTime();
    }

    /**
     * Sets the end time of the graph. Converts the timestamp to milliseconds if necessary.
     *
     * @param timestamp The end time, all the data points returned will have a
     *                  timestamp less than or equal to this one.
     * @throws IllegalArgumentException if timestamp is less than or equal to 0,
     *                                  or if it can't fit on 32 bits.
     * @throws IllegalArgumentException if
     *                                  {@code timestamp <= }{@link #getStartTime getStartTime}.
     */
    @Override
    public void setEndTime(long timestamp) {
        if ((timestamp & Const.SECOND_MASK) == 0) { timestamp *= 1000L; }

        rawQuery.setEndTime(timestamp);
    }

    /**
     * Returns whether or not the data queried will be deleted.
     *
     * @return A boolean
     * @since 2.4
     */
    @Override
    public boolean getDelete() {
        return rawQuery.getDelete();
    }

    /**
     * Sets whether or not the data queried will be deleted.
     *
     * @param delete True if data should be deleted, false otherwise.
     * @since 2.4
     */
    @Override
    public void setDelete(boolean delete) {
        if (rollupQuery != null) {
            rollupQuery.setDelete(delete);
        }
        rawQuery.setDelete(delete);
    }

    /**
     * Sets the time series to the query.
     *
     * @param metric       The metric to retrieve from the TSDB.
     * @param tags         The set of tags of interest.
     * @param function     The aggregation function to use.
     * @param rate         If true, the rate of the series will be used instead of the
     *                     actual values.
     * @param rate_options If included specifies additional options that are used
     *                     when calculating and graph rate values
     * @throws NoSuchUniqueName if the name of a metric, or a tag name/value
     *                          does not exist.
     * @since 2.4
     */
    @Override
    public void setTimeSeries(String metric,
                              Map<String, String> tags,
                              Aggregator function,
                              boolean rate,
                              RateOptions rate_options) throws NoSuchUniqueName {
        if (rollupQuery != null) {
            rollupQuery.setTimeSeries(metric, tags, function, rate, rate_options);
        }
        rawQuery.setTimeSeries(metric, tags, function, rate, rate_options);
    }

    /**
     * Sets the time series to the query.
     *
     * @param metric   The metric to retrieve from the TSDB.
     * @param tags     The set of tags of interest.
     * @param function The aggregation function to use.
     * @param rate     If true, the rate of the series will be used instead of the
     *                 actual values.
     * @throws NoSuchUniqueName if the name of a metric, or a tag name/value
     *                          does not exist.
     */
    @Override
    public void setTimeSeries(String metric, Map<String, String> tags, Aggregator function, boolean rate) throws NoSuchUniqueName {
        if (rollupQuery != null) {
            rollupQuery.setTimeSeries(metric, tags, function, rate);
        }
        rawQuery.setTimeSeries(metric, tags, function, rate);
    }

    /**
     * Sets up a query for the given timeseries UIDs. For now, all TSUIDs in the
     * group must share a common metric. This is to avoid issues where the scanner
     * may have to traverse the entire data table if one TSUID has a metric of
     * 000001 and another has a metric of FFFFFF. After modifying the query code
     * to run asynchronously and use different scanners, we can allow different
     * TSUIDs.
     * <b>Note:</b> This method will not check to determine if the TSUIDs are
     * valid, since that wastes time and we *assume* that the user provides TSUIDs
     * that are up to date.
     *
     * @param tsuids   A list of one or more TSUIDs to scan for
     * @param function The aggregation function to use on results
     * @param rate     Whether or not the results should be converted to a rate
     * @throws IllegalArgumentException if the tsuid list is null, empty or the
     *                                  TSUIDs do not share a common metric
     * @since 2.4
     */
    @Override
    public void setTimeSeries(List<String> tsuids, Aggregator function, boolean rate) {
        if (rollupQuery != null) {
            rollupQuery.setTimeSeries(tsuids, function, rate);
        }
        rawQuery.setTimeSeries(tsuids, function, rate);
    }

    /**
     * Sets up a query for the given timeseries UIDs. For now, all TSUIDs in the
     * group must share a common metric. This is to avoid issues where the scanner
     * may have to traverse the entire data table if one TSUID has a metric of
     * 000001 and another has a metric of FFFFFF. After modifying the query code
     * to run asynchronously and use different scanners, we can allow different
     * TSUIDs.
     * <b>Note:</b> This method will not check to determine if the TSUIDs are
     * valid, since that wastes time and we *assume* that the user provides TSUIDs
     * that are up to date.
     *
     * @param tsuids       A list of one or more TSUIDs to scan for
     * @param function     The aggregation function to use on results
     * @param rate         Whether or not the results should be converted to a rate
     * @param rate_options If included specifies additional options that are used
     *                     when calculating and graph rate values
     * @throws IllegalArgumentException if the tsuid list is null, empty or the
     *                                  TSUIDs do not share a common metric
     * @since 2.4
     */
    @Override
    public void setTimeSeries(List<String> tsuids, Aggregator function, boolean rate, RateOptions rate_options) {
        if (rollupQuery != null) {
            rollupQuery.setTimeSeries(tsuids, function, rate, rate_options);
        }
        rawQuery.setTimeSeries(tsuids, function, rate, rate_options);
    }

    /**
     * Prepares a query against HBase by setting up group bys and resolving
     * strings to UIDs asynchronously. This replaces calls to all of the setters
     * like the {@link setTimeSeries}, {@link setStartTime}, etc.
     * Make sure to wait on the deferred return before calling {@link runAsync}.
     *
     * @param query The main query to fetch the start and end time from
     * @param index The index of which sub query we're executing
     * @return A deferred to wait on for UID resolution. The result doesn't have
     * any meaning and can be discarded.
     * @throws IllegalArgumentException if the query was missing sub queries or
     *                                  the index was out of bounds.
     * @throws NoSuchUniqueName         if the name of a metric, or a tag name/value
     *                                  does not exist. (Bubbles up through the deferred)
     * @since 2.4
     */
    @Override
    public Deferred<Object> configureFromQuery(TSQuery query, int index) {
        return configureFromQuery(query, index, false);
    }

    @Override
    public Deferred<Object> configureFromQuery(TSQuery query, int index, boolean force_raw) {
        if (force_raw) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        if (!rollupQuery.needsSplitting()) {
            return rollupResolution;
        }

        rawQuery = new TsdbQuery(tsdb);
        rawResolution = rollupQuery.split(query, index, rawQuery);

        if (rollupQuery.getRollupQuery().getLastRollupTimestampSeconds() * 1000L < rollupQuery.getStartTime()) {
            // We're looking at a query that would normally hit a rollup table, but the table doesn't
            // have data guaranteed to be available for the requested time period or any part of it
            // (i.e. the last guaranteed rollup point is before the query actually starts)
            // So we won't bother running it.
            rollupQuery = null;
        }

        return Deferred.group(rollupResolution, rawResolution).addCallback(new GroupCallback());
    }

    /**
     * Downsamples the results by specifying a fixed interval between points.
     * <p>
     * Technically, downsampling means reducing the sampling interval.  Here
     * the idea is similar.  Instead of returning every single data point that
     * matched the query, we want one data point per fixed time interval.  The
     * way we get this one data point is by aggregating all the data points of
     * that interval together using an {@link Aggregator}.  This enables you
     * to compute things like the 5-minute average or 10 minute 99th percentile.
     *
     * @param interval    Number of seconds wanted between each data point.
     * @param downsampler Aggregation function to use to group data points
     */
    @Override
    public void downsample(long interval, Aggregator downsampler) {
        if (rollupQuery != null) {
            rollupQuery.downsample(interval, downsampler);
        }
        rawQuery.downsample(interval, downsampler);
    }

    /**
     * Sets an optional downsampling function on this query
     *
     * @param interval    The interval, in milliseconds to rollup data points
     * @param downsampler An aggregation function to use when rolling up data points
     * @param fill_policy Policy specifying whether to interpolate or to fill
     *                    missing intervals with special values.
     * @throws NullPointerException     if the aggregation function is null
     * @throws IllegalArgumentException if the interval is not greater than 0
     * @since 2.4
     */
    @Override
    public void downsample(long interval, Aggregator downsampler, FillPolicy fill_policy) {
        if (rollupQuery != null) {
            rollupQuery.downsample(interval, downsampler, fill_policy);
        }
        rawQuery.downsample(interval, downsampler, fill_policy);
    }

    /**
     * Executes the query asynchronously
     *
     * @return The data points matched by this query.
     * <p>
     * Each element in the non-{@code null} but possibly empty array returned
     * corresponds to one time series for which some data points have been
     * matched by the query.
     * @throws HBaseException if there was a problem communicating with HBase to
     *                        perform the search.
     * @since 1.2
     */
    @Override
    public Deferred<DataPoints[]> runAsync() throws HBaseException {
        Deferred<DataPoints[]> rollupResults = Deferred.fromResult(new DataPoints[0]);
        if (rollupQuery != null) {
            rollupResults = rollupQuery.runAsync();
        }
        Deferred<DataPoints[]> rawResults = rawQuery.runAsync();

        return Deferred.groupInOrder(Arrays.asList(rollupResults, rawResults)).addCallback(new RunCB());
    }

    /**
     * Runs this query asynchronously.
     *
     * @return The data points matched by this query and applied with percentile calculation
     * <p>
     * Each element in the non-{@code null} but possibly empty array returned
     * corresponds to one time series for which some data points have been
     * matched by the query.
     * @throws HBaseException        if there was a problem communicating with HBase to
     *                               perform the search.
     * @throws IllegalStateException if the query is not a histogram query
     */
    @Override
    public Deferred<DataPoints[]> runHistogramAsync() throws HBaseException {
        Deferred<DataPoints[]> rollupResults = Deferred.fromResult(new DataPoints[0]);
        if (rollupQuery != null) {
            rollupResults = rollupQuery.runHistogramAsync();
        }        Deferred<DataPoints[]> rawResults = rawQuery.runHistogramAsync();

        return Deferred.groupInOrder(Arrays.asList(rollupResults, rawResults)).addCallback(new RunCB());
    }

    /**
     * Returns an index for this sub-query in the original set of queries.
     *
     * @return A zero based index.
     * @since 2.4
     */
    @Override
    public int getQueryIdx() {
        return rawQuery.getQueryIdx();
    }

    /**
     * Check this is a histogram query or not
     *
     * @return
     */
    @Override
    public boolean isHistogramQuery() {
        return rawQuery.isHistogramQuery();
    }

    /**
     * Check this is a rollup query or not
     *
     * @return Whether or not this is a rollup query
     * @since 2.4
     */
    @Override
    public boolean isRollupQuery() {
        return rollupQuery != null && RollupQuery.isValidQuery(rollupQuery.getRollupQuery());
    }

    /**
     * @since 2.4
     */
    @Override
    public boolean needsSplitting() {
        // No further splitting supported
        return false;
    }

    /**
     * Set the percentile calculation parameters for this query if this is
     * a histogram query
     *
     * @param percentiles
     */
    @Override
    public void setPercentiles(List<Float> percentiles) {
        if (rollupQuery != null) {
            rollupQuery.setPercentiles(percentiles);
        }
        rawQuery.setPercentiles(percentiles);
    }

    private class RunCB implements Callback<DataPoints[], ArrayList<DataPoints[]>> {

        private ByteMap<SpanGroup> makeSpanGroupMap(DataPoints[] dataPointsArray) {
            ByteMap<SpanGroup> map = new ByteMap<SpanGroup>();

            for (DataPoints points : dataPointsArray) {
                if (!(points instanceof SpanGroup)) {
                    throw new IllegalArgumentException("Only SpanGroups implemented");
                }
                SpanGroup spanGroup = (SpanGroup) points;
                map.put(spanGroup.group(), spanGroup);
            }

            return map;
        }

        private DataPoints[] merge(DataPoints[] rollup, DataPoints[] raw) {
            ByteMap<SpanGroup> rollupResults = makeSpanGroupMap(rollup);
            ByteMap<SpanGroup> rawResults = makeSpanGroupMap(raw);

            TreeSet<byte[]> allGroups = new TreeSet<byte[]>(Bytes.MEMCMP);
            allGroups.addAll(rollupResults.keySet());
            allGroups.addAll(rawResults.keySet());

            List<SplitRollupSpanGroup> results = new ArrayList<SplitRollupSpanGroup>(allGroups.size());

            for (byte[] group : allGroups) {
                SpanGroup rawGroup = rawResults.get(group);
                SpanGroup rollupGroup = rollupResults.get(group);
                results.add(new SplitRollupSpanGroup(rollupGroup, rawGroup));
            }

            return results.toArray(new DataPoints[0]);
        }

        /**
         * After both queries have run, merge their results
         *
         * @param dataPointArrays The results from both queries
         * @return The merged data points
         */
        @Override
        public DataPoints[] call(ArrayList<DataPoints[]> dataPointArrays) {
            DataPoints[] rollupResults = dataPointArrays.get(0);
            DataPoints[] rawResults = dataPointArrays.get(1);

            return merge(rollupResults, rawResults);
        }
    }
}
