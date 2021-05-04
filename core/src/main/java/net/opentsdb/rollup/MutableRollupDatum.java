// This file is part of OpenTSDB.
// Copyright (C) 2015-2021  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.rollup;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesDatumId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;

/**
 * Represents a single rolled up data point.
 *
 * @since 2.4
 */
public class MutableRollupDatum implements RollupDatum {

  /** The interval in the format <#><units> such as 1m or 2h */
  private String interval;

  /** Optional aggregation function if this was a pre-aggregated data point. */
  protected String groupby_aggregator;

  /** The value we'll mutate. */
  protected MutableNumericSummaryValue value;

  /** A ref to the ID. */
  protected TimeSeriesDatumId id;

  /**
   * Default Ctor necessary for de/serialization
   */
  public MutableRollupDatum() {
    value = new MutableNumericSummaryValue();
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("id=").append(id)
       .append(" value=").append(value)
       .append(" value=").append(this.value).append(" ");
    buf.append(" groupByAggregator=")
       .append(groupby_aggregator)
       .append(" interval=").append(interval);
    return buf.toString();
  }

  @Override
  public String intervalString() {
    return interval;
  }

  /** @param interval The interval for this data point such as "1m" */
  public void setInterval(final String interval) {
    this.interval = interval;
  }

  @Override
  public final String groupByAggregatorString() {
    return groupby_aggregator;
  }

  /** @param groupby_aggregator an optional aggregation function if the data
   * point was pre-aggregated */
  public final void setGroupByAggregator(final String groupby_aggregator) {
    this.groupby_aggregator = groupby_aggregator;
  }

  @Override
  public int groupByAggregator() {
    // TODO
    return -1;
  }

  @Override
  public TimeSeriesDatumId id() {
    return id;
  }

  /**
   * @param id Set the ID.
   */
  public void setId(final TimeSeriesDatumId id) {
    this.id = id;
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> value() {
    return value;
  }

  /**
   * Resets the local value by copying the timestamp and value from the arguments.
   * @param value A numeric summary value.
   * @throws IllegalArgumentException if the timestamp or value was null.
   */
  public void reset(final TimeSeriesValue<NumericSummaryType> value) {
    this.value.reset(value);
  }

  /**
   * Resets with the non-null timestamp and optional value. If the value
   * is null, the local value is nulled. If not, then summaries are
   * copied over and summaries that don't exist in the given value are
   * cleared.
   * @param timestamp A non-null timestamp.
   * @param value An optional value to clone from.
   * @throws IllegalArgumentException if the timestamp was null.
   */
  public void reset(final TimeStamp timestamp,
                    final NumericSummaryType value) {
    this.value.reset(timestamp, value);
  }

  /**
   * Resets the value to null with the given timestamp.
   * @param timestamp A non-null timestamp to update from.
   * @throws IllegalArgumentException if the timestamp was null.
   */
  public void resetNull(final TimeStamp timestamp) {
    this.value.resetNull(timestamp);
  }

  /**
   * Resets just the value for this summary.
   * @param summary A summary ID.
   * @param value A value to set.
   */
  public void resetValue(final int summary, final long value) {
    this.value.resetValue(summary, value);
  }

  /**
   * Resets just the value for this summary.
   * @param summary A summary ID.
   * @param value A value to set.
   */
  public void resetValue(final int summary, final double value) {
    this.value.resetValue(summary, value);
  }

  /**
   * Resets the value for this summary. If the value was null and the
   * map has been cleared, we set the whole data point to null.
   * @param summary A summary ID.
   * @param value An optional value to reset.
   */
  public void resetValue(final int summary, final NumericType value) {
    this.value.resetValue(summary, value);
  }

  /**
   * Resets the value for this summary. If the value was null and the
   * map has been cleared, we set the whole data point to null.
   * @param summary A summary ID.
   * @param value An optional value to reset.
   */
  public void resetValue(final int summary,
                         final TimeSeriesValue<NumericType> value) {
    this.value.resetValue(summary, value);
  }

  /**
   * Removes the value and nulls the whole data point if the map is empty.
   * @param summary The summary to remove.
   */
  public void nullSummary(final int summary) {
    this.value.nullSummary(summary);
  }

  /**
   * Resets just the timestamp of the data point. Good for use with
   * aggregators but be CAREFUL not to return without updating the value.
   * @param timestamp The timestamp to set.
   */
  public void resetTimestamp(final TimeStamp timestamp) {
    this.value.resetTimestamp(timestamp);
  }
}
