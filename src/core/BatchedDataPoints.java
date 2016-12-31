// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.stumbleupon.async.Deferred;

import org.hbase.async.Bytes;
import org.hbase.async.Bytes.ByteMap;

import net.opentsdb.meta.Annotation;

/**
 * Receives new data points and stores them in compacted form. No points are 
 * written until {@code flushNow} is called. This ensures that true batch 
 * dynamics can be leveraged. This implementation will allow an entire hours 
 * worth of data to be written in a single transaction to the data table.
 */
final class BatchedDataPoints implements WritableDataPoints {

  /**
   * The {@code TSDB} instance we belong to.
   */
  private final TSDB tsdb;

  /**
   * The row key. Optional salt + 3 bytes for the metric name, 4 bytes for the 
   * base timestamp, 6 bytes per tag (3 for the name, 3 for the value).
   */
  private byte[] row_key;

  /**
   * Track the last timestamp written for this series.
   */
  private long last_timestamp;

  /**
   * Number of data points in this row.
   */
  private int size = 0;

  /**
   * Storage of the compacted qualifier.
   */
  private byte[] batched_qualifier = new byte[Const.MAX_TIMESPAN * 4];

  /**
   * Storage of the compacted value.
   */
  private byte[] batched_value = new byte[Const.MAX_TIMESPAN * 8];

  /**
   * Track the index position where the next qualifier gets written.
   */
  private int qualifier_index = 0;

  /**
   * Track the index position where the next value gets written.
   */
  private int value_index = 0;

  /**
   * Track the base time for this batch of points.
   */
  private long base_time;

  /**
   * Constructor.
   * 
   * @param tsdb The TSDB we belong to.
   */
  BatchedDataPoints(final TSDB tsdb, final String metric, 
      final Map<String, String> tags) {
    this.tsdb = tsdb;
    setSeries(metric, tags);
  }

  /**
   * Sets the metric name and tags of this batch. This method only need be 
   * called if there is a desire to reuse the data structure after the data has
   * been flushed. This will reset all cached information in this data structure.
   * @throws IllegalArgumentException if the metric name is empty or contains 
   * illegal characters or if the tags list is empty or one of the elements 
   * contains illegal characters.
   */
  @Override
  public void setSeries(final String metric, final Map<String, String> tags) {
    IncomingDataPoints.checkMetricAndTags(metric, tags);
    try {
      row_key = IncomingDataPoints.rowKeyTemplate(tsdb, metric, tags);
      RowKey.prefixKeyWithSalt(row_key);
      reset();
    }
    catch (RuntimeException e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException("Should never happen", e);
    }
  }

  /**
   * Resets the indices without overwriting the buffers. So the same amount of
   * space will remain allocated.
   */
  private void reset() {
    size = 0;
    qualifier_index = 0;
    value_index = 0;
    base_time = Long.MIN_VALUE;
    last_timestamp = Long.MIN_VALUE;
  }

  /**
   * A copy of the values is created and sent with a put request. A reset is 
   * initialized which makes this data structure ready to be reused for the same 
   * metric and tags but for a different hour of data.
   * @return {@inheritDoc}
   */
  @Override
  public Deferred<Object> persist() {
    final byte[] q = Arrays.copyOfRange(batched_qualifier, 0, qualifier_index);
    final byte[] v = Arrays.copyOfRange(batched_value, 0, value_index);
    final byte[] r = Arrays.copyOfRange(row_key, 0, row_key.length);
    reset();
    return tsdb.put(r, q, v);
  }

  @Override
  public void setBufferingTime(short time) {
    // does nothing
  }

  @Override
  public void setBatchImport(boolean batchornot) {
    // does nothing
  }

  @Override
  public Deferred<Object> addPoint(final long timestamp, final long value) {
    final byte[] v;
    if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
      v = new byte[] {(byte) value};
    }
    else if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
      v = Bytes.fromShort((short) value);
    }
    else if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE) {
      v = Bytes.fromInt((int) value);
    }
    else {
      v = Bytes.fromLong(value);
    }
    final short flags = (short) (v.length - 1);  // Just the length.
    return addPointInternal(timestamp, v, flags);
  }

  @Override
  public Deferred<Object> addPoint(final long timestamp, final float value) {
    if (Float.isNaN(value) || Float.isInfinite(value)) {
      throw new IllegalArgumentException("value is NaN or Infinite: " + value
          + " for timestamp=" + timestamp);
    }
    final short flags = Const.FLAG_FLOAT | 0x3;  // A float stored on 4 bytes.
    return addPointInternal(timestamp, 
        Bytes.fromInt(Float.floatToRawIntBits(value)), flags);
  }

  /**
   * Implements {@link #addPoint} by storing a value with a specific flag.
   * 
   * @param timestamp The timestamp to associate with the value.
   * @param value The value to store.
   * @param flags Flags to store in the qualifier (size and type of the data point).
   */
  private Deferred<Object> addPointInternal(final long timestamp,
      final byte[] value, final short flags) throws IllegalDataException {
    final boolean ms_timestamp = (timestamp & Const.SECOND_MASK) != 0;

    // we only accept unix epoch timestamps in seconds or milliseconds
    if (timestamp < 0 || (ms_timestamp && timestamp > 9999999999999L)) {
      throw new IllegalArgumentException((timestamp < 0 ? "negative " : "bad")
          + " timestamp=" + timestamp
          + " when trying to add value=" + Arrays.toString(value) + " to " + this);
    }

    // always maintain lastTimestamp in milliseconds
    if ((ms_timestamp ? timestamp : timestamp * 1000) <= last_timestamp) {
      throw new IllegalArgumentException("New timestamp=" + timestamp
          + " is less than or equal to previous=" + last_timestamp
          + " when trying to add value=" + Arrays.toString(value)
          + " to " + this);
    }
    last_timestamp = (ms_timestamp ? timestamp : timestamp * 1000);

    long incomingBaseTime;
    if (ms_timestamp) {
      // drop the ms timestamp to seconds to calculate the base timestamp
      incomingBaseTime = 
          ((timestamp / 1000) - ((timestamp / 1000) % Const.MAX_TIMESPAN));
    }
    else {
      incomingBaseTime = (timestamp - (timestamp % Const.MAX_TIMESPAN));
    }

    /**
     * First time we add a point initialize the rows timestamp.
     */
    if (base_time == Long.MIN_VALUE) {
      base_time = incomingBaseTime;
      Bytes.setInt(row_key, (int) base_time, 
          tsdb.metrics.width() + Const.SALT_WIDTH());
    }

    if (incomingBaseTime - base_time >= Const.MAX_TIMESPAN) {
      throw new IllegalDataException(
          "The timestamp is beyond the boundary of this batch of data points");
    }
    if (incomingBaseTime < base_time) {
      throw new IllegalDataException(
          "The timestamp is prior to the boundary of this batch of data points");
    }

    // Java is so stupid with its auto-promotion of int to float.
    final byte[] new_qualifier = Internal.buildQualifier(timestamp, flags);

    // compact this data point with the previously compacted data points.
    append(new_qualifier, value);
    size++;

    /**
     * Satisfies the interface.
     */
    return Deferred.fromResult((Object) null);
  }

  /**
   * Checks the size of the qualifier and value arrays to make sure we have
   * space. If not then we double the size of the arrays. This way a row
   * allocates space for a full hour of second data but if the user requires
   * millisecond storage with more than 3600 points, it will expand.
   * @param next_qualifier The next qualifier to use for it's length
   * @param next_value The next value to use for it's length
   */
  private void ensureCapacity(final byte[] next_qualifier, 
      final byte[] next_value) {
    if (qualifier_index + next_qualifier.length >= batched_qualifier.length) {
      batched_qualifier = Arrays.copyOf(batched_qualifier,
          batched_qualifier.length * 2);
    }
    if (value_index + next_value.length >= batched_value.length) {
      batched_value = Arrays.copyOf(batched_value, batched_value.length * 2);
    }
  }

  /**
   * Appends the value and qualifier to the appropriate arrays
   * @param next_qualifier The next qualifier to append
   * @param next_value The next value to append
   */
  private void append(final byte[] next_qualifier, final byte[] next_value) {
    ensureCapacity(next_qualifier, next_value);

    // Now let's simply concatenate all the values together.
    System.arraycopy(next_value, 0, batched_value, value_index, next_value.length);
    value_index += next_value.length;

    // Now let's concatenate all the qualifiers together.
    System.arraycopy(next_qualifier, 0, batched_qualifier, qualifier_index, 
        next_qualifier.length);
    qualifier_index += next_qualifier.length;
  }

  @Override
  public String metricName() {
    try {
      return metricNameAsync().joinUninterruptibly();
    }
    catch (RuntimeException e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }

  @Override
  public Deferred<String> metricNameAsync() {
    if (row_key == null) {
      throw new IllegalStateException("Instance was not properly constructed!");
    }
    final byte[] id = Arrays.copyOfRange(row_key, Const.SALT_WIDTH(),
        tsdb.metrics.width() + Const.SALT_WIDTH());
    return tsdb.metrics.getNameAsync(id);
  }

  @Override
  public byte[] metricUID() {
    return Arrays.copyOfRange(row_key, Const.SALT_WIDTH(), 
        Const.SALT_WIDTH() + TSDB.metrics_width());
  }
  
  @Override
  public Map<String, String> getTags() {
    try {
      return getTagsAsync().joinUninterruptibly();
    }
    catch (RuntimeException e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  @Override
  public ByteMap<byte[]> getTagUids() {
    return Tags.getTagUids(row_key);
  }

  @Override
  public Deferred<Map<String, String>> getTagsAsync() {
    return Tags.getTagsAsync(tsdb, row_key);
  }

  @Override
  public List<String> getAggregatedTags() {
    return Collections.emptyList();
  }

  @Override
  public Deferred<List<String>> getAggregatedTagsAsync() {
    final List<String> empty = Collections.emptyList();
    return Deferred.fromResult(empty);
  }

  @Override
  public List<byte[]> getAggregatedTagUids() {
    return Collections.emptyList();
  }
  
  @Override
  public List<String> getTSUIDs() {
    return Collections.emptyList();
  }

  @Override
  public List<Annotation> getAnnotations() {
    return null;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public int aggregatedSize() {
    return 0;
  }

  @Override
  public SeekableView iterator() {
    return new DataPointsIterator(this);
  }

  /**
   * @throws IndexOutOfBoundsException if {@code i} is out of bounds.
   */
  private void checkIndex(final int i) {
    if (i > size) {
      throw new IndexOutOfBoundsException("index " + i + " > " + size
          + " for this=" + this);
    }
    if (i < 0) {
      throw new IndexOutOfBoundsException("negative index " + i
          + " for this=" + this);
    }
  }

  /**
   * Computes the proper offset to reach qualifier 
   * @param i
   * @return
   */
  private int qualifierOffset(final int i) {
    int offset = 0;
    for (int j = 0; j < i; j++) {
      offset += Internal.getQualifierLength(batched_qualifier, offset);
    }
    return offset;
  }

  @Override
  public long timestamp(final int i) {
    checkIndex(i);
    return Internal.getTimestampFromQualifier(batched_qualifier, base_time, qualifierOffset(i));
  }

  @Override
  public boolean isInteger(final int i) {
    checkIndex(i);
    return isInteger(i, qualifierOffset(i));
  }

  /**
   * Tells whether or not the ith value is integer. Uses pre-computed qualifier offset.
   * @param i
   * @param q_offset qualifier offset
   * @return
   */
  private boolean isInteger(final int i, final int q_offset) {
    final short flags = Internal.getFlagsFromQualifier(batched_qualifier, q_offset);
    return (flags & Const.FLAG_FLOAT) == 0x0;
  }

  @Override
  public long longValue(final int i) {
    checkIndex(i);
    // compute the prope value and qualifier offsets
    int v_offset = 0;
    int q_offset = 0;
    for (int j = 0; j < i; j++) {
      v_offset += Internal.getValueLengthFromQualifier(batched_qualifier, q_offset);
      q_offset += Internal.getQualifierLength(batched_qualifier, q_offset);
    }

    if (isInteger(i, q_offset)) {
      final short flags = Internal.getFlagsFromQualifier(batched_qualifier, q_offset);
      return Internal.extractIntegerValue(batched_value, v_offset, (byte)flags);
    }
    throw new ClassCastException("value #" + i + " is not a long in " + this);
  }
  
  @Override
  public double doubleValue(final int i) {
    checkIndex(i);
    // compute the proper value and qualifier offsets
    int v_offset = 0;
    int q_offset = 0;
    for (int j = 0; j < i; j++) {
      v_offset += Internal.getValueLengthFromQualifier(batched_qualifier, q_offset);
      q_offset += Internal.getQualifierLength(batched_qualifier, q_offset);
    }

    if (!isInteger(i, q_offset)) {
      final short flags = Internal.getFlagsFromQualifier(batched_qualifier, q_offset);
      return Internal.extractFloatingPointValue(batched_value, v_offset, (byte)flags);
    }
    throw new ClassCastException("value #" + i + " is not a float in " + this);
  }

  /**
   * Returns a human readable string representation of the object.
   */
  @Override
  public String toString() {
    // The argument passed to StringBuilder is a pretty good estimate of the
    // length of the final string based on the row key and number of elements.
    final String metric = metricName();
    final StringBuilder buf = new StringBuilder(80 + metric.length()
        + row_key.length * 4 + size * 16);
    buf.append("BatchedDataPoints(")
        .append(row_key == null ? "<null>" : Arrays.toString(row_key))
        .append(" (metric=")
        .append(metric)
        .append("), base_time=")
        .append(base_time)
        .append(" (")
        .append(base_time > 0 ? new Date(base_time * 1000) : "no date")
        .append("), [");
    int q_offset = 0;
    int v_offset = 0;
    for (int i = 0; i < size; i++) {
      buf.append('+').append(Internal.getOffsetFromQualifier(batched_qualifier, q_offset));
      final short flags = Internal.getFlagsFromQualifier(batched_qualifier, q_offset);
      if (isInteger(i, q_offset)) {
        buf.append(":long(")
          .append(Internal.extractIntegerValue(batched_value, v_offset, (byte)flags));
      }
      else {
        buf.append(":float(")
          .append(Internal.extractFloatingPointValue(batched_value, v_offset, (byte)flags));
      }
      buf.append(')');
      if (i != size - 1) {
        buf.append(", ");
      }
      v_offset += Internal.getValueLengthFromQualifier(batched_qualifier, q_offset);
      q_offset += Internal.getQualifierLength(batched_qualifier, q_offset);
    }
    buf.append("])");
    return buf.toString();
  }

  public int getQueryIndex() {
    throw new UnsupportedOperationException("Not mapped to a query");
  }
}
