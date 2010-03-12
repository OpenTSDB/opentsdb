// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import net.opentsdb.HBaseException;

/**
 * Receives new data points and stores them in HBase.
 */
final class IncomingDataPoints implements WritableDataPoints {

  private static final Logger LOG = LoggerFactory.getLogger(IncomingDataPoints.class);

  /** Default number of {@link Put}s to group together for batch imports. */
  private static final short DEFAULT_BATCH_SIZE = 1024;

  /** The {@code TSDB} instance we belong to. */
  private final TSDB tsdb;

  /**
   * The row key.
   * 3 bytes for the metric name, 4 bytes for the base timestamp, 6 bytes per
   * tag (3 for the name, 3 for the value).
   */
  private byte[] row;

  /**
   * Qualifiers for individual data points.
   * The last Const.FLAG_BITS bits are used to store flags (the type of the
   * data point - integer or floating point - and the size of the data point
   * in bytes).  The remaining MSBs store a delta in seconds from the base
   * timestamp stored in the row key.
   */
  private short[] qualifiers;

  /** Each value in the row. */
  private long[] values;

  /** Number of data points in this row. */
  private short size;

  /** Are we doing a batch import? */
  private boolean batch_import;

  /**
   * Constructor.
   * @param tsdb The TSDB we belong to.
   */
  IncomingDataPoints(final TSDB tsdb) {
    this.tsdb = tsdb;
    this.qualifiers = new short[3];
    this.values = new long[3];
  }

  public void setSeries(final String metric, final Map<String, String> tags) {
    if (tags.size() <= 0) {
      throw new IllegalArgumentException("Need at least one tags (metric="
          + metric + ", tags=" + tags + ')');
    } else if (tags.size() > Const.MAX_NUM_TAGS) {
      throw new IllegalArgumentException("Too many tags: " + tags.size()
          + " maximum allowed: " + Const.MAX_NUM_TAGS + ", tags: " + tags);
    }

    Tags.validateString("metric name", metric);
    for (final Map.Entry<String, String> tag : tags.entrySet()) {
      Tags.validateString("tag name", tag.getKey());
      Tags.validateString("tag name", tag.getValue());
    }

    final short metric_width = tsdb.metrics.width();
    final short tag_name_width = tsdb.tag_names.width();
    final short tag_value_width = tsdb.tag_values.width();
    final short num_tags = (short) tags.size();

    int row_size = (metric_width + Const.TIMESTAMP_BYTES
                    + tag_name_width * num_tags
                    + tag_value_width * num_tags);
    if (row == null || row.length != row_size) {
      row = new byte[row_size];
    }
    size = 0;

    short pos = 0;
    copyInRowKey(pos, tsdb.metrics.getId(metric));
    pos += metric_width;

    pos += Const.TIMESTAMP_BYTES;

    for(final byte[] tag : Tags.resolveOrCreateAll(tsdb, tags)) {
      copyInRowKey(pos, tag);
      pos += tag.length;
    }
  }

  /**
   * Copies the specified byte array at the specified offset in the row key.
   * @param offset The offset in the row key to start copying at.
   * @param bytes The bytes to copy.
   */
  private void copyInRowKey(short offset, byte[] bytes) {
    System.arraycopy(bytes, 0, row, offset, bytes.length);
  }

  /**
   * Implements {@link #addPoint} by storing a value with a specific flag.
   * @param timestamp The timestamp to associate with the value.
   * @param value The value to store, converted to a 64 bit representation and
   * stored in a long.  For instance, if the value is a floating point value,
   * it's expected to be converted using {@link Float#floatToRawIntBits} and
   * then cast the resulting 32 bit int to a 64 bit long.
   * @param flags Flags to store in the qualifier (size and type of the data
   * point).
   */
  private void addPointInternal(final long timestamp, final long value,
                                final short flags) throws HBaseException {
    if (row == null) {
      throw new IllegalStateException("setSeries() never called!");
    }
    if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
      // => timestamp < 0 || timestamp > Integer.MAX_VALUE
      throw new IllegalArgumentException((timestamp < 0 ? "negative " : "bad")
          + " timestamp=" + timestamp
          + " when trying to add value=" + value + " to " + this);
    }

    long base_time;
    if (size > 0) {
      base_time = baseTime();
      final long last_ts = (base_time
                            + (qualifiers[size - 1] >>> Const.FLAG_BITS));
      if (timestamp <= last_ts) {
        throw new IllegalArgumentException("New timestamp=" + timestamp
            + " is less than previous=" + last_ts
            + " when trying to add value=" + value + " to " + this);
      } else if (timestamp - base_time > Const.MAX_TIMESPAN) {
        // Need to start a new row as we've exceeded Const.MAX_TIMESPAN.
        // We force the starting timestamp to be on a MAX_TIMESPAN boundary
        // so that all TSDs create rows with the same base time.  Otherwise
        // we'd need to coordinate TSDs to avoid creating rows that cover
        // overlapping time periods.
        base_time = timestamp - (timestamp % Const.MAX_TIMESPAN);
        // XXX use putInt XXX
        copyInRowKey(tsdb.metrics.width(), Bytes.toBytes((int) base_time));
        size = 0;
        //LOG.info("Starting a new row @ " + this);
      }
    } else {
      // This is the first data point, let's record the starting timestamp.
      // See comment above regarding overlapping rows to understand the modulo.
      base_time = timestamp - (timestamp % Const.MAX_TIMESPAN);
      copyInRowKey(tsdb.metrics.width(), Bytes.toBytes((int) base_time));
    }

    if (values.length == size) {
      grow();
    }

    // Java is so stupid with its auto-promotion of int to float.
    final short qualifier = (short) ((timestamp - base_time) << Const.FLAG_BITS
                                     | flags);
    qualifiers[size] = qualifier;
    values[size] = value;
    size++;

    final Put point = new Put(row);
    if (batch_import) {
      point.setWriteToWAL(false);  // Risky but much faster.
    }
    point.add(TSDB.FAMILY, Bytes.toBytes(qualifier), Bytes.toBytes(value));
    try {
      synchronized (tsdb.timeseries_table) {
        tsdb.timeseries_table.put(point);
      }
    } catch (IOException e) {
      final String errmsg = "Error while storing data in HBase.  Last Put="
        + point + ", there are "
        + tsdb.timeseries_table.getWriteBuffer().size()
        + " Puts currently in the write buffer.";
      LOG.error(errmsg, e);
      throw new HBaseException(errmsg, e);
    }
  }

  private void grow() {
    // We can't have more than 1 value per second, so MAX_TIMESPAN values.
    final int new_size = Math.min(size * 2, Const.MAX_TIMESPAN);
    if (new_size == size) {
      throw new AssertionError("Can't grow " + this + " larger.");
    }
    values = Arrays.copyOf(values, new_size);
    qualifiers = Arrays.copyOf(qualifiers, new_size);
  }

  /** Extracts the base timestamp from the row key. */
  private long baseTime() {
    return RowKey.baseTime(tsdb.metrics.width(), row);
  }

  public void addPoint(final long timestamp,
                       final long value) throws HBaseException {
    final short flags = 0x7;  // An int stored on 8 bytes.
    addPointInternal(timestamp, value, flags);
  }

  public void addPoint(long timestamp, float value) throws HBaseException {
    if (Float.isNaN(value) || Float.isInfinite(value)) {
      throw new IllegalArgumentException("value is NaN or Infinite: " + value
                                         + " for timestamp=" + timestamp);
    }
    final short flags = Const.FLAG_FLOAT | 0x3;  // A float stored on 4 bytes.
    addPointInternal(timestamp,
                     Float.floatToRawIntBits(value) & 0x00000000FFFFFFFFL,
                     flags);
  }

  public void groupCommit(final short commits) throws HBaseException {
    if (commits < 0) {
      throw new IllegalArgumentException("invalid size: " + commits);
    }
    try {
      tsdb.timeseries_table.setWriteBufferSize(commits * Const.AVG_PUT_SIZE);
    } catch (IOException e) {
      final String errmsg = "Error while auto-flushing Puts.  There are"
        + tsdb.timeseries_table.getWriteBuffer().size()
        + " Puts left in the write buffer.";
      LOG.error(errmsg, e);
      throw new HBaseException(errmsg, e);
    }
  }

  public void setBatchImport(final boolean batchornot) {
    if (batch_import == batchornot) {
      return;
    }
    final long current_size = tsdb.timeseries_table.getWriteBufferSize();
    final int new_size = DEFAULT_BATCH_SIZE * Const.AVG_PUT_SIZE;
    if (batchornot) {
      batch_import = true;
      // If we already were given a larger groupCommit size, don't override it.
      if (new_size > current_size) {
        groupCommit(DEFAULT_BATCH_SIZE);
      }
    } else {
      batch_import = false;
      // If we're using the DEFAULT_BATCH_SIZE, revert back to 0.
      if (current_size == new_size) {
        groupCommit((short) 0);
      }
    }
  }

  public String metricName() {
    if (row == null) {
      throw new IllegalStateException("setSeries never called before!");
    }
    final byte[] id = Arrays.copyOfRange(row, 0, tsdb.metrics.width());
    return tsdb.metrics.getName(id);
  }

  public Map<String, String> getTags() {
    return Tags.getTags(tsdb, row);
  }

  public List<String> getAggregatedTags() {
    return Collections.emptyList();
  }

  public int size() {
    return size;
  }

  public int aggregatedSize() {
    return 0;
  }

  public SeekableView iterator() {
    return new DataPointsIterator(this);
  }

  /** @throws IndexOutOfBoundsException if {@code i} is out of bounds. */
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

  public long timestamp(final int i) {
    checkIndex(i);
    return baseTime() + ((qualifiers[i] >>> Const.FLAG_BITS) & 0xFFFF);
  }

  public boolean isInteger(final int i) {
    checkIndex(i);
    return (qualifiers[i] & Const.FLAG_FLOAT) == 0x0;
  }

  public long longValue(final int i) {
    checkIndex(i);
    if (isInteger(i)) {
      return values[i];
    }
    throw new ClassCastException("value #" + i + " is not a long in " + this);
  }

  public double doubleValue(final int i) {
    checkIndex(i);
    if (!isInteger(i)) {
      return Float.intBitsToFloat((int) values[i]);
    }
    throw new ClassCastException("value #" + i + " is not a float in " + this);
  }

  /** Returns a human readable string representation of the object. */
  public String toString() {
    // The argument passed to StringBuilder is a pretty good estimate of the
    // length of the final string based on the row key and number of elements.
    final String metric = metricName();
    final StringBuilder buf = new StringBuilder(80 + metric.length()
                                                + row.length * 4 + size * 16);
    final long base_time = baseTime();
    buf.append("IncomingDataPoints(")
       .append(row == null ? "<null>" : Arrays.toString(row))
       .append(" (metric=")
       .append(metric)
       .append("), base_time=")
       .append(base_time)
       .append(" (")
       .append(base_time > 0 ? new Date(base_time * 1000) : "no date")
       .append("), [");
    for (short i = 0; i < size; i++) {
      buf.append('+').append(qualifiers[i] >>> Const.FLAG_BITS);
      if (isInteger(i)) {
        buf.append(":long(").append(longValue(i));
      } else {
        buf.append(":float(").append(doubleValue(i));
      }
      buf.append(')');
      if (i != size - 1) {
        buf.append(", ");
      }
    }
    buf.append("])");
    return buf.toString();
  }

}
