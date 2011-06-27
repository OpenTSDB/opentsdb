// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;

/**
 * Represents a read-only sequence of continuous HBase rows.
 * <p>
 * This class stores in memory the data of one or more continuous
 * HBase rows for a given time series.
 */
final class RowSeq implements DataPoints {

  private static final Logger LOG = LoggerFactory.getLogger(RowSeq.class);

  /** The {@link TSDB} instance we belong to. */
  private final TSDB tsdb;

  /** First row key. */
  byte[] key;

  /**
   * Qualifiers for individual data points.
   * <p>
   * Each qualifier is on 2 bytes.  The last {@link Const#FLAG_BITS} bits are
   * used to store flags (the type of the data point - integer or floating
   * point - and the size of the data point in bytes).  The remaining MSBs
   * store a delta in seconds from the base timestamp stored in the row key.
   */
  private byte[] qualifiers;

  /** Values in the row.  */
  private byte[] values;

  /**
   * Constructor.
   * @param tsdb The TSDB we belong to.
   */
  RowSeq(final TSDB tsdb) {
    this.tsdb = tsdb;
  }

  /**
   * Sets the row this instance holds in RAM using a row from a scanner.
   * @param row The compacted HBase row to set.
   * @throws IllegalStateException if this method was already called.
   */
  void setRow(final KeyValue row) {
    if (this.key != null) {
      throw new IllegalStateException("setRow was already called on " + this);
    }

    this.key = row.key();
    this.qualifiers = row.qualifier();
    this.values = row.value();
  }

  /**
   * Merges another HBase row into this one.
   * When two continuous rows in HBase have data points that are close enough
   * together that they could be stored into the same row, it makes sense to
   * merge them into the same {@link RowSeq} instance in memory in order to save
   * RAM.
   * @param row The compacted HBase row to merge into this instance.
   * @throws IllegalStateException if {@link #setRow} wasn't called first.
   * @throws IllegalArgumentException if the data points in the argument
   * aren't close enough to those in this instance time-wise to be all merged
   * together.
   */
  void addRow(final KeyValue row) {
    if (this.key == null) {
      throw new IllegalStateException("setRow was never called on " + this);
    }

    final byte[] key = row.key();
    final long base_time = Bytes.getUnsignedInt(key, tsdb.metrics.width());
    final int time_adj = (int) (base_time - baseTime());
    if (time_adj <= 0) {
      // Corner case: if the time difference is 0 and the key is the same, it
      // means we've already added this row, possibly parts of it.  This
      // doesn't normally happen but can happen if the scanner we're using
      // timed out (its lease expired for whatever reason), in which case
      // asynchbase will transparently re-open the scanner and start scanning
      // from the row key we were on at the time the timeout happened.  In
      // that case, the easiest thing to do is to discard everything we know
      // about this row and start over, since we're going to get the full row
      // again anyway.
      if (time_adj != 0 || !Bytes.equals(this.key, key)) {
        throw new IllegalDataException("Attempt to add a row with a base_time="
          + base_time + " <= baseTime()=" + baseTime() + "; Row added=" + row
          + ", this=" + this);
      }
      this.key = null;  // To keep setRow happy.
      this.qualifiers = null;  // Throw away our previous work.
      this.values = null;      // free();
      setRow(row);
      return;
    }

    final byte[] qual = row.qualifier();
    final int len = qual.length;
    int last_delta = Bytes.getUnsignedShort(qualifiers, qualifiers.length - 2);
    last_delta >>= Const.FLAG_BITS;

    final int old_qual_len = qualifiers.length;
    final byte[] newquals = new byte[old_qual_len + len];
    System.arraycopy(qualifiers, 0, newquals, 0, old_qual_len);
    // Adjust the delta in all the qualifiers.
    for (int i = 0; i < len; i += 2) {
      short qualifier = Bytes.getShort(qual, i);
      final int time_delta = time_adj + ((qualifier & 0xFFFF) >>> Const.FLAG_BITS);
      if (!canTimeDeltaFit(time_delta)) {
         throw new IllegalDataException("time_delta at index " + i
           + " is too large: " + time_delta
           + " (qualifier=0x" + Integer.toHexString(qualifier & 0xFFFF)
           + " baseTime()=" + baseTime() + ", base_time=" + base_time
           + ", time_adj=" + time_adj
           + ") for " + row + " to be added to " + this);
      }
      if (last_delta >= time_delta) {
        LOG.error("new timestamp = " + (baseTime() + time_delta)
                  + " (index=" + i
                  + ") is < previous=" + (baseTime() + last_delta)
                  + " in addRow with row=" + row + " in this=" + this);
        return;  // Ignore this row, it came out of order.
      }
      qualifier = (short) ((time_delta << Const.FLAG_BITS)
                           | (qualifier & Const.FLAGS_MASK));
      Bytes.setShort(newquals, qualifier, old_qual_len + i);
    }
    this.qualifiers = newquals;

    final byte[] val = row.value();
    // We need to subtract 1 from the value length because both `values' and
    // `val' have an extra byte of meta-data at the end, but only need one of
    // them, not both.
    final int old_val_len = values.length - 1;
    final byte[] newvals = new byte[old_val_len + val.length];
    System.arraycopy(values, 0, newvals, 0, old_val_len);
    System.arraycopy(val, 0, newvals, old_val_len, val.length);
    this.values = newvals;
  }

  /**
   * Checks whether a time delta is short enough for a {@link RowSeq}.
   * @param time_delta A time delta in seconds.
   * @return {@code true} if the delta is small enough that two data points
   * separated by the time delta can fit together in the same {@link RowSeq},
   * {@code false} if they're distant enough in time that they must go in
   * different {@link RowSeq} instances.
   */
  static boolean canTimeDeltaFit(final long time_delta) {
    return time_delta < 1 << (Short.SIZE - Const.FLAG_BITS);
  }

  /**
   * Extracts the value of a cell containing a data point.
   * @param value The contents of a cell in HBase.
   * @param value_idx The offset inside {@code values} at which the value
   * starts.
   * @param flags The flags for this value.
   * @return The value of the cell.
   */
  private static long extractIntegerValue(final byte[] values,
                                          final int value_idx,
                                          final byte flags) {
    switch (flags & Const.LENGTH_MASK) {
      case 7: return Bytes.getLong(values, value_idx);
      case 3: return Bytes.getInt(values, value_idx);
      case 1: return Bytes.getShort(values, value_idx);
      case 0: return values[value_idx];
    }
    throw new IllegalDataException("Integer value @ " + value_idx
                                   + " not on 8/4/2/1 bytes in "
                                   + Arrays.toString(values));
  }

  /**
   * Extracts the value of a cell containing a data point.
   * @param value The contents of a cell in HBase.
   * @param value_idx The offset inside {@code values} at which the value
   * starts.
   * @param flags The flags for this value.
   * @return The value of the cell.
   */
  private static double extractFloatingPointValue(final byte[] values,
                                                  final int value_idx,
                                                  final byte flags) {
    switch (flags & Const.LENGTH_MASK) {
      case 7: return Double.longBitsToDouble(Bytes.getLong(values, value_idx));
      case 3: return Float.intBitsToFloat(Bytes.getInt(values, value_idx));
    }
    throw new IllegalDataException("Floating point value @ " + value_idx
                                   + " not on 8 or 4 bytes in "
                                   + Arrays.toString(values));
  }

  public String metricName() {
    if (key == null) {
      throw new IllegalStateException("the row key is null!");
    }
    return RowKey.metricName(tsdb, key);
  }

  public Map<String, String> getTags() {
    return Tags.getTags(tsdb, key);
  }

  public List<String> getAggregatedTags() {
    return Collections.emptyList();
  }

  public int size() {
    return qualifiers.length / 2;
  }

  public int aggregatedSize() {
    return 0;
  }

  public SeekableView iterator() {
    return internalIterator();
  }

  /** Package private iterator method to access it as a DataPointsIterator. */
  DataPointsIterator internalIterator() {
    // XXX this is now grossly inefficient, need to walk the arrays once.
    return new DataPointsIterator(this);
  }

  /** Extracts the base timestamp from the row key. */
  long baseTime() {
    return Bytes.getUnsignedInt(key, tsdb.metrics.width());
  }

  /** @throws IndexOutOfBoundsException if {@code i} is out of bounds. */
  private void checkIndex(final int i) {
    if (i >= size()) {
      throw new IndexOutOfBoundsException("index " + i + " >= " + size()
          + " for this=" + this);
    }
    if (i < 0) {
      throw new IndexOutOfBoundsException("negative index " + i
          + " for this=" + this);
    }
  }

  public long timestamp(final int i) {
    checkIndex(i);
    return baseTime()
      + (Bytes.getUnsignedShort(qualifiers, i * 2) >>> Const.FLAG_BITS);
  }

  public boolean isInteger(final int i) {
    checkIndex(i);
    return (qualifiers[i * 2 + 1] & Const.FLAG_FLOAT) == 0x0;
  }

  /**
   * Finds the offset of the {@code i}th value in {@link #values}.
   * <p>
   * Because values can have varying lengths, we need to walk the
   * {@link #qualifiers} array to sum up the length of all the values
   * up to the {@code i}th one, so we can tell where it starts.
   */
  private int findValueOffset(final int i) {
    int value_idx = 0;  // Need to find the index of the ith value.
    for (int j = 0; j < i; j++) {
      final byte flags = qualifiers[j * 2 + 1];
      value_idx += (flags & Const.LENGTH_MASK) + 1;
    }
    return value_idx;
  }

  public long longValue(final int i) {
    checkIndex(i);
    final int value_idx = findValueOffset(i);
    final byte flags = qualifiers[i * 2 + 1];
    if ((flags & Const.FLAG_FLOAT) != 0x0) {
      throw new ClassCastException("value #" + i + " is not a long in " + this);
    }
    return extractIntegerValue(values, value_idx, flags);
  }

  public double doubleValue(final int i) {
    checkIndex(i);
    final int value_idx = findValueOffset(i);
    final byte flags = qualifiers[i * 2 + 1];
    if ((flags & Const.FLAG_FLOAT) == 0x0) {
      throw new ClassCastException("value #" + i + " is not a float in " + this);
    }
    return extractFloatingPointValue(values, value_idx, flags);
  }

  /**
   * Returns the {@code i}th data point as a double value.
   */
  double toDouble(final int i) {
    if (isInteger(i)) {
      return longValue(i);
    } else {
      return doubleValue(i);
    }
  }

  /** Returns a human readable string representation of the object. */
  public String toString() {
    // The argument passed to StringBuilder is a pretty good estimate of the
    // length of the final string based on the row key and number of elements.
    final String metric = metricName();
    final int size = size();
    final StringBuilder buf = new StringBuilder(80 + metric.length()
                                                + key.length * 4
                                                + size * 16);
    final long base_time = baseTime();
    buf.append("RowSeq(")
       .append(key == null ? "<null>" : Arrays.toString(key))
       .append(" (metric=")
       .append(metric)
       .append("), base_time=")
       .append(base_time)
       .append(" (")
       .append(base_time > 0 ? new Date(base_time * 1000) : "no date")
       .append("), [");
    for (short i = 0; i < size; i++) {
      final short qual = Bytes.getShort(qualifiers, i * 2);
      buf.append('+').append((qual & 0xFFFF) >>> Const.FLAG_BITS);
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
