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
   * The last Const.FLAG_BITS bits are used to store flags (the type of the
   * data point - integer or floating point - and the size of the data point
   * in bytes).  The remaining MSBs store a delta in seconds from the base
   * timestamp stored in the row key.
   */
  private short[] qualifiers;

  /** Each value in the row. */
  private long[] values;

  /**
   * Constructor.
   * @param tsdb The TSDB we belong to.
   */
  RowSeq(final TSDB tsdb) {
    this.tsdb = tsdb;
  }

  /**
   * Sets the row this instance holds in RAM using a row from a scanner.
   * @param row The HBase row to set.
   * @throws IllegalStateException if this method was already called.
   */
  void setRow(final ArrayList<KeyValue> row) {
    final byte[] key = row.get(0).key();

    if (this.key == null) {
      this.key = key;
      final int npoints = row.size();
      values = new long[npoints];
      qualifiers = new short[npoints];
    } else {
      throw new IllegalStateException("setRow was already called on " + this);
    }

    int index = 0;  // position in `values'.
    for (final KeyValue kv : row) {
      final short qualifier = extractQualifier(kv);
      qualifiers[index] = qualifier;
      values[index] = extractLValue(qualifier, kv);
      if (index > 0 && timestamp(index - 1) >= timestamp(index)) {
        throw new IllegalDataException("new timestamp = " + timestamp(index)
            + " is < previous=" + timestamp(index -1)
            + " in setRow with kv=" + kv);
      }
      index++;
    }
  }

  /**
   * Merges another HBase row into this one.
   * When two continuous rows in HBase have data points that are close enough
   * together that they could be stored into the same row, it makes sense to
   * merge them into the same {@link RowSeq} instance in memory in order to save
   * RAM.
   * @param row The HBase row to merge into this instance.
   * @throws IllegalStateException if {@link #setRow} wasn't called first.
   * @throws IllegalArgumentException if the data points in the argument
   * aren't close enough to those in this instance time-wise to be all merged
   * together.
   */
  void addRow(final ArrayList<KeyValue> row) {
    final byte[] key = row.get(0).key();
    final long base_time = Bytes.getUnsignedInt(key, tsdb.metrics.width());

    // Save the old arrays in case we need to revert what we've done.
    final short old_qualifiers[] = qualifiers;
    final long old_values[] = values;

    int index = values.length;  // position in `values'.
    if (this.key != null) {
      final int new_length = values.length + row.size();
      values = Arrays.copyOf(values, new_length);
      qualifiers = Arrays.copyOf(qualifiers, new_length);
    } else {
      throw new IllegalStateException("setRow was never called on " + this);
    }

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
    for (final KeyValue kv : row) {
      short qualifier = extractQualifier(kv);
      final int time_delta = (qualifier & 0xFFFF) >>> Const.FLAG_BITS;
      if (!canTimeDeltaFit(time_delta)) {
        throw new IllegalArgumentException("time_delta too large " + time_delta
                                           + " to be added to " + this);
      }
      qualifier = (short) (((time_delta + time_adj) << Const.FLAG_BITS)
                           | (qualifier & Const.FLAGS_MASK));
      qualifiers[index] = qualifier;
      values[index] = extractLValue(qualifier, kv);
      if (index > 0 && timestamp(index - 1) >= timestamp(index)) {
        LOG.error("new timestamp = " + timestamp(index) + " (index=" + index
                  + ") is < previous=" + timestamp(index - 1)
                  + " in addRow with kv=" + kv + " in row=" + row);
        // Undo what we've done so far.
        qualifiers = old_qualifiers;
        values = old_values;
        return;  // Ignore this row, it came out of order.
      }
      index++;
    }
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
   * Extracts the qualifier of a cell containing a data point.
   * @param kv The cell.
   * @return The qualifier, on a short, since it's expected to be on 2 bytes.
   */
  private short extractQualifier(final KeyValue kv) {
    if (!Bytes.equals(TSDB.FAMILY, kv.family())) {
      throw new IllegalDataException("unexpected KeyValue family: "
                                     + Bytes.pretty(kv.family()));
    }
    final byte[] qual = kv.qualifier();
    if (qual.length != 2) {
      throw new IllegalDataException("Invalid qualifier length: "
                                     + Bytes.pretty(qual));
    }
    return Bytes.getShort(qual);
  }

  /**
   * Extracts the value of a cell containing a data point.
   * @param qualifier The qualifier of that cell, as returned by
   * {@link #extractQualifier}.
   * @param kv The cell.
   * @return The value of the cell, as a {@code long}, since it's expected to
   * be on 8 bytes at most.  If the cell contains a floating point value, the
   * bits of the {@code long} represent some kind of a floating point value.
   */
  private static long extractLValue(final short qualifier, final KeyValue kv) {
    final byte[] value = kv.value();
    if ((qualifier & Const.FLAG_FLOAT) != 0) {
      if (value.length == 4) {
        return Bytes.getInt(value);
      } else if (value.length == 8) {
        if (value[0] != 0 || value[1] != 0
            || value[2] != 0 || value[3] != 0) {
          throw new IllegalDataException("Float value with nonzero byte MSBs: " + kv);
        }
        return Bytes.getInt(value, 4);
      } else {
        throw new IllegalDataException("Float value not on 4 or 8 bytes: " + kv);
      }
    } else {
      switch (value.length) {
        case 8: return Bytes.getLong(value);
        case 4: return Bytes.getInt(value);
        case 2: return Bytes.getShort(value);
        case 1: return value[0];
      }
      throw new IllegalDataException("Integer value not on 8/4/2/1 bytes: " + kv);
    }
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
    return values.length;
  }

  public int aggregatedSize() {
    return 0;
  }

  public SeekableView iterator() {
    return internalIterator();
  }

  /** Package private iterator method to access it as a DataPointsIterator. */
  DataPointsIterator internalIterator() {
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
    // Stupid Java without unsigned integers and auto-promotion to int.
    return baseTime() + ((qualifiers[i] & 0xFFFF) >>> Const.FLAG_BITS);
  }

  public boolean isInteger(final int i) {
    checkIndex(i);
    return (qualifiers[i] & Const.FLAG_FLOAT) == 0x0;
  }

  public long longValue(final int i) {
    // Don't call checkIndex(i) because isInteger(i) already calls it.
    if (isInteger(i)) {
      return values[i];
    }
    throw new ClassCastException("value #" + i + " is not a long in " + this);
  }

  public double doubleValue(final int i) {
    // Don't call checkIndex(i) because isInteger(i) already calls it.
    if (!isInteger(i)) {
      return Float.intBitsToFloat((int) values[i]);
    }
    throw new ClassCastException("value #" + i + " is not a float in " + this);
  }

  /**
   * Returns the {@code i}th data point as a double value.
   */
  double toDouble(final int i) {
    if (isInteger(i)) {
      return values[i];
    } else {
      return Float.intBitsToFloat((int) values[i]);
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
