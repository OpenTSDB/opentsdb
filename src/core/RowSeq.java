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

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

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
    // If both the current `values' and the new `val' are single values, then
    // we neither of them has a meta data byte so we need to add one to be
    // consistent with what we expect from compacted values.  Otherwise, we
    // need to subtract 1 from the value length.
    final int old_val_len = values.length - (old_qual_len == 2 ? 0 : 1);
    final byte[] newvals = new byte[old_val_len + val.length
      // Only add a meta-data byte if the new values don't have it.
      + (len == 2 ? 1 : 0)];
    System.arraycopy(values, 0, newvals, 0, old_val_len);
    System.arraycopy(val, 0, newvals, old_val_len, val.length);
    assert newvals[newvals.length - 1] == 0:
      "Incorrect meta data byte after merge of " + row
      + " resulting qualifiers=" + Arrays.toString(qualifiers)
      + ", values=" + Arrays.toString(newvals)
      + ", old values=" + Arrays.toString(values);
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
  static long extractIntegerValue(final byte[] values,
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
  static double extractFloatingPointValue(final byte[] values,
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

  /** Package private iterator method to access it as a {@link Iterator}. */
  Iterator internalIterator() {
    // XXX this is now grossly inefficient, need to walk the arrays once.
    return new Iterator();
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
    // Important: Span.addRow assumes this method to work in O(1).
    return baseTime()
      + (Bytes.getUnsignedShort(qualifiers, i * 2) >>> Const.FLAG_BITS);
  }

  public boolean isInteger(final int i) {
    checkIndex(i);
    return (qualifiers[i * 2 + 1] & Const.FLAG_FLOAT) == 0x0;
  }

  public long longValue(int i) {
    if (!isInteger(i)) {
      throw new ClassCastException("value #" + i + " is not a long in " + this);
    }
    final Iterator it = new Iterator();
    while (i-- >= 0) {
      it.next();
    }
    return it.longValue();
  }

  public double doubleValue(int i) {
    if (isInteger(i)) {
      throw new ClassCastException("value #" + i + " is not a float in " + this);
    }
    final Iterator it = new Iterator();
    while (i-- >= 0) {
      it.next();
    }
    return it.doubleValue();
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

  /** Iterator for {@link RowSeq}s.  */
  final class Iterator implements SeekableView, DataPoint {

    /** Current qualifier.  */
    private short qualifier;

    /** Next index in {@link #qualifiers}.  */
    private short qual_index;

    /** Next index in {@link #values}.  */
    private short value_index;

    /** Pre-extracted base time of this row sequence.  */
    private final long base_time = baseTime();

    Iterator() {
    }

    // ------------------ //
    // Iterator interface //
    // ------------------ //

    public boolean hasNext() {
      return qual_index < qualifiers.length;
    }

    public DataPoint next() {
      if (!hasNext()) {
        throw new NoSuchElementException("no more elements");
      }
      qualifier = Bytes.getShort(qualifiers, qual_index);
      qual_index += 2;
      final byte flags = (byte) qualifier;
      value_index += (flags & Const.LENGTH_MASK) + 1;
      //LOG.debug("next -> now=" + toStringSummary());
      return this;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    // ---------------------- //
    // SeekableView interface //
    // ---------------------- //

    public void seek(final long timestamp) {
      if ((timestamp & 0xFFFFFFFF00000000L) != 0) {  // negative or not 32 bits
        throw new IllegalArgumentException("invalid timestamp: " + timestamp);
      }
      qual_index = 0;
      value_index = 0;
      final int len = qualifiers.length;
      while (qual_index < len && peekNextTimestamp() < timestamp) {
        qual_index += 2;
        final byte flags = (byte) qualifier;
        value_index += (flags & Const.LENGTH_MASK) + 1;
      }
      if (qual_index > 0) {
        qualifier = Bytes.getShort(qualifiers, qual_index - 2);
      }
      //LOG.debug("seek to " + timestamp + " -> now=" + toStringSummary());
    }

    // ------------------- //
    // DataPoint interface //
    // ------------------- //

    public long timestamp() {
      assert qualifier != 0: "not initialized: " + this;
      return base_time + ((qualifier & 0xFFFF) >>> Const.FLAG_BITS);
    }

    public boolean isInteger() {
      assert qualifier != 0: "not initialized: " + this;
      return (qualifier & Const.FLAG_FLOAT) == 0x0;
    }

    public long longValue() {
      if (!isInteger()) {
        throw new ClassCastException("value #"
          + ((qual_index - 2) / 2) + " is not a long in " + this);
      }
      final byte flags = (byte) qualifier;
      final byte vlen = (byte) ((flags & Const.LENGTH_MASK) + 1);
      return extractIntegerValue(values, value_index - vlen, flags);
    }

    public double doubleValue() {
      if (isInteger()) {
        throw new ClassCastException("value #"
          + ((qual_index - 2) / 2) + " is not a float in " + this);
      }
      final byte flags = (byte) qualifier;
      final byte vlen = (byte) ((flags & Const.LENGTH_MASK) + 1);
      return extractFloatingPointValue(values, value_index - vlen, flags);
    }

    public double toDouble() {
      return isInteger() ? longValue() : doubleValue();
    }

    // ---------------- //
    // Helpers for Span //
    // ---------------- //

    /** Helper to take a snapshot of the state of this iterator.  */
    int saveState() {
      return (qual_index << 16) | (value_index & 0xFFFF);
    }

    /** Helper to restore a snapshot of the state of this iterator.  */
    void restoreState(int state) {
      value_index = (short) (state & 0xFFFF);
      state >>>= 16;
      qual_index = (short) state;
      qualifier = 0;
    }

    /**
     * Look a head to see the next timestamp.
     * @throws IndexOutOfBoundsException if we reached the end already.
     */
    long peekNextTimestamp() {
      return base_time
        + (Bytes.getUnsignedShort(qualifiers, qual_index) >>> Const.FLAG_BITS);
    }

    /** Only returns internal state for the iterator itself.  */
    String toStringSummary() {
      return "RowSeq.Iterator(qual_index=" + qual_index
        + ", value_index=" + value_index;
    }

    public String toString() {
      return toStringSummary() + ", seq=" + RowSeq.this + ')';
    }

  }
}
