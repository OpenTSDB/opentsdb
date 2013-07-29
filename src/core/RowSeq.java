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
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import net.opentsdb.meta.Annotation;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;

/**
 * Represents a read-only sequence of continuous HBase rows.
 * <p>
 * This class stores in memory the data of one or more continuous
 * HBase rows for a given time series. To consolidate memory, the data points
 * are stored in two byte arrays: one for the time offsets/flags and another
 * for the values. Access is granted via pointers.
 */
final class RowSeq implements DataPoints {

  /** The {@link TSDB} instance we belong to. */
  private final TSDB tsdb;

  /** First row key. */
  byte[] key;

  /**
   * Qualifiers for individual data points.
   * <p>
   * Each qualifier is on 2 or 4 bytes.  The last {@link Const#FLAG_BITS} bits 
   * are used to store flags (the type of the data point - integer or floating
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
   * Merges data points for the same HBase row into the local object.
   * When executing multiple async queries simultaneously, they may call into 
   * this method with data sets that are out of order. This may ONLY be called 
   * after setRow() has initiated the rowseq.
   * @param row The compacted HBase row to merge into this instance.
   * @throws IllegalStateException if {@link #setRow} wasn't called first.
   * @throws IllegalArgumentException if the data points in the argument
   * do not belong to the same row as this RowSeq
   */
  void addRow(final KeyValue row) {
    if (this.key == null) {
      throw new IllegalStateException("setRow was never called on " + this);
    }

    final byte[] key = row.key();
    if (!Bytes.equals(this.key, key)) {
      throw new IllegalDataException("Attempt to add a different row="
          + row + ", this=" + this);
    }

    final byte[] remote_qual = row.qualifier();
    final byte[] remote_val = row.value();
    final byte[] merged_qualifiers = new byte[qualifiers.length + remote_qual.length];
    final byte[] merged_values = new byte[values.length + remote_val.length]; 

    int remote_q_index = 0;
    int local_q_index = 0;
    int merged_q_index = 0;
    
    int remote_v_index = 0;
    int local_v_index = 0;
    int merged_v_index = 0;
    short v_length;
    short q_length;
    while (remote_q_index < remote_qual.length || 
        local_q_index < qualifiers.length) {
      // if the remote q has finished, we just need to handle left over locals
      if (remote_q_index >= remote_qual.length) {
        v_length = Internal.getValueLengthFromQualifier(qualifiers, 
            local_q_index);
        System.arraycopy(values, local_v_index, merged_values, 
            merged_v_index, v_length);
        local_v_index += v_length;
        merged_v_index += v_length;
        
        q_length = Internal.getQualifierLength(qualifiers, 
            local_q_index);
        System.arraycopy(qualifiers, local_q_index, merged_qualifiers, 
            merged_q_index, q_length);
        local_q_index += q_length;
        merged_q_index += q_length;
        
        continue;
      }
      
      // if the local q has finished, we need to handle the left over remotes
      if (local_q_index >= qualifiers.length) {
        v_length = Internal.getValueLengthFromQualifier(remote_qual, 
            remote_q_index);
        System.arraycopy(remote_val, remote_v_index, merged_values, 
            merged_v_index, v_length);
        remote_v_index += v_length;
        merged_v_index += v_length;
        
        q_length = Internal.getQualifierLength(remote_qual, 
            remote_q_index);
        System.arraycopy(remote_qual, remote_q_index, merged_qualifiers, 
            merged_q_index, q_length);
        remote_q_index += q_length;
        merged_q_index += q_length;
        
        continue;
      }
      
      // for dupes, we just need to skip and continue
      final int sort = Internal.compareQualifiers(remote_qual, remote_q_index, 
          qualifiers, local_q_index);
      if (sort == 0) {
        //LOG.debug("Discarding duplicate timestamp: " + 
        //    Internal.getOffsetFromQualifier(remote_qual, remote_q_index));
        v_length = Internal.getValueLengthFromQualifier(remote_qual, 
            remote_q_index);
        remote_v_index += v_length;
        q_length = Internal.getQualifierLength(remote_qual, 
            remote_q_index);
        remote_q_index += q_length;
        continue;
      }
      
      if (sort < 0) {
        v_length = Internal.getValueLengthFromQualifier(remote_qual, 
            remote_q_index);
        System.arraycopy(remote_val, remote_v_index, merged_values, 
            merged_v_index, v_length);
        remote_v_index += v_length;
        merged_v_index += v_length;
        
        q_length = Internal.getQualifierLength(remote_qual, 
            remote_q_index);
        System.arraycopy(remote_qual, remote_q_index, merged_qualifiers, 
            merged_q_index, q_length);
        remote_q_index += q_length;
        merged_q_index += q_length;
      } else {
        v_length = Internal.getValueLengthFromQualifier(qualifiers, 
            local_q_index);
        System.arraycopy(values, local_v_index, merged_values, 
            merged_v_index, v_length);
        local_v_index += v_length;
        merged_v_index += v_length;
        
        q_length = Internal.getQualifierLength(qualifiers, 
            local_q_index);
        System.arraycopy(qualifiers, local_q_index, merged_qualifiers, 
            merged_q_index, q_length);
        local_q_index += q_length;
        merged_q_index += q_length;
      }
    }
    
    // we may have skipped some columns if we were given duplicates. Since we
    // had allocated enough bytes to hold the incoming row, we need to shrink
    // the final results
    if (merged_q_index == merged_qualifiers.length) {
      qualifiers = merged_qualifiers;
    } else {
      qualifiers = Arrays.copyOfRange(merged_qualifiers, 0, merged_q_index);
    }
    
    // we need to leave a meta byte on the end of the values array, so no
    // matter the index value, just increment it by one. The merged_values will
    // have two meta bytes, we only want one.
    values = Arrays.copyOfRange(merged_values, 0, merged_v_index + 1);
  }

  /**
   * Extracts the value of a cell containing a data point.
   * @param value The contents of a cell in HBase.
   * @param value_idx The offset inside {@code values} at which the value
   * starts.
   * @param flags The flags for this value.
   * @return The value of the cell.
   * @throws IllegalDataException if the data is malformed
   */
  static long extractIntegerValue(final byte[] values,
                                  final int value_idx,
                                  final byte flags) {
    switch (flags & Const.LENGTH_MASK) {
      case 7: return Bytes.getLong(values, value_idx);
      case 3: return Bytes.getInt(values, value_idx);
      case 1: return Bytes.getShort(values, value_idx);
      case 0: return values[value_idx] & 0xFF;
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
   * @throws IllegalDataException if the data is malformed
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

  /** @return an empty list since aggregated tags cannot exist on a single row */
  public List<String> getAggregatedTags() {
    return Collections.emptyList();
  }
  
  public List<String> getTSUIDs() {
    return Collections.emptyList();
  }
  
  /** @return null since annotations are stored at the SpanGroup level. They
   * are filtered when a row is compacted */ 
  public List<Annotation> getAnnotations() {
    return Collections.emptyList();
  }

  /** @return the number of data points in this row 
   * Unfortunately we must walk the entire array as there may be a mix of
   * second and millisecond timestamps */
  public int size() {
    int size = 0;
    for (int i = 0; i < qualifiers.length; i += 2) {
      if ((qualifiers[i] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
        i += 2;
      }
      size++;
    }
    return size;
  }

  /** @return 0 since aggregation cannot happen at the row level */
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
    // ^^ Can't do that with mixed support as seconds are on 2 bytes and ms on 4
    int index = 0;
    for (int idx = 0; idx < qualifiers.length; idx += 2) {
      if (i == index) {
        return Internal.getTimestampFromQualifier(qualifiers, baseTime(), idx);
      }
      if (Internal.inMilliseconds(qualifiers[idx])) {
        idx += 2;
      }      
      index++;
    }
    
    throw new RuntimeException(
        "WTF timestamp for index: " + i + " on " + this);
  }

  public boolean isInteger(final int i) {
    checkIndex(i);
    return (Internal.getFlagsFromQualifier(qualifiers, i) & 
        Const.FLAG_FLOAT) == 0x0;
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
   * Returns the value at index {@code i} regardless whether it's an integer or
   * floating point
   * @param i A 0 based index incremented per the number of data points in the
   * row.
   * @return the value as a double
   * @throws IndexOutOfBoundsException if the index would be out of bounds
   * @throws IllegalDataException if the data is malformed
   */
  double toDouble(final int i) {
    if (isInteger(i)) {
      return longValue(i);
    } else {
      return doubleValue(i);
    }
  }

  /** Returns a human readable string representation of the object. */
  @Override
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
       .append(")");    
    // TODO - fix this so it doesn't cause infinite recursions. If longValue()
    // throws an exception, the exception will call this method, trying to get
    // longValue() again, which will throw another exception.... For now, just
    // dump the raw data as hex
    //for (short i = 0; i < size; i++) {
    //  final short qual = (short) Bytes.getUnsignedShort(qualifiers, i * 2);
    //  buf.append('+').append((qual & 0xFFFF) >>> Const.FLAG_BITS);
    //  
    //  if (isInteger(i)) {
    //    buf.append(":long(").append(longValue(i));
    //  } else {
    //    buf.append(":float(").append(doubleValue(i));
    //  }
    //  buf.append(')');
    //  if (i != size - 1) {
    //    buf.append(", ");
    //  }
    //}
    buf.append("(datapoints=").append(size);
    buf.append("), (qualifier=[").append(Arrays.toString(qualifiers));
    buf.append("]), (values=[").append(Arrays.toString(values));
    buf.append("])");
    return buf.toString();
  }

  /**
   * Used to compare two RowSeq objects when sorting a {@link Span}. Compares
   * on the {@code RowSeq#baseTime()}
   * @since 2.0
   */
  public static final class RowSeqComparator implements Comparator<RowSeq> {
    public int compare(final RowSeq a, final RowSeq b) {
      if (a.baseTime() == b.baseTime()) {
        return 0;
      }
      return a.baseTime() < b.baseTime() ? -1 : 1;
    }
  }
  
  /** Iterator for {@link RowSeq}s.  */
  final class Iterator implements SeekableView, DataPoint {

    /** Current qualifier.  */
    private int qualifier;

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
      
      if (Internal.inMilliseconds(qualifiers[qual_index])) {
        qualifier = Bytes.getInt(qualifiers, qual_index);
        qual_index += 4;
      } else {
        qualifier = Bytes.getUnsignedShort(qualifiers, qual_index);
        qual_index += 2;
      }
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
      if ((timestamp & Const.MILLISECOND_MASK) != 0) {  // negative or not 48 bits
        throw new IllegalArgumentException("invalid timestamp: " + timestamp);
      }
      qual_index = 0;
      value_index = 0;
      final int len = qualifiers.length;
      //LOG.debug("Peeking timestamp: " + (peekNextTimestamp() < timestamp));
      while (qual_index < len && peekNextTimestamp() < timestamp) {
        //LOG.debug("Moving to next timestamp: " + peekNextTimestamp());
        if (Internal.inMilliseconds(qualifiers[qual_index])) {
          qualifier = Bytes.getInt(qualifiers, qual_index);
          qual_index += 4;
        } else {
          qualifier = Bytes.getUnsignedShort(qualifiers, qual_index);
          qual_index += 2;
        }
        final byte flags = (byte) qualifier;
        value_index += (flags & Const.LENGTH_MASK) + 1;
      }
      //LOG.debug("seek to " + timestamp + " -> now=" + toStringSummary());
    }

    // ------------------- //
    // DataPoint interface //
    // ------------------- //

    public long timestamp() {
      assert qual_index > 0: "not initialized: " + this;
      if ((qualifier & Const.MS_FLAG) == Const.MS_FLAG) {
        final long ms = (qualifier & 0x0FFFFFC0) >>> (Const.MS_FLAG_BITS);
        return (base_time * 1000) + ms;            
      } else {
        final long seconds = (qualifier & 0xFFFF) >>> Const.FLAG_BITS;
        return (base_time + seconds) * 1000;
      }
    }

    public boolean isInteger() {
      assert qual_index > 0: "not initialized: " + this;
      return (qualifier & Const.FLAG_FLOAT) == 0x0;
    }

    public long longValue() {
      if (!isInteger()) {
        throw new ClassCastException("value @"
          + qual_index + " is not a long in " + this);
      }
      final byte flags = (byte) qualifier;
      final byte vlen = (byte) ((flags & Const.LENGTH_MASK) + 1);
      return extractIntegerValue(values, value_index - vlen, flags);
    }

    public double doubleValue() {
      if (isInteger()) {
        throw new ClassCastException("value @"
          + qual_index + " is not a float in " + this);
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
      return Internal.getTimestampFromQualifier(qualifiers, base_time, qual_index);
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
