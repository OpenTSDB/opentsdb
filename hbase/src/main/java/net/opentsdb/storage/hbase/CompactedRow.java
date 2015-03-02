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
package net.opentsdb.storage.hbase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import net.opentsdb.core.Const;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.Internal;
import net.opentsdb.core.SeekableView;
import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.IdUtils;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.SignedBytes;
import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents a read-only sequence of continuous HBase rows.
 * <p>
 * This class stores in memory the data of one or more continuous
 * HBase rows for a given time series. To consolidate memory, the data points
 * are stored in two byte arrays: one for the time offsets/flags and another
 * for the values. Access is granted via pointers.
 */
public final class CompactedRow implements DataPoints {
  /** First row key. */
  private final byte[] key;

  /**
   * Qualifiers for individual data points.
   * <p>
   * Each qualifier is on 2 or 4 bytes.  The last {@link net.opentsdb.core.Const#FLAG_BITS} bits
   * are used to store flags (the type of the data point - integer or floating
   * point - and the size of the data point in bytes).  The remaining MSBs
   * store a delta in seconds from the base timestamp stored in the row key.
   */
  private final byte[] qualifiers;

  /** Values in the row.  */
  private final byte[] values;

  /** The annotations contained within this row */
  private final List<Annotation> annotations;

  public CompactedRow(final KeyValue kv, final List<Annotation> annotations) {
    this.key = checkNotNull(kv.key());
    this.qualifiers = checkNotNull(kv.qualifier());
    this.values = checkNotNull(kv.value());

    this.annotations = checkNotNull(annotations);
  }

  /**
   * Extracts the value of a cell containing a data point.
   * @param values The contents of a cell in HBase.
   * @param value_idx The offset inside {@code values} at which the value
   * starts.
   * @param flags The flags for this value.
   * @return The value of the cell.
   * @throws net.opentsdb.core.IllegalDataException if the data is malformed
   */
  public static long extractIntegerValue(final byte[] values,
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
   * @param values The contents of a cell in HBase.
   * @param value_idx The offset inside {@code values} at which the value
   * starts.
   * @param flags The flags for this value.
   * @return The value of the cell.
   * @throws IllegalDataException if the data is malformed
   */
  public static double extractFloatingPointValue(final byte[] values,
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

  /**
   * @see DataPoints#metric()
   */
  @Override
  public byte[] metric() {
    return RowKey.metric(key);
  }

  /**
   * @see DataPoints#tags()
   */
  @Override
  public Map<byte[],byte[]> tags() {
    return RowKey.tags(key);
  }

  /**
   * @see DataPoints#aggregatedTags()
   */
  @Override
  public List<byte[]> aggregatedTags() {
    return Collections.emptyList();
  }

  /**
   * @see DataPoints#getTSUIDs()
   */
  @Override
  public List<String> getTSUIDs() {
    byte[] str_tsuid = RowKey.tsuid(key);
    return Lists.newArrayList(IdUtils.uidToString(str_tsuid));
  }

  /**
   * @see DataPoints#getAnnotations()
   */
  @Override
  public List<Annotation> getAnnotations() {
    return annotations;
  }

  /** @return the number of data points in this row 
   * Unfortunately we must walk the entire array as there may be a mix of
   * second and millisecond timestamps */
  @Override
  public int size() {
    // if we don't have a mix of second and millisecond qualifiers we can run
    // this in O(1), otherwise we have to run O(n)
    if ((values[values.length - 1] & Const.MS_MIXED_COMPACT) == 
      Const.MS_MIXED_COMPACT) {
      int size = 0;
      for (int i = 0; i < qualifiers.length; i += 2) {
        if ((qualifiers[i] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
          i += 2;
        }
        size++;
      }
      return size;
    } else if ((qualifiers[0] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
      return qualifiers.length / 4;
    } else {
      return qualifiers.length / 2;
    }
  }

  /** @return 0 since aggregation cannot happen at the row level */
  @Override
  public int aggregatedSize() {
    return 0;
  }

  @Override
  public SeekableView iterator() {
    return new Iterator();
  }

  @Override
  public long timestamp(final int i) {
    checkElementIndex(i, size(), "i");
    // if we don't have a mix of second and millisecond qualifiers we can run
    // this in O(1), otherwise we have to run O(n)
    // Important: Span.addRow assumes this method to work in O(1).
    if ((values[values.length - 1] & Const.MS_MIXED_COMPACT) == 
      Const.MS_MIXED_COMPACT) {
      int index = 0;
      for (int idx = 0; idx < qualifiers.length; idx += 2) {
        if (i == index) {
          return Internal.getTimestampFromQualifier(qualifiers, RowKey.baseTime(key), idx);
        }
        if (Internal.inMilliseconds(qualifiers[idx])) {
          idx += 2;
        }      
        index++;
      }
    } else if ((qualifiers[0] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
      return Internal.getTimestampFromQualifier(qualifiers, RowKey.baseTime(key), i * 4);
    } else {
      return Internal.getTimestampFromQualifier(qualifiers, RowKey.baseTime(key), i * 2);
    }
    
    throw new RuntimeException(
        "WTF timestamp for index: " + i + " on " + this);
  }

  @Override
  public boolean isInteger(final int i) {
    checkElementIndex(i, size(), "i");
    return (Internal.getFlagsFromQualifier(qualifiers, i) & 
        Const.FLAG_FLOAT) == 0x0;
  }

  @Override
  public long longValue(int i) {
    return Iterators.get(new Iterator(), i).longValue();
  }

  @Override
  public double doubleValue(int i) {
    return Iterators.get(new Iterator(), i).doubleValue();
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
    final String metric = Arrays.toString(metric());
    final int size = size();
    final StringBuilder buf = new StringBuilder(80 + metric.length()
                                                + key.length * 4
                                                + size * 16);
    final long base_time = RowKey.baseTime(key);
    buf.append("CompactedRow(")
       .append(Arrays.toString(key))
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
   * Used to compare two {@link CompactedRow}s.
   */
  @Override
  public int compareTo(final DataPoints that) {
    if (this == checkNotNull(that))
      return 0;

    if (that instanceof CompactedRow) {
      return compareTo((CompactedRow) that);
    }

    // TODO This does so we can't compare different types of DataPoints but
    // that is ok for now at least.
    return 1;
  }

  public int compareTo(final CompactedRow that) {
    int result = SignedBytes.lexicographicalComparator().compare(this.key, that.key);

    if (result == 0) {
      return Ints.saturatedCast(timestamp(0) - that.timestamp(0));
    }

    return result;
  }
  
  /** Iterator for {@link CompactedRow}s.  */
  final class Iterator implements SeekableView, DataPoint {

    /** Current qualifier.  */
    private int qualifier;

    /** Next index in {@link #qualifiers}.  */
    private int qual_index;

    /** Next index in {@link #values}.  */
    private int value_index;

    /** Pre-extracted base time of this row sequence.  */
    private final long base_time = RowKey.baseTime(key);

    Iterator() {
    }

    // ------------------ //
    // Iterator interface //
    // ------------------ //

    @Override
    public boolean hasNext() {
      return qual_index < qualifiers.length;
    }

    @Override
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

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    // ---------------------- //
    // SeekableView interface //
    // ---------------------- //

    @Override
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

    @Override
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

    @Override
    public boolean isInteger() {
      assert qual_index > 0: "not initialized: " + this;
      return (qualifier & Const.FLAG_FLOAT) == 0x0;
    }

    @Override
    public long longValue() {
      if (!isInteger()) {
        throw new ClassCastException("value @"
          + qual_index + " is not a long in " + this);
      }
      final byte flags = (byte) qualifier;
      final byte vlen = (byte) ((flags & Const.LENGTH_MASK) + 1);
      return extractIntegerValue(values, value_index - vlen, flags);
    }

    @Override
    public double doubleValue() {
      if (isInteger()) {
        throw new ClassCastException("value @"
          + qual_index + " is not a float in " + this);
      }
      final byte flags = (byte) qualifier;
      final byte vlen = (byte) ((flags & Const.LENGTH_MASK) + 1);
      return extractFloatingPointValue(values, value_index - vlen, flags);
    }

    @Override
    public double toDouble() {
      return isInteger() ? longValue() : doubleValue();
    }

    /**
     * Look a head to see the next timestamp.
     * @throws IndexOutOfBoundsException if we reached the end already.
     */
    long peekNextTimestamp() {
      return Internal.getTimestampFromQualifier(qualifiers, base_time, qual_index);
    }

    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("qual_index", qual_index)
              .add("value_index", value_index)
              .add("seq", CompactedRow.this)
              .toString();
    }
  }
}
