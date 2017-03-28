// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric;

import java.util.NoSuchElementException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.DataShard;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.TimeStampComparator;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.utils.Bytes;

/**
 * An implementation of a data shard that stores 64 bit signed longs or single
 * or double precision floating point values (along with "real" value counts)
 * in compacted byte arrays for efficiency. This is similar to OpenTSDB v1/2's 
 * RowSeqs.
 * <p>
 * Adding values MUST be in increasing time order and duplicates are not allowed.
 * <p>
 * Adding can continue after iteration has started but after making a clone, values
 * cannot be added to either the original or the clone. Copies contain a reference
 * to the original arrays of data, avoiding memory waste.
 * <p>
 * Data is encoded as follows:
 * <b>Offsets</b>: Each offset is encoded on {@link #encodeOn()} bytes. The last
 * 7 bits encode the length of the real count as well as the type of value
 * (floating or integer) and the length of the value. The remaining bits are 
 * shifted to represent the offset. 
 * Of the 7 bits, the first three are the length - 1 of the real count (from 0 
 * to 7). The next bit is a 1 if the value is a floating point or 0 if it's an
 * integer. The last 3 bytes are the length - 1 of the value.
 * <b>Values:</b>: The values are varying width with a VLE real count followed by
 * either a VLE integer, a 4 bytes single precision float or 8 byte double 
 * precision double. 
 * 
 * @since 3.0
 */
public class NumericMillisecondShard extends TimeSeriesIterator<NumericType> 
  implements DataShard<NumericType> {

  /** The ID for the series. */
  private final TimeSeriesId id;
  
  /** The *width* of the data (in ms) to be stored in this shard so we can 
   * calculate how many bytes are needed to store offsets from the base time. */
  private final long span;
  
  /** How many bytes to encode the offset one from 1 to 8. */
  private final byte encode_on;
  
  /** An order if shard is part of a slice config. */
  private final int order;
  
  /** The base timestamp for the shard (the first timestamp added). */
  private long base_timestamp = -1;
  
  /** Index's for the write and read paths over the array. */
  private int write_offset_idx;
  private int write_value_idx;
  private int read_offset_idx;
  private int read_value_idx;
  
  /** The time offsets and real + value flags. */
  private byte[] offsets;
  
  /** The real counts and values. */
  private byte[] values;
  
  /** The last timestamp recorded to track dupes and OOO data. */
  private long last_timestamp = -1;
  
  /** The data point reset and returned. */
  private final MutableNumericType dp;
  
  /** A holder for the timestamp to update the {@link MutableNumericType}. */
  private final TimeStamp timestamp;
  
  /** Whether or not the shard was copied. */
  private boolean copied;
  
  /**
   * Default ctor that sizes the arrays for 1 value.
   * 
   * @param id A non-null ID to associate with the shard.
   * @param span The width of data to store in milliseconds.
   * @throws IllegalArgumentException if the ID was null or span was less than 1.
   */
  public NumericMillisecondShard(final TimeSeriesId id, final long span) {
    this(id, span, -1, 1);
  }
  
  /**
   * Default ctor that sizes the arrays for the given count. If the count is zero
   * then the arrays will be initialized empty.
   * 
   * @param id A non-null ID to associate with the shard.
   * @param span The width of data to store in milliseconds.
   * @param order An optional order within a slice config.
   * @throws IllegalArgumentException if the ID was null or span was less than
   * 1 or the count was less than zero.
   */
  public NumericMillisecondShard(final TimeSeriesId id, final long span, 
      final int order) {
    this(id, span, order, 1);
  }
  
  /**
   * Default ctor that sizes the arrays for the given count. If the count is zero
   * then the arrays will be initialized empty.
   * 
   * @param id A non-null ID to associate with the shard.
   * @param span The width of data to store in milliseconds.
   * @param order An optional order within a slice config.
   * @param count The expected number of values in the set.
   * @throws IllegalArgumentException if the ID was null or span was less than
   * 1 or the count was less than zero.
   */
  public NumericMillisecondShard(final TimeSeriesId id, final long span, 
      final int order, final int count) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    if (span < 1) {
      throw new IllegalArgumentException("Span cannot be less than 1.");
    }
    if (count < 0) {
      throw new IllegalArgumentException("Count cannot be less than zero.");
    }
    this.id = id;
    this.span = span;
    this.order = order;
    dp = new MutableNumericType(id);
    encode_on = NumericType.encodeOn(span, NumericType.TOTAL_FLAG_BITS);
    timestamp = new MillisecondTimeStamp(-1);
    offsets = new byte[count * encode_on];
    values = new byte[count * 4]; // may be too large or too small.
  }
  
  @Override
  public int order() {
    return order;
  }
  
  @Override
  public Deferred<Object> initialize() {
    updateContext();
    return Deferred.fromResult(null);
  }
  
  /**
   * Add a value to the shard. The value's timestamp must be greater than the
   * previously stored value.
   * @param timestamp A timestamp in Unix Epoch milliseconds.
   * @param value A signed integer value.
   * @param reals A real value.
   * @throws IllegalStateException if the shard has been copied and is no longer
   * accepting values.
   */
  public void add(final long timestamp, final long value, final long reals) {
    if (copied) {
      throw new IllegalStateException("Cannot add data after the shard has "
          + "been copied.");
    }
    if (base_timestamp < 0) {
      base_timestamp = timestamp;
      last_timestamp = timestamp;
    } else if (timestamp <= last_timestamp) {
      throw new IllegalArgumentException("Timestamp must be greater than "
          + "last time: " + last_timestamp);
    }
    last_timestamp = timestamp;
    
    final byte[] real_bytes = NumericType.vleEncodeLong(reals);
    final byte[] vle = NumericType.vleEncodeLong(value);
    final byte real_length = (byte) ((real_bytes.length - 1) 
        << NumericType.VALUE_FLAG_BITS);
    final byte flags = (byte) ((vle.length - 1) | real_length);
    final byte[] offset = Bytes.fromLong(
       (((timestamp - base_timestamp) << NumericType.TOTAL_FLAG_BITS) | flags));
    final byte[] v = new byte[real_bytes.length + vle.length];
    System.arraycopy(real_bytes, 0, v, 0, real_bytes.length);
    System.arraycopy(vle, 0, v, real_bytes.length, vle.length);
    add(offset, v);
  }
  
  /**
   * Add a value to the shard. The value's timestamp must be greater than the
   * previously stored value.
   * NOTE: Don't write a double 0 or you'll waste 8 bytes. Use the long zero.
   * @param timestamp A timestamp in Unix Epoch milliseconds.
   * @param value A signed floating point value. If it can fit within a single
   * precision encoding, the value will be converted.
   * @param reals A real value.
   * @throws IllegalStateException if the shard has been copied and is no longer
   * accepting values.
   */
  public void add(final long timestamp, final double value, final long reals) {
    if (copied) {
      throw new IllegalStateException("Cannot add data after the shard has "
          + "been copied.");
    }
    if (base_timestamp < 0) {
      base_timestamp = timestamp;
      last_timestamp = timestamp;
    } else if (timestamp <= last_timestamp) {
      throw new IllegalArgumentException("Timestamp must be greater than "
          + "last time: " + last_timestamp);
    }
    last_timestamp = timestamp;
    final byte[] real_bytes = NumericType.vleEncodeLong(reals);
    final byte[] vle = NumericType.fitsInFloat(value) ? 
        Bytes.fromInt(Float.floatToIntBits((float) value)) :
          Bytes.fromLong(Double.doubleToLongBits(value));
    final byte real_length = (byte) ((real_bytes.length - 1) 
        << NumericType.VALUE_FLAG_BITS);
    final byte flags = (byte) ((vle.length - 1) 
        | NumericType.FLAG_FLOAT | real_length);
    final byte[] offset = Bytes.fromLong(
       (((timestamp - base_timestamp) << NumericType.TOTAL_FLAG_BITS) | flags));
    final byte[] v = new byte[real_bytes.length + vle.length];
    System.arraycopy(real_bytes, 0, v, 0, real_bytes.length);
    System.arraycopy(vle, 0, v, real_bytes.length, vle.length);
    add(offset, v);
  }
  
  /**
   * Expands the array as necessary and writes the encoded offset and values
   * to the arrays.
   * @param offset A non-null encoded offset.
   * @param value A non-null encoded value.
   */
  private void add(final byte[] offset, final byte[] value) {
    while (write_offset_idx + encode_on >= offsets.length) {
      final byte[] offset_copy = new byte[offsets.length * 2];
      System.arraycopy(offsets, 0, offset_copy, 0, offsets.length);
      offsets = offset_copy;
    }
    
    while (write_value_idx + value.length >= values.length) {
      final byte[] values_copy = new byte[values.length * 2];
      System.arraycopy(values, 0, values_copy, 0, values.length);
      values = values_copy;
    }
    
    System.arraycopy(offset, 8 - encode_on, offsets, write_offset_idx, encode_on);
    write_offset_idx += encode_on;
    System.arraycopy(value, 0, values, write_value_idx, value.length);
    write_value_idx += value.length;
  }
  
  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public TimeStamp baseTime() {
    return new MillisecondTimeStamp(base_timestamp);
  }

  @Override
  public TypeToken<NumericType> type() {
    return NumericType.TYPE;
  }

  @Override
  public boolean cached() {
    return false;
  }

  @Override
  public TimeSeriesIterator<NumericType> iterator() {
    return this;
  }

  @Override
  public TimeSeriesValue<NumericType> next() {
    // TODO - fill
    if (read_offset_idx >= write_offset_idx || read_value_idx >= write_value_idx) {
      if (context != null) {
        dp.reset(context.syncTimestamp(), Double.NaN, 0);
        return dp;
      }
      throw new NoSuchElementException("No more data in shard");
    }
    final byte[] offset_copy = new byte[8];
    System.arraycopy(offsets, read_offset_idx, offset_copy, 8 - encode_on, encode_on);
    long offset = Bytes.getLong(offset_copy);
    final byte flags = (byte) offset;
    offset = offset >> NumericType.TOTAL_FLAG_BITS;

    final byte reals_length = (byte) ((flags & NumericType.REALS_LENGTH_MASK) 
        >> NumericType.VALUE_FLAG_BITS);
    final byte vlen = (byte) ((flags & NumericType.VALUE_LENGTH_MASK) + 1);
    timestamp.updateMsEpoch(base_timestamp + offset);
    
    
    if (context != null && 
        context.syncTimestamp().compare(TimeStampComparator.NE, timestamp)) {
      dp.reset(context.syncTimestamp(), Double.NaN, 0);
      updateContext();
      return dp;
    }
    
    // TODO - change reals to a long. *could* happen.
    final int reals = (int) NumericType.extractIntegerValue(values, 
        read_value_idx, reals_length);
    
    if ((flags & NumericType.FLAG_FLOAT) == 0x0) {
      dp.reset(timestamp, NumericType.extractIntegerValue(values, 
          read_value_idx + reals_length + 1, flags), reals);
    } else {
      dp.reset(timestamp, NumericType.extractFloatingPointValue(values, 
          read_value_idx + reals_length + 1, flags), reals);
    }
    read_offset_idx += encode_on;
    read_value_idx += vlen + reals_length + 1;
    
    updateContext();
    return dp;
  }

  @Override
  public TimeSeriesIterator<NumericType> getCopy(QueryContext context) {
    final NumericMillisecondShard shard = new NumericMillisecondShard(id, span);
    shard.base_timestamp = base_timestamp;
    shard.last_timestamp = last_timestamp;
    shard.read_offset_idx = 0;
    shard.write_offset_idx = write_offset_idx;
    shard.read_value_idx = 0;
    shard.write_value_idx = write_value_idx;
    shard.offsets = offsets;
    shard.values = values;
    shard.copied = true;
    copied = true;
    shard.setContext(context);
    return shard;
  }

  /**
   * If the context is not null, updates it with the status and timestamp.
   */
  private void updateContext() {
    if (context != null) {
      if (read_offset_idx >= write_offset_idx) {
        context.updateContext(IteratorStatus.END_OF_DATA, null);
      } else {
        final byte[] offset_copy = new byte[8];
        System.arraycopy(offsets, read_offset_idx, offset_copy, 8 - encode_on, encode_on);
        long offset = Bytes.getLong(offset_copy);
        offset = offset >> NumericType.TOTAL_FLAG_BITS;
        timestamp.updateMsEpoch(base_timestamp + offset);
        context.updateContext(IteratorStatus.HAS_DATA, timestamp);
      }
    }
  }
  
  @VisibleForTesting
  byte[] offsets() {
    return offsets;
  }
  
  @VisibleForTesting
  byte[] values() {
    return values;
  }
  
  @VisibleForTesting
  byte encodeOn() {
    return encode_on;
  }
}
