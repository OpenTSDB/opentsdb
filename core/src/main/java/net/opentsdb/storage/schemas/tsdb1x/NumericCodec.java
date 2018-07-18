// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.storage.schemas.tsdb1x;

import java.util.Arrays;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.ZonedNanoTimeStamp;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.IllegalDataException;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Pair;

/**
 * TODO - doc me and finish me
 * 
 * This is the OpenTSDB 1.x and 2.x storage schema for numeric values 
 * with an extension in 3.0 to support nanosecond qualifiers. It's fully
 * backwards compatible with old data.
 * <h1>Timestamps:</h1>
 * Timestamp offsets from the row key base timestamp are encoded in 
 * qualifiers along with value lengths and types on either 2, 4 or 
 * 8 (in 3.x) bytes. The last 4 bits (LSBs) are flags and the rest of 
 * the bytes are either resolution masks, real data or unused. To 
 * retrieve the value timestamp, parse add the offset to the row key
 * base timestamp which is always an epoch.
 * 
 * <h2>Seconds:</h2>
 * Second offsets are always on 2 bytes with the last 4 bits the flags
 * and the remainder the full offset value. For example the max value 
 * for a second offset, 3599, is encoded as:
 * <pre>
 * [ 0b11100000, 0b11110111 ]  2 bytes
 *     <-------------->^<->
 *       delta         1 type, 3 value length
 * </pre>
 * 
 * <h2>Milliseconds:</h2>
 * Milliseconds are on 4 bytes with the last 4 bits the flags, the next
 * 2 bits unused and the first 4 bits set to 1 to denote that the 
 * qualifier is for millisecond offsets. An example is:
 * <pre>
 * [ 0b11111101, 0b10111011, 0b10011111, 0b11000111 ]  4 bytes
 *     <--><-------------------------------->xx^<->
 *      msec mask,     delta                 2 unused, 1 type, 3 value length
 * </pre>
 * 
 * <h2>Nanoseconds:</h2>
 * Nanoseconds are on 8 bytes with the first byte wasted as the mask and 
 * the last 4 bits the flags. For example:
 * <pre>
 * [ 0b11111111, 0b10111011, ... 0b11111111, 0b00001111 ]  8 bytes
 *     <------>    <------------------------------>^<->
 *      nsec mask       delta                      1 type, 3 value length
 * </pre>
 * 
 * @since 3.0
 */
public class NumericCodec implements Codec {

  /** Mask to select the size of a value from the qualifier.  */
  public static final short LENGTH_MASK = 0x7;
  
  /** Number of LSBs in time_deltas reserved for flags.  */
  public static final short FLAG_BITS = 4;
  
  /** Number of LSBs in time_deltas reserved for flags.  */
  public static final short MS_FLAG_BITS = 6;
  
  /** The width of raw table qualifiers for second offsets. */
  public static final int S_Q_WIDTH = 2;
  
  /** The width of raw table qualifiers for millisecond offsets. */
  public static final int MS_Q_WIDTH = 4;
  
  /** The width of raw table qualifiers for nanosecond offsets. */
  public static final int NS_Q_WIDTH = 8;
  
  /**
   * When this bit is set, the value is a floating point value.
   * Otherwise it's an integer value.
   */
  public static final short FLAG_FLOAT = 0x8;
  
  /** Mask for the millisecond qualifier flag */
  public static final byte MS_BYTE_FLAG = (byte) 0xF0;
  
  /** Mask for the nanosecond qualifier flag */
  public static final byte NS_BYTE_FLAG = (byte) 0xFF;
  
  /** Flag to set on millisecond qualifier timestamps */
  public static final int MS_FLAG = 0xF0000000;

  /** Flag to set on nanosecond qualifier timestamps */
  public static final long NS_FLAG = 0xFF00000000000000L;
  
  /** ask applied to get the offset from a second qualifier. */
  public static final int S_MASK = 0x0000FFF0;
  
  /** Mask applied to get the offset from a millisecond qualifier. */
  public static final int MS_MASK = 0x0FFFFFC0;
  
  /** Mask applied to get the offset from a nansecond qualifier. */
  public static final long NS_MASK = 0x000FFFFFFFFFFFFFL;
  
  /** Mask to select all the FLAG_BITS in a reals length + values length/type
   * encoded offset.  */
  public static final short FLAGS_MASK = FLAG_FLOAT | LENGTH_MASK;
  
  /** Helper that fills a full 8 byte array with 1s. */
  public static final byte[] FULL_8_BYTES = new byte[] { (byte) 0xFF,  
      (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 
      (byte) 0xFF, (byte) 0xFF };
  
  /** Flag to determine if a compacted column is a mix of seconds and ms */
  public static final byte MS_MIXED_COMPACT = 1;
  
  /** The append qualifier. */
  public static final byte[] APPEND_QUALIFIER = new byte[] { 0, 0, 5 };
  
  /** The resolution of an offset for encoding/decoding purposes. */
  public static enum OffsetResolution {
    NANOS,
    MILLIS,
    SECONDS
  }
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return NumericType.TYPE;
  }

  @Override
  public Span<? extends TimeSeriesDataType> newSequences(
      final boolean reversed) {
    return new NumericSpan(reversed);
  }

  @Override
  public RowSeq newRowSeq(final long base_time) {
    return new NumericRowSeq(base_time);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public Pair<byte[], byte[]> encode(
      final TimeSeriesValue<? extends TimeSeriesDataType> value,
      final boolean append_format,
      final int base_time,
      final RollupInterval rollup_interval) {
    
    final byte[] v;
    final boolean is_float;
    if (((TimeSeriesValue<NumericType>) value).value().isInteger()) {
        v = vleEncodeLong(((TimeSeriesValue<NumericType>) value).value().longValue());
        is_float = false;
    } else {
      final double d = ((TimeSeriesValue<NumericType>) value).value().doubleValue();
      if ((float) d == d) {
        v = Bytes.fromInt(Float.floatToIntBits((float) d));
      } else {
        v = Bytes.fromLong(Double.doubleToRawLongBits(d));
      }
      is_float = true;
    }

    // TODO - someday, allow flexible offsets. Right now it's just on
    // the hour.
    final long offset;
    final OffsetResolution resolution;
    switch (value.timestamp().units()) {
    case SECONDS:
      offset = value.timestamp().epoch() - (long) base_time;
      resolution = OffsetResolution.SECONDS;
      break;
    case MILLIS:
      offset = value.timestamp().msEpoch() - (long) (base_time * 1000L);
      resolution = OffsetResolution.MILLIS;
      break;
    default:
      // TODO - nanos.
      throw new IllegalStateException("Unsupported resolution: " 
          + value.timestamp().units());
    }
    if (append_format) {
      return new Pair<byte[], byte[]>(APPEND_QUALIFIER, 
          encodeAppendValue(resolution, offset, v, is_float));
    } else {
      short flag = (short) ((is_float ? FLAG_FLOAT : (short) 0) | (short) (v.length - 1));
      switch (resolution) {
      case SECONDS:
        return new Pair<byte[], byte[]>(buildSecondQualifier(offset, flag), v);
      case MILLIS:
        return new Pair<byte[], byte[]>(buildMsQualifier(offset, flag), v);
      default:
        // TODO - nanos.
        throw new IllegalStateException("Unsupported resolution: " 
            + value.timestamp().units());
      }
    }
  }
  
  /**
   * Returns whether or not this is a floating value that needs to be fixed.
   * <p>
   * OpenTSDB used to encode all floating point values as `float' (4 bytes)
   * but actually store them on 8 bytes, with 4 leading 0 bytes, and flags
   * correctly stating the value was on 4 bytes.
   * (from CompactionQueue)
   * @param flags The least significant byte of a qualifier.
   * @param value The value that may need to be corrected.
   */
  public static boolean floatingPointValueToFix(final byte flags,
                                                final byte[] value) {
    return (flags & Const.FLAG_FLOAT) != 0   // We need a floating point value.
      && (flags & Const.LENGTH_MASK) == 0x3  // That pretends to be on 4 bytes.
      && value.length == 8;                  // But is actually using 8 bytes.
  }
  
  /**
   * Returns a corrected value if this is a floating point value to fix.
   * <p>
   * OpenTSDB used to encode all floating point values as `float' (4 bytes)
   * but actually store them on 8 bytes, with 4 leading 0 bytes, and flags
   * correctly stating the value was on 4 bytes.
   * <p>
   * This function detects such values and returns a corrected value, without
   * the 4 leading zeros.  Otherwise it returns the value unchanged.
   * (from CompactionQueue)
   * @param flags The least significant byte of a qualifier.
   * @param value The value that may need to be corrected.
   * @throws IllegalDataException if the value is malformed.
   */
  public static byte[] fixFloatingPointValue(final byte flags,
                                             final byte[] value) {
    if (floatingPointValueToFix(flags, value)) {
      // The first 4 bytes should really be zeros.
      if (value[0] == 0 && value[1] == 0 && value[2] == 0 && value[3] == 0) {
        // Just keep the last 4 bytes.
        return new byte[] { value[4], value[5], value[6], value[7] };
      } else {  // Very unlikely.
        throw new IllegalDataException("Corrupted floating point value: "
          + Arrays.toString(value) + " flags=0x" + Integer.toHexString(flags)
          + " -- first 4 bytes are expected to be zeros.");
      }
    }
    return value;
  }
  
  /**
   * Returns an 8 byte qualifier with the offset encoded and flags set.
   * @param offset An offset from [ 0, 3599999999999L ].
   * @param flags A flag encoding the value length and type.
   * @return A non-null byte.
   * @throws IllegalArgumentException if the offset was out of bounds.
   */
  public static byte[] buildNanoQualifier(final long offset, 
                                          final short flags) {
    if (offset < 0 || offset > 3599999999999L) {
      throw new IllegalArgumentException("Offset cannot be less than "
          + "zero or greater than 3599999999999");
    }
    return Bytes.fromLong((offset << FLAG_BITS) | flags | NS_FLAG);
  }
  
  /**
   * Returns a 4 byte qualifier with the offset encoded and flags set.
   * @param offset An offset from [ 0, 3599999 ].
   * @param flags A flag encoding the value length and type.
   * @return A non-null byte.
   * @throws IllegalArgumentException if the offset was out of bounds.
   */
  public static byte[] buildMsQualifier(final long offset, 
                                        final short flags) {
    if (offset < 0 || offset > 3599999L) {
      throw new IllegalArgumentException("Offset cannot be less than "
          + "zero or greater than 3599999");
    }
    return Bytes.fromInt((int) (offset << MS_FLAG_BITS) | flags | MS_FLAG);
  }
  
  /**
   * Returns a 2 byte qualifier with the offset encoded and flags set.
   * @param offset An offset from [ 0, 3599 ].
   * @param flags A flag encoding the value length and type.
   * @return A non-null byte.
   * @throws IllegalArgumentException if the offset was out of bounds.
   */
  public static byte[] buildSecondQualifier(final long offset, 
                                            final short flags) {
    if (offset < 0 || offset > 3599) {
      throw new IllegalArgumentException("Offset cannot be less than "
          + "zero or greater than 3599");
    }
    return Bytes.fromShort((short) ((offset << FLAG_BITS) | flags));
  }
    
  /**
   * Returns the offest in nanoseconds from the base time.
   * @param qualifier The non-null and non-empty qualifier of at least
   * 8 bytes.
   * @param offset A zero based index within the qualifier.
   * @return The offset in nanoseconds.
   * @throws NullPointerException if the qualifier is null.
   * @throws ArrayIndexOutOfBoundsException if the offset was out of 
   * bounds.
   */
  public static long offsetFromNanoQualifier(final byte[] qualifier, 
                                             final int offset) {
    return Bytes.getLong(qualifier, offset) >>> FLAG_BITS & NS_MASK;
  }
  
  /**
   * Returns the offest in nanoseconds from the base time.
   * @param qualifier The non-null and non-empty qualifier of at least
   * 8 bytes.
   * @param offset A zero based index within the qualifier.
   * @return The offset in nanoseconds.
   * @throws NullPointerException if the qualifier is null.
   * @throws ArrayIndexOutOfBoundsException if the offset was out of 
   * bounds.
   */
  public static long offsetFromMsQualifier(final byte[] qualifier, 
                                           final int offset) {
    return ((Bytes.getUnsignedInt(qualifier, offset) & MS_MASK) 
        >>> MS_FLAG_BITS) * 1000 * 1000;
  }
  
  /**
   * Returns the offest in nanoseconds from the base time.
   * @param qualifier The non-null and non-empty qualifier of at least
   * 8 bytes.
   * @param offset A zero based index within the qualifier.
   * @return The offset in nanoseconds.
   * @throws NullPointerException if the qualifier is null.
   * @throws ArrayIndexOutOfBoundsException if the offset was out of 
   * bounds.
   */
  public static long offsetFromSecondQualifier(final byte[] qualifier, 
                                               final int offset) {
    return ((Bytes.getUnsignedShort(qualifier, offset) & S_MASK) 
        >>> FLAG_BITS) * 1000L * 1000L * 1000L;
  }
  
  /**
   * Returns the length of the value, in bytes, parsed from the last
   * byte of the qualifier
   * @param qualifier The qualifier to parse
   * @return The length of the value in bytes, from 1 to 8.
   * @throws NullPointerException if the qualifier is null.
   * @throws ArrayIndexOutOfBoundsException if the offset was out of 
   * bounds.
   */
  public static byte getValueLengthFromQualifier(final byte[] qualifier) {
    return getValueLengthFromQualifier(qualifier, qualifier.length - 1);
  }
  
  /**
   * Returns the length of the value, in bytes, parsed from the qualifier
   * at the given offset.
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array where the length is
   * encoded. E.g. if it's a 2 byte qualifier, set offset = 1.
   * @return The length of the value in bytes, from 1 to 8.
   * @throws NullPointerException if the qualifier is null.
   * @throws ArrayIndexOutOfBoundsException if the offset was out of 
   * bounds.
   */
  public static byte getValueLengthFromQualifier(final byte[] qualifier, 
                                                 final int offset) {
    return (byte) ((qualifier[offset] & LENGTH_MASK) + 1);
  }
  
  /**
   * Encodes a long on 1, 2, 4 or 8 bytes
   * @param value The value to encode
   * @return A byte array containing the encoded value
   */
  public static byte[] vleEncodeLong(final long value) {
    if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
      return new byte[] { (byte) value };
    } else if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
      return Bytes.fromShort((short) value);
    } else if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE) {
      return Bytes.fromInt((int) value);
    } else {
      return Bytes.fromLong(value);
    }
  }
  
  /**
   * Retrieves the flags byte from an encoded offsets array.
   * @param offsets An non-null array of time offsets.
   * @param offset A zero based offset within the offsets array.
   * @param encoded_on The number of bytes the offset uses for encoding.
   * @return A byte representing containing the encoding flags.
   * @throws IllegalArgumentException if the offsets array was null or the 
   * offset and encoded_on parameters exceeded the size of the array, the offset
   * was negative or the encoded_on was out of range.
   */
  public static byte getFlags(final byte[] offsets, 
                              final int offset, 
                              final byte encoded_on) {
    if (offsets == null) {
      throw new IllegalArgumentException("Offsets array cannot be null");
    }
    if (offset < 0) {
      throw new IllegalArgumentException("Offset cannot be negative.");
    }
    if (encoded_on < 1 || encoded_on > 8) {
      throw new IllegalArgumentException("Encoded on cannot be less than 1 "
          + "or greater than 8.");
    }
    if (offset + (encoded_on - 1) >= offsets.length) {
      throw new IllegalArgumentException("Offset + encoding exceeds "
          + "offsets length.");
    }
    return (byte) (offsets[offset + (encoded_on - 1)]);
  }
  
  /**
   * Applies the {@link #VALUE_LENGTH_MASK} to the flags and returns the
   * length of the value in bytes (from 1 to 8).
   * @param flags The value flags to apply the mask to.
   * @return The length of encoded value in bytes.
   */
  public static byte getValueLength(final byte flags) {
    return (byte) ((byte) (flags & LENGTH_MASK) + 1);
  }
  
  /**
   * Determines the number of bytes (1 to 8) required to encode the given
   * time span offsets while reserving a number of bits for encoding parameters. 
   * The span can be in any time unit but must be a positive value. Reserved
   * bits are the LSB.
   * @param span A positive span width.
   * @param reserved The number of bits to reserve for encoding.
   * @return The number of bytes required to encode the span offsets.
   * @throws IllegalArgumentException if the span was too large to fit in 
   * 8 bytes with the given reserved bits or if the span was less than zero.
   */
  public static byte encodeOn(final long span, final int reserved) {
    if (span < 1) {
      throw new IllegalArgumentException("Span cannot be negative.");
    }
    if (reserved < 1) {
      throw new IllegalArgumentException("Can't use this method if you don't "
          + "need to reserve any bits.");
    }
    long mask = Bytes.getLong(FULL_8_BYTES);
    mask = mask << 64 - reserved;
    if ((span & mask) != 0) {
      throw new IllegalArgumentException("Span was too large to encode: " 
          + span + " Need at least " + reserved + " bits free.");
    }
    byte encode_on = 8;
    while (encode_on > 0) {
      if ((span & mask) != 0) {
        return (byte) (encode_on + 1);
      }
      encode_on--;
      mask = Bytes.getLong(FULL_8_BYTES);
      mask = mask << ((8 * encode_on) - reserved);
    }
    return (byte) (encode_on + 1);
  }
  
  /**
   * Extracts the value of a cell containing a data point.
   * @param value The contents of a cell in HBase.
   * @param value_idx The offset inside {@code values} at which the value
   * starts.
   * @param flags The flags for this value.
   * @return The value of the cell.
   * @throws IllegalDataException if the data is malformed and not aligned
   * on 8/4/2/1 bytes.
   */
  public static long extractIntegerValue(final byte[] values,
                                         final int value_idx,
                                         final byte flags) {
    switch (flags & LENGTH_MASK) {
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
   * @throws IllegalDataException if the data is malformed and not aligned on
   * 4 or 8 bytes.
   */
  public static double extractFloatingPointValue(final byte[] values,
                                          final int value_idx,
                                          final byte flags) {
    switch (flags & LENGTH_MASK) {
      case 7: return Double.longBitsToDouble(Bytes.getLong(values, value_idx));
      case 3: return Float.intBitsToFloat(Bytes.getInt(values, value_idx));
    }
    throw new IllegalDataException("Floating point value @ " + value_idx
                                   + " not on 8 or 4 bytes in "
                                   + Arrays.toString(values));
  }
  
  /**
   * Helper to encode a long value into an appended qualifier + value 
   * byte array. Uses VLE encoding.
   * @param resolution The resolution to encode the offset on, assumes
   * that the offset is already in the proper resolution.
   * @param offset The offset at the appropriate resolution.
   * @param value The value to encode.
   * @return A byte array.
   */
  public static byte[] encodeAppendValue(final OffsetResolution resolution, 
                                         final long offset, 
                                         final long value) {
    final byte[] encoded_value = vleEncodeLong(value);
    return encodeAppendValue(resolution, offset, encoded_value, false);
  }
  
  /**
   * Helper to encode a floating point value (4 bytes) into an appended
   * qualifier + value byte array.
   * @param resolution The resolution to encode the offset on, assumes
   * that the offset is already in the proper resolution.
   * @param offset The offset at the appropriate resolution.
   * @param value The value to encode.
   * @return A byte array.
   */
  public static byte[] encodeAppendValue(final OffsetResolution resolution, 
                                         final long offset, 
                                         final float value) {
    final byte[] encoded_value = Bytes.fromInt(Float.floatToRawIntBits(value));
    return encodeAppendValue(resolution, offset, encoded_value, true);
  }
  
  /**
   * Helper to encode a floating point value (8 bytes) into an appended
   * qualifier + value byte array.
   * @param resolution The resolution to encode the offset on, assumes
   * that the offset is already in the proper resolution.
   * @param offset The offset at the appropriate resolution.
   * @param value The value to encode.
   * @return A byte array.
   */
  public static byte[] encodeAppendValue(final OffsetResolution resolution, 
                                         final long offset, 
                                         final double value) {
    final byte[] encoded_value = Bytes.fromLong(Double.doubleToRawLongBits(value));
    return encodeAppendValue(resolution, offset, encoded_value, true);
  }
  
  private static byte[] encodeAppendValue(final OffsetResolution resolution, 
                                          final long offset, 
                                          final byte[] value,
                                          final boolean is_float) {
    final byte[] new_value;
    short flag = (short) ((is_float ? FLAG_FLOAT : (short) 0) | (short) (value.length - 1));
    switch (resolution) {
    case NANOS:
      new_value = new byte[NS_Q_WIDTH + value.length];
      System.arraycopy(buildNanoQualifier(offset, flag), 0, new_value, 0, NS_Q_WIDTH);
      break;
    case MILLIS:
      new_value = new byte[MS_Q_WIDTH + value.length];
      System.arraycopy(buildMsQualifier(offset, flag), 0, new_value, 0, MS_Q_WIDTH);
      break;
    case SECONDS:
      new_value = new byte[S_Q_WIDTH + value.length];
      System.arraycopy(buildSecondQualifier(offset, flag), 0, new_value, 0, S_Q_WIDTH);
      break;
    default:
      throw new IllegalArgumentException("Unhandled resolution type: " 
          + resolution);
    }
    
    System.arraycopy(value, 0, new_value, new_value.length - value.length, value.length);
    return new_value;
  }
  
  /**
   * <b>HELPER</b> Method only for testing to decode an append column.
   * @param base_seconds The base timestamp in unix epoch seconds.
   * @param value The value array.
   * @param offset An offset into the value array.
   * @return A numeric type value.
   */
  public static TimeSeriesValue<NumericType> valueFromAppend(
      final long base_seconds, 
      final byte[] value, 
      final int offset) {
    final long time_offset;
    final byte flags;
    final int value_idx;
    if ((value[offset] & NS_BYTE_FLAG) == NS_BYTE_FLAG) {
      time_offset = offsetFromNanoQualifier(value, offset);
      flags = getFlags(value, offset, (byte) NS_Q_WIDTH);
      value_idx = offset + NS_Q_WIDTH;
    } else if ((value[offset] & MS_BYTE_FLAG) == MS_BYTE_FLAG) {
      time_offset = offsetFromMsQualifier(value, offset);
      flags = getFlags(value, offset, (byte) MS_Q_WIDTH);
      value_idx = offset + MS_Q_WIDTH;
    } else {
      time_offset = offsetFromSecondQualifier(value, offset);
      flags = getFlags(value, offset, (byte) S_Q_WIDTH);
      value_idx = offset +  S_Q_WIDTH;
    }
    
    final boolean is_integer;
    if ((flags & FLAG_FLOAT) == FLAG_FLOAT) {
      is_integer = false;
    } else {
      is_integer = true;
    }
    
    class LocalValue implements TimeSeriesValue<NumericType>, NumericType {

      final TimeStamp timestamp;
      
      LocalValue() {
        final long seconds_offset = (time_offset / 1000L / 1000L / 1000L); 
        final long epoch = base_seconds + seconds_offset;
        final long nanos = time_offset - (seconds_offset * 1000L * 1000L * 1000L);
        timestamp = new ZonedNanoTimeStamp(epoch, nanos, Const.UTC);
      }
      
      @Override
      public boolean isInteger() {
        return is_integer;
      }

      @Override
      public long longValue() {
        if (!is_integer) {
          throw new IllegalDataException("This is not an integer!");
        }
        return extractIntegerValue(value, value_idx, flags);
      }

      @Override
      public double doubleValue() {
        if (is_integer) {
          throw new IllegalDataException("This is not a float!");
        }
        return extractFloatingPointValue(value, value_idx, flags);
      }

      @Override
      public double toDouble() {
        if (is_integer) {
          return (double) longValue();
        }
        return doubleValue();
      }

      @Override
      public TimeStamp timestamp() {
        return timestamp;
      }

      @Override
      public NumericType value() {
        return this;
      }

      @Override
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
      }
      
      @Override
      public String toString() {
        final StringBuilder buf = new StringBuilder("timestamp={")
            .append(timestamp)
            .append("}, isInteger=")
            .append(is_integer)
            .append(", value=");
        if (is_integer) {
          buf.append(longValue());
        } else {
          buf.append(doubleValue());
        }
        return buf.toString();
      }
    }
    return new LocalValue();
  }


}
