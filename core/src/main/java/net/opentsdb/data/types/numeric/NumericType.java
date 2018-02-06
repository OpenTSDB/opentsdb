// This file is part of OpenTSDB.
// Copyright (C) 2010-2017  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric;

import java.util.Arrays;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.exceptions.IllegalDataException;
import net.opentsdb.utils.Bytes;

/**
 * Represents a single numeric data point.
 * <p>
 * Implementations of this interface aren't expected to be synchronized.
 */
public interface NumericType extends TimeSeriesDataType {

  /** The data type reference to pass around. */
  public static final TypeToken<NumericType> TYPE = TypeToken.of(NumericType.class);
  
  /** Number of LSBs in ms time offsets reserved for flags.  */
  public static final short TOTAL_FLAG_BITS = 7;
  
  /** Number of LSBs describing the length and type of value in offsets. */
  public static final short VALUE_FLAG_BITS = 4;
  
  /** A mask on the bits describing the reals value length in a compacted
   * real + value byte array. */
  public static final short REALS_LENGTH_MASK = 0x70;
  
  /** Mask to select the size of a value from the qualifier.  */
  public static final short VALUE_LENGTH_MASK = 0x7;
  
  /**
   * When this bit is set, the value is a floating point value.
   * Otherwise it's an integer value.
   */
  public static final short FLAG_FLOAT = 0x8;
  
  /** Mask to select all the FLAG_BITS in a reals length + values length/type
   * encoded offset.  */
  public static final short FLAGS_MASK = 
      FLAG_FLOAT | VALUE_LENGTH_MASK | REALS_LENGTH_MASK;
  
  /** Helper that fills a full 8 byte array with 1s. */
  public static final byte[] FULL_8_BYTES = new byte[] { (byte) 0xFF,  
      (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 
      (byte) 0xFF, (byte) 0xFF };
  
  /**
   * Tells whether or not the this data point is a value of integer type.
   * @return {@code true} if the value is of integer type (call {@link #longValue()}), 
   * {@code false} if it's of double point type (call {@link #doubleValue()}.
   */
  public boolean isInteger();

  /**
   * Returns the value of the this data point as a {@code long}.
   * @return The value as a long if {@link #isInteger()} was true.
   * @throws ClassCastException if the {@code isInteger() == false}.
   */
  public long longValue();

  /**
   * Returns the value of the this data point as a {@code double}.
   * @return The value as a double if {@link #isInteger()} was false.
   * @throws ClassCastException if the {@code isInteger() == true}.
   */
  public double doubleValue();

  /**
   * Returns the value of the this data point as a {@code double}, even if
   * it's a {@code long}.
   * @return When {@code isInteger() == false}, this method returns the same
   * thing as {@link #doubleValue}.  Otherwise, it returns the same thing as
   * {@link #longValue}'s return value casted to a {@code double}.
   */
  public double toDouble();

  /**
   * Encodes a signed long on 1, 2, 4 or 8 bytes.
   * @param value The value to encode.
   * @return A byte array containing the encoded value.
   * (used to be in net.opentsdb.core.Internal)
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
  public static byte getFlags(final byte[] offsets, final int offset, 
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
    return (byte) (offsets[offset + (encoded_on - 1)] & FLAGS_MASK);
  }
  
  /**
   * Applies the {@link #VALUE_LENGTH_MASK} to the flags and returns the
   * length of the value in bytes (from 1 to 8).
   * @param flags The value flags to apply the mask to.
   * @return The length of encoded value in bytes.
   */
  public static byte getValueLength(final byte flags) {
    return (byte) ((byte) (flags & VALUE_LENGTH_MASK) + 1);
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
  static long extractIntegerValue(final byte[] values,
                                  final int value_idx,
                                  final byte flags) {
    switch (flags & VALUE_LENGTH_MASK) {
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
  static double extractFloatingPointValue(final byte[] values,
                                          final int value_idx,
                                          final byte flags) {
    switch (flags & VALUE_LENGTH_MASK) {
      case 7: return Double.longBitsToDouble(Bytes.getLong(values, value_idx));
      case 3: return Float.intBitsToFloat(Bytes.getInt(values, value_idx));
    }
    throw new IllegalDataException("Floating point value @ " + value_idx
                                   + " not on 8 or 4 bytes in "
                                   + Arrays.toString(values));
  }
  
  /**
   * Returns true if the given string can fit into a float.
   * @param value The String holding the float value.
   * @return true if the value can fit into a float, false otherwise.
   * @throws NumberFormatException if the value is not numeric.
   */
  public static boolean fitsInFloat(final String value) {
    // TODO - probably still a better way to do this and we could save a lot
    // of space by dropping useless precision, but for now this should help. 
    final double d = Double.parseDouble(value);
    return ((float) d) == d;
  }
  
  /**
   * Returns true if the given double can fit into a float.
   * @param value The double to evaluate
   * @return true if the value can fit into a float, false otherwise.
   * @throws NumberFormatException if the value is not numeric.
   */
  public static boolean fitsInFloat(final double value) {
    // TODO - probably still a better way to do this and we could save a lot
    // of space by dropping useless precision, but for now this should help. 
    return ((float) value) == value;
  }

  /**
   * Returns true if the given string looks like an integer.
   * <p>
   * This function doesn't do any checking on the string other than looking
   * for some characters that are generally found in floating point values
   * such as '.' or 'e'.
   * @param value The value to validate.
   * @return True if it appears to be an integer, false if not.
   * @since 1.1
   */
  public static boolean looksLikeInteger(final String value) {
    final int n = value.length();
    for (int i = 0; i < n; i++) {
      final char c = value.charAt(i);
      if (c == '.' || c == 'e' || c == 'E') {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Parses an integer value as a long from the given character sequence.
   * <p>
   * This is equivalent to {@link Long#parseLong(String)} except it's up to
   * 100% faster on {@link String} and always works in O(1) space even with
   * {@link StringBuilder} buffers (where it's 2x to 5x faster).
   * @param s The character sequence containing the integer value to parse.
   * @return The value parsed.
   * @throws NumberFormatException if the value is malformed or overflows.
   * @since 1.0
   */
  public static long parseLong(final CharSequence s) {
    final int n = s.length();  // Will NPE if necessary.
    if (n == 0) {
      throw new NumberFormatException("Empty string");
    }
    char c = s.charAt(0);  // Current character.
    int i = 1;  // index in `s'.
    if (c < '0' && (c == '+' || c == '-')) {  // Only 1 test in common case.
      if (n == 1) {
        throw new NumberFormatException("Just a sign, no value: " + s);
      } else if (n > 20) {  // "+9223372036854775807" or "-9223372036854775808"
          throw new NumberFormatException("Value too long: " + s);
      }
      c = s.charAt(1);
      i = 2;  // Skip over the sign.
    } else if (n > 19) {  // "9223372036854775807"
      throw new NumberFormatException("Value too long: " + s);
    }
    long v = 0;  // The result (negated to easily handle MIN_VALUE).
    do {
      if ('0' <= c && c <= '9') {
        v -= c - '0';
      } else {
        throw new NumberFormatException("Invalid character '" + c
                                        + "' in " + s);
      }
      if (i == n) {
        break;
      }
      v *= 10;
      c = s.charAt(i++);
    } while (true);
    if (v > 0) {
      throw new NumberFormatException("Overflow in " + s);
    } else if (s.charAt(0) == '-') {
      return v;  // Value is already negative, return unchanged.
    } else if (v == Long.MIN_VALUE) {
      throw new NumberFormatException("Overflow in " + s);
    } else {
      return -v;  // Positive value, need to fix the sign.
    }
  }

}
