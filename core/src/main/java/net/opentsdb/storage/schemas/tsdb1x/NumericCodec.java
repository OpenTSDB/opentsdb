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

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.utils.Bytes;

/**
 * TODO - doc me and finish me
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
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public StorageSeries newIterable() {
    // TODO Auto-generated method stub
    return null;
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
  
}
