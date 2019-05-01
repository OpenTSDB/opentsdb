// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;

/**
 * For summaries we'll do:
 * <8B timestamp><1B num following values><1B type><1B flags><nB value>...[repeat]
 * 
 * It's a first draft. We'll see how it performs.
 * 
 * @since 3.0
 */
public interface NumericByteArraySummaryType extends TimeSeriesDataType {
  public static final TypeToken<NumericByteArraySummaryType> TYPE = 
      TypeToken.of(NumericByteArraySummaryType.class);
  
  /** Flag set when the timestamp is in milliseconds instead of seconds or 
   * nanoseconds. */
  public static final long MILLISECOND_FLAG = 0x4000000000000000L;
  
  /** Flag set when the timestamp is in nanoseconds in which case the next 
   * long is the nanos offset. */
  public static final long NANOSECOND_FLAG = 0x2000000000000000L;
  
  /** Flag set when the long value is an encoded double. Otherwise it's a 
   * straight long. */
  public static final byte FLOAT_FLAG = (byte) 0x80;
  
  /** A mask to zero out the flag bits before reading the timestamp. */
  public static final long TIMESTAMP_MASK = 0xFFFFFFFFFFFFFFFL;
  
  @Override
  default TypeToken<? extends TimeSeriesDataType> type() {
    return TYPE;
  }
  
  /**
   * The starting offset into the array where data begins for this value. Used
   * for shared arrays. For non-shared arrays this should always be zero.
   * @return The starting offset into the array.
   */
  public int offset();

  /**
   * The index into the array where data is no longer valid. E.g. for a 
   * while loop you'd write {code for (int i = offset(); i < end(); i++)}. So
   * stop reading at end() - 1.
   * @return The index into the array where data is no longer valid.
   */
  public int end();

  /**
   * The array of encoded data. This may be null or empty in which case 
   * {@link offset()} must equal {@link end()}.
   * @return The array of data, may be null or empty.
   */
  public long[] data();
  
}