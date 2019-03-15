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
 * An encoding of timestamp and numeric values in a {@link long[]} for better
 * cache usage so that we're working on a vector instead of iterators. This is
 * 15% more efficient than the old iterative method and 4% more efficient than 
 * having an iterator on top of this array.
 * <p>
 * The format is an array of long primitives. The first entry is a 4 bit header 
 * and full epoch timestamp in seconds or milliseconds. The second entry is
 * either a nanosecond offset if the timestamp is in nanoseconds or the value
 * as either a raw long or a long encoded double precision value.
 * The flags are:
 * 64 - 0 == raw long, 1 == double value
 * 63 + 62 - 00 == seconds, 10 == milliseconds, 01 == nanoseconds
 * 61 - terminal entry. 
 * 
 * TODO - probably a better way. E.g. we can do the delta of delta of timestamps
 * like Gorilla though we need the external ref and another calculation. Gotta
 * bench that for cache hits/misses, etc.
 * 
 * TODO - may default to this and re-do the data types. Though downsampled is 
 * still faster since we don't deal with timestamps.
 * 
 * @since 3.0
 */
public interface NumericLongArrayType extends TimeSeriesDataType {
  public static final TypeToken<NumericLongArrayType> TYPE = 
      TypeToken.of(NumericLongArrayType.class);
  
  /** Flag set when the timestamp is in milliseconds instead of seconds or 
   * nanoseconds. */
  public static final long MILLISECOND_FLAG = 0x4000000000000000L;
  
  /** Flag set when the timestamp is in nanoseconds in which case the next 
   * long is the nanos offset. */
  public static final long NANOSECOND_FLAG = 0x2000000000000000L;
  
  /** Flag set when the long value is an encoded double. Otherwise it's a 
   * straight long. */
  public static final long FLOAT_FLAG = 0x8000000000000000L;
  
  /** Indicates this is the last long in the set. */
  public static final long TERIMNAL_FLAG = 0x1000000000000000L;
  
  /** A mask to zero out the flag bits before reading the timestamp. */
  public static final long TIMESTAMP_MASK = 0xFFFFFFFFFFFFFFFL;
  
  @Override
  default TypeToken<? extends TimeSeriesDataType> type() {
    return TYPE;
  }
  
}