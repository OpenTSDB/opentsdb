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
package net.opentsdb.data.types.numeric;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;

/**
 * TODO - scratch work for now when we have a normalized timeseries.
 * 
 * <b>WARNING:</b> It should go without saying. DON'T edit values in the
 * arrays.
 * 
 * Can only be used in conjunction with a downsampled time specification.
 * 
 * Since this can be returned in a value iterator, the timestamp returned
 * should be that of the first value. The rest of the timestamps are
 * computed from the time specification.
 * 
 * @since 3.0
 */
public interface NumericArrayType extends TimeSeriesDataType {

  /** The data type reference to pass around. */
  public static final TypeToken<NumericArrayType> TYPE = 
      TypeToken.of(NumericArrayType.class);
  
  @Override
  default TypeToken<? extends TimeSeriesDataType> type() {
    return TYPE;
  }
  
  /**
   * An index into the array that determines the start, inclusive, of 
   * valid data. Usually set to zero.
   * @return A zero based index into the array.
   */
  public int offset();
  
  /**
   * An index into or outside of the array, exclusive, of valid data.
   * For use in a for loop, e.g.
   * {@code for (int i = offset(); i < end(); i++)...}
   * @return The exclusive index to iterate until.
   */
  public int end();
  
  /**
   * Tells whether or not the array is of integer values, in which case
   * call {@link #longArray()}, or if it's a double array so call on
   * {@link #doubleArray()}.
   */
  public boolean isInteger();
  
  /**
   * The array of long values when {@link #isInteger()} returns true.
   * @return A non-null array of length zero or more when 
   * {@link #isInteger()} is true, null if false.
   */
  public long[] longArray();
  
  /**
   * The array of double values with {@link #isInteger()} return fasel. 
   * @return A non-null array of length zero or more when 
   * {@link #isInteger()} is false, null if true.
   */
  public double[] doubleArray();
}