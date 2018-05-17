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

import java.time.temporal.ChronoUnit;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;

/**
 * Represents a read-only sequence of continuous columns.
 * <p>
 * This class stores in memory the data of one or more continuous
 * storage columns for a given time series. To consolidate memory, the 
 * data points are stored in one byte arrays in the format following
 * TSDB 2.0 appends with a column qualifier followed by a column value
 * then the next qualifier, next value, etc. Access is granted via pointers.
 * 
 * @since 3.0
 */
public interface RowSeq {

  /** @return The data type handled by this sequence. */
  public TypeToken<? extends TimeSeriesDataType> type();
  
  /**
   * Simply appends the value to the data array in the append column
   * format.
   * <b>NOTE:</b> Since this is in the fast path we don't validate the
   * qualifier and value for length and data. Please do that before
   * calling.
   * 
   * @param prefix A prefix of either 0 or {@link Schema#APPENDS_PREFIX}.
   * @param qualifier A non-null and non-empty qualifier.
   * @param value A non-null and non-empty value.
   */
  public void addColumn(final byte prefix, 
                        final byte[] qualifier, 
                        final byte[] value);
  
  /**
   * Iterates over the results, checking for out-of-order or duplicate
   * values. Assumes the data is in time ascending order and will 
   * re-order when called.
   * @param keep_earliest True to keep the first data point recorded via
   * {@link #addColumn(byte, byte[], byte[])} or false to keep the
   * last value recorded.
   * @param reverse Whether or not the result should be in time 
   * descending order.
   * @return The highest resolution timestamp for this sequence.
   */
  public ChronoUnit dedupe(final boolean keep_earliest, final boolean reverse);
  
  /** @return The size of this object in bytes, including header. */
  public int size();
  
  /** @return A rough estimate of the number of values in this row. */
  public int dataPoints();
}
