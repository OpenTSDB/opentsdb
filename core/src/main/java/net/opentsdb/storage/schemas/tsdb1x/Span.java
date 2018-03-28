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

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;

/**
 * A collection of ordered {@link RowSeq}s that can be iterated.
 * 
 * @param <T> The type of data.
 * 
 * @since 3.0
 */
public interface Span<T extends TimeSeriesDataType> extends 
    Iterable<TimeSeriesValue<? extends TimeSeriesDataType>> {

  /**
   * Adds the sequence to the row list if the row time is greater than
   * the previous time and the row has data. If the row's data array is
   * null or empty, the row is skipped.
   * 
   * @param sequence A non-null and non-empty row sequence.
   * @param keep_earliest Whether or not to keep the earliest or latest
   * values.
   * @throws IllegalArgumentException if the sequence was null.
   * @throws IllegalStateException if the row was out of order.
   */
  public void addSequence(final RowSeq sequence, 
                          final boolean keep_earliest);
}
