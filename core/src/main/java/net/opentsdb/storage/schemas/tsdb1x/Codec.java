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
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.utils.Pair;

/**
 * A class that will return a storage object that can be populated
 * by the storage system with data. It's the shim between a
 * TimeSeries entry and storage. Codecs for each type of data handled
 * by a schema should be registered in the schema.
 * 
 * @since 3.0
 */
public interface Codec {

  /** @return The non-null token refering to the type of data returned
   * by this codec. */
  public TypeToken<? extends TimeSeriesDataType> type();
  
  /**
   * Instantiates a new sequences (span) object for rows of the data type
   * this codec handles.
   * @param reversed Whether or not the data will be reversed.
   * @return A non-null sequences object.
   */
  public Span<? extends TimeSeriesDataType> newSequences(
      final boolean reversed);

  /**
   * Instantiates a new row sequence for the data type.
   * @param base_time The Unix Epoch row base time in seconds.
   * @return A new row sequence.
   */
  public RowSeq newRowSeq(final long base_time);

  /**
   * Encodes the given value into a qualifier and value to send to the
   * key/value column store.  
   * @param value A non-null value to encode.
   * @param append_format Whether or not to generate the append format.
   * @param base_time The base time in Unix epoch seconds.
   * @param rollup_interval An optional rollup interval.
   * @return A non-null pair where the key is the qualifier and the value
   * is the column value.
   */
  public Pair<byte[], byte[]> encode(
      final TimeSeriesValue<? extends TimeSeriesDataType> value,
      final boolean append_format,
      final int base_time,
      final RollupInterval rollup_interval);
}
