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

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.stats.Span;

/**
 * A time series implementation that parses the results of a storage
 * fetch and can be passed to a TimeSeries container for processing 
 * upstream.
 * <p>
 * This expects a column of data from a TSDB V1/2 schema. It's similar 
 * to the old {@code RowSeq}.
 * 
 * @since 3.0
 */
public interface StorageSeries extends Iterable<TimeSeriesValue<?>> {
  
  /**
   * Processes a column of data and loads it into the series.
   * @param base A non-null base timestamp from the row, used to compute
   * the value timestamp with the qualifier or value.
   * @param tsuid The non-null and non-empty TSUID for the row.
   * @param prefix A prefix pulled from the qualifier to assist in decoding
   * the column.
   * @param qualifier The non-null raw column qualifier.
   * @param value The non-null raw column value.
   * @param span An optional tracing span.
   */
  public void decode(final TimeStamp base, 
                     final byte[] tsuid, 
                     final byte prefix, 
                     final byte[] qualifier, 
                     final byte[] value,
                     final Span span);
  
}
