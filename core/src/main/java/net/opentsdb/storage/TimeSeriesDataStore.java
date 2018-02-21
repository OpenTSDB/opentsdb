// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import com.stumbleupon.async.Deferred;

import io.opentracing.Span;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.stats.TsdbTrace;

/**
 * The class for reading or writing time series data to a local data store. 
 * <p>
 * This class is generally meant to implement a time series storage schema on
 * either:
 * <ul>
 * <li>A local system such as using flat files or an LSM implementation on the
 * local disk using something like RocksDB or LevelDB.<li>
 * <li>A remote distributed store such as HBase, Bigtable or Cassandra.<li>
 * </ul>
 *
 * TODO - more complete calls and documentation
 * 
 * @since 3.0
 */
public abstract class TimeSeriesDataStore extends BaseTSDBPlugin implements 
  QueryNodeFactory {
  
  /**
   * Writes the given value to the data store.
   * @param id A non-null ID for the value.
   * @param value A non-null value to write.
   * @param trace An optional tracer.
   * @param upstream_span An optional span for tracing.
   * @return A deferred resolving to null on success or an exception if the 
   * value was unable to be written.
   */
  public abstract Deferred<Object> write(final TimeSeriesStringId id,
                                         final TimeSeriesValue<?> value, 
                                         final TsdbTrace trace, 
                                         final Span upstream_span);
  
}
