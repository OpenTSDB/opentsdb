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
package net.opentsdb.storage;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.stats.Span;

/**
 * The base interface used to implement a storage schema, i.e. encoding
 * or decoding time series data.
 * 
 * @since 3.0
 */
public interface StorageSchema {

  /** @return A non-null and non-empty unique runtime ID for this storage
   * schema instance. Multiple schemas of the same type can be registered
   * at runtime. */
  public String id();
  
  /**
   * Resolve the given byte ID to the string ID if applicable.
   * @param id A non-null ID encoded with this schema.
   * @param span An optional tracing span.
   * @return A deferred resolving to the string ID or an exception if
   * something goes wrong.
   * @throws IllegalArgumentException if the ID was null or the ID was
   * encoded with a different schema.
   */
  public Deferred<TimeSeriesStringId> resolveByteId(final TimeSeriesByteId id, 
                                                    final Span span);
  
}
