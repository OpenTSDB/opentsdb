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

import java.util.List;

import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.stats.Span;

/**
 * The interface used for implementing a data store that can accept 
 * writes via the OpenTSDB APIs.
 * 
 * @since 3.0
 */
public interface WritableTimeSeriesDataStore {
  
  /**
   * Writes the given value to the data store.
   * @param state A required state to use for authorization, filtering 
   * and routing.
   * @param datum A single value.
   * @param span An optional span for tracing.
   * @return A deferred resolving to a WriteState.
   */
  public Deferred<WriteStatus> write(final AuthState state, 
                                    final TimeSeriesDatum datum, 
                                    final Span span);
  
  /**
   * Writes the given value to the data store.
   * @param state A required state to use for authorization, filtering 
   * and routing.
   * @param data A set of values to store.
   * @param span An optional span for tracing.
   * @return A deferred resolving to a list of WriteStates in the same
   * same order and number as the entries in the data iterator.
   */
  public Deferred<List<WriteStatus>> write(final AuthState state, 
                                          final TimeSeriesSharedTagsAndTimeData data, 
                                          final Span span);
  
}
