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
package net.opentsdb.query.filter;

import com.stumbleupon.async.Deferred;

import net.opentsdb.stats.Span;

/**
 * Base interface stub for a query filter class.
 * 
 * @since 3.0
 */
public interface QueryFilter {
  
  /** A statically initialized deferred in case the implementation has
   * nothing to do. */
  public static final Deferred<Void> INITIALIZED = 
      Deferred.fromResult(null);
  
  /** @return A name for the filter tied to a factory for lookups. */
  public String getType();
  
  /**
   * Called to initialize a filter, e.g. if it needs to make a call out 
   * to another service, do so here.
   * @param span An optional tracing span.
   * @return A deferred resolving to a null or an exception if something
   * went wrong with the initialization.
   */
  public Deferred<Void> initialize(final Span span);
}
