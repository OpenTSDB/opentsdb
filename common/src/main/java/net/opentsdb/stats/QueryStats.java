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
package net.opentsdb.stats;

/**
 * A collector for statistics about a particular query including tracing and
 * various measurements.
 * TODO - more coming soon.
 * 
 * @since 3.0
 */
public interface QueryStats {

  /**
   * @return An optional tracer to use for the query. May be null if tracing
   * is disabled. If the value is not null, {@link #querySpan()} <i>must</i>
   * return a non-null span.
   */
  public Trace trace();

  /**
   * @return The optional upstream query Span for tracing. May be null if tracing
   * is disabled. If the span is set, then {@link #trace()} <i>must</i> return
   * a non-null tracer.
   */
  public Span querySpan();
}
