// This file is part of OpenTSDB.
// Copyright (C) 2017-2019  The OpenTSDB Authors.
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

import net.opentsdb.query.QueryContext;

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
  
  /**
   * Called by the context to set the reference. Since we assign this to the 
   * context during a build, we don't have a context to set. So the context
   * <b>must</b> call this if we want any good stats.
   * @param context The non-null context to set.
   */
  public void setQueryContext(final QueryContext context);
  
  /**
   * Called to emit the stats collected by this object.
   */
  public void emitStats();
  
  public void incrementRawDataSize(final long size);
  
  public void incrementSerializedDataSize(final long size);
  
  public void incrementRawTimeSeriesCount(final long count);
  
  public void incrementSerializedTimeSeriesCount(final long count);
  
  public long rawDataSize();
  
  public long serializedDataSize();
  
  public long rawTimeSeriesCount();
  
  public long serializedTimeSeriesCount();
  
}
