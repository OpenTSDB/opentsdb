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
 * A simple default implementation of the Query Stats object. It simply takes a
 * trace for now. It will start a new span if the trace is not null.
 * 
 * TODO - flesh this out as we flesh out the stats interface.
 * 
 * @since 3.0
 */
public class DefaultQueryStats implements QueryStats {
  private final Trace trace;
  private final Span query_span;
  
  /**
   * Protected ctor.
   * @param builder A non-null builder.
   */
  DefaultQueryStats(final Builder builder) {
    trace = builder.trace;
    if (trace != null) {
      query_span = trace.newSpan("OpenTSDB Query")
          .start();
    } else {
      query_span = null;
    }
  }
  
  @Override
  public Trace trace() {
    return trace;
  }
  
  @Override
  public Span querySpan() {
    return query_span;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Builder for the {@link DefaultQueryStats} class.
   */
  public static class Builder {
    private Trace trace;
    
    /**
     * @param trace An optional trace.
     * @return The builder.
     */
    public Builder setTrace(final Trace trace) {
      this.trace = trace;
      return this;
    }
    
    /** @return An instantiated {@link DefaultQueryStats} object. */
    public QueryStats build() {
      return new DefaultQueryStats(this);
    }
  }
  
}
