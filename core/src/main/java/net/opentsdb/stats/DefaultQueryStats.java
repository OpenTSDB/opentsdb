// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
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
