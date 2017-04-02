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

import com.fasterxml.jackson.core.JsonGenerator;

import io.opentracing.Span;
import io.opentracing.Tracer;

/**
 * A unique trace object for a given operation in OpenTSDB. IT contains a 
 * reference to the tracer used to generate spans as well as methods to
 * serialize the final results of the trace.
 * <p>
 * Callers receiving a new trace should store the first span via 
 * {@link #setFirstSpan(Span)}.
 * 
 * @since 3.0
 */
public abstract class TsdbTrace {
  
  /** A non-null tracer to generate spans from. */
  private final Tracer tracer;
  
  /** The first span in the (local) trace. */
  private Span first_span;
  
  /**
   * Default ctor that stores the tracer.
   * @param tracer A non-null tracer.
   * @throws IllegalArgumentException if the tracer was null.
   */
  public TsdbTrace(final Tracer tracer) {
    if (tracer == null) {
      throw new IllegalArgumentException("Tracer cannot be null.");
    }
    this.tracer = tracer;
  }
  
  /** @return The tracer that will generate spans. */
  public Tracer tracer() {
    return tracer;
  }
  
  /** @param span The first span of the trace. */
  public void setFirstSpan(final Span span) {
    this.first_span = span;
  }
  
  /** @return The first span of the trace if set. */
  public Span getFirstSpan() {
    return first_span;
  }
  
  /**
   * Serialize the trace as JSON using the given JsonGenerator. The name is 
   * applied to the array or object.
   * @param name A non-null name to apply to the array or object.
   * @param json A non-null generator to write to.
   */
  public abstract void serializeJSON(final String name, final JsonGenerator json);
  
  /** @return The trace as a string. */
  public abstract String serializeToString();
}
