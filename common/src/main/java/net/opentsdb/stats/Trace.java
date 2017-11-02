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

import net.opentsdb.stats.Span.SpanBuilder;

/**
 * An implementation agnostic interface for tracing OpenTSDB code. This is the
 * interface for a specific trace (collection of calls). A {@link Tracer} must
 * emit traces.
 * 
 * @since 3.0
 */
public interface Trace {

  /**
   * Returns a new span builder for this tracer without any tags.
   * @param id A non-null and non-empty span ID.
   * @return A non-null span builder.
   * @throws IllegalArgumentException if a required argument was invalid.
   */
  public SpanBuilder newSpan(final String id);
  
  /**
   * Returns a new span builder for this tracer with the given tags set.
   * @param id A non-null and non-empty span ID.
   * @param tags A list of key/value pairs. Must be an even number of non-null
   * and non-empty strings.
   * @return A non-null span builder.
   * @throws IllegalArgumentException if a required argument was invalid.
   */
  public SpanBuilder newSpan(final String id, final String... tags);
  
  /**
   * Returns a new span builder for this tracer with the thread name set as in
   * "startThread=&lt;local_thread_name&gt;".
   * @param id A non-null and non-empty span ID.
   * @return A non-null span builder.
   * @throws IllegalArgumentException if a required argument was invalid.
   */
  public SpanBuilder newSpanWithThread(final String id);
  
  /**
   * Returns a new span builder for this tracer with the given tags set and the
   * thread name set as in "startThread=&lt;local_thread_name&gt;".
   * @param id A non-null and non-empty span ID.
   * @param tags A list of key/value pairs. Must be an even number of non-null
   * and non-empty strings.
   * @return A non-null span builder.
   * @throws IllegalArgumentException if a required argument was invalid.
   */
  public SpanBuilder newSpanWithThread(final String id, final String... tags);
  
  /**
   * Whether or not this tracer is in debug mode and should record detailed
   * information.
   * @return True if in debug mode, false if not.
   */
  public boolean isDebug();
  
  /**
   * @return The trace ID as a hexadecimal string (to accommodate 128 bit IDs).
   * @throws IllegalStateException if the {@link #firstSpan()} hasn't been set 
   * yet.
   */
  public String traceId();
  
  /**
   * @return The first span of the trace if set. May be null if no span has been 
   * recorded yet.
   */
  public Span firstSpan();
}