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
 * An implementation agnostic interface for tracing OpenTSDB code.
 * 
 * @since 3.0
 */
public interface Tracer {

  /**
   * Returns a new span builder for this tracer.
   * @param id A non-null and non-empty span ID.
   * @return A non-null span builder.
   */
  public SpanBuilder newSpan(final String id);
  
  /**
   * Whether or not this tracer is in debug mode and should record detailed
   * information.
   * @return True if in debug mode, false if not.
   */
  public boolean isDebug();
}