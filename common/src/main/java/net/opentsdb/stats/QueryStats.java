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
  public Tracer tracer();

  /**
   * @return The optional upstream query Span for tracing. May be null if tracing
   * is disabled. If the span is set, then {@link #tracer()} <i>must</i> return
   * a non-null tracer.
   */
  public Span querySpan();
}