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
 * Class used for testing pipelines with a mock stats collector.
 */
public class MockStats implements QueryStats {
  private Trace tracer;
  private Span query_span;
  
  /**
   * Default ctor.
   * @param tracer The mock tracer, may be null.
   * @param parent_span The mock parent span, may be null.
   */
  public MockStats(final Trace tracer, final Span parent_span) {
    this.tracer = tracer;
    query_span = parent_span;
  }
  
  @Override
  public Trace trace() {
    return tracer;
  }

  @Override
  public Span querySpan() {
    return query_span;
  }

}