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

import net.opentsdb.query.QueryContext;

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

  @Override
  public void setQueryContext(QueryContext context) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void emitStats() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void incrementRawDataSize(long size) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void incrementSerializedDataSize(long size) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void incrementRawTimeSeriesCount(long count) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void incrementSerializedTimeSeriesCount(long count) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public long rawDataSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long serializedDataSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long rawTimeSeriesCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long serializedTimeSeriesCount() {
    // TODO Auto-generated method stub
    return 0;
  }

}
