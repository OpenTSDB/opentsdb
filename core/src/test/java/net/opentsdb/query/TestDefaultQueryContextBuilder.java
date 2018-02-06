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
package net.opentsdb.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.stats.MockStats;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.stats.QueryStats;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DefaultQueryContextBuilder.class })
public class TestDefaultQueryContextBuilder {
  private DefaultTSDB tsdb;
  private TSDBV2Pipeline pipeline_context;
  private TimeSeriesQuery query;
  private QuerySink sink;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(DefaultTSDB.class);
    pipeline_context = mock(TSDBV2Pipeline.class);
    query = mock(TimeSeriesQuery.class);
    sink = mock(QuerySink.class);
    
    PowerMockito.whenNew(TSDBV2Pipeline.class).withAnyArguments()
      .thenReturn(pipeline_context);
  }
  
  @Test
  public void buildWithoutStats() throws Exception {
    final QueryContext context = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(sink)
        .build();
    
    assertEquals(QueryMode.SINGLE, context.mode());
    assertSame(sink, context.sinks().iterator().next());
    assertNull(context.stats());
  }
  
  @Test
  public void buildWithStats() throws Exception {
    final MockTrace tracer = new MockTrace();
    final QueryStats stats = new MockStats(tracer, tracer.newSpan("mock").start());
    
    final QueryContext context = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(sink)
        .setStats(stats)
        .build();
    
    assertEquals(QueryMode.SINGLE, context.mode());
    assertSame(sink, context.sinks().iterator().next());
    assertSame(stats, context.stats());
    assertEquals(0, tracer.spans.size());
    context.close();
    assertEquals(1, tracer.spans.size());
  }
  
  @Test
  public void buildErrors() throws Exception {
    try {
      DefaultQueryContextBuilder.newBuilder(null)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(sink)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DefaultQueryContextBuilder.newBuilder(tsdb)
        //.setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(sink)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        //.setMode(QueryMode.SINGLE)
        .addQuerySink(sink)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        //.addQuerySink(sink)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
