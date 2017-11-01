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
