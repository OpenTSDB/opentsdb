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
package net.opentsdb.query.execution;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.junit.Before;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.opentracing.Span;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.pojo.TimeSeriesQuery;

public class BaseExecutorTest {
  
  protected DefaultTSDB tsdb;
  protected QueryContext context;
  protected DefaultRegistry registry;
  protected ExecutionGraph graph;
  protected Timer timer;
  protected Timeout timeout;
  protected Span span;
  
  protected TimeSeriesQuery query;
  protected ExecutionGraphNode node;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(DefaultTSDB.class);
    context = mock(QueryContext.class);
    registry = mock(DefaultRegistry.class);
    graph = mock(ExecutionGraph.class);
    timer = mock(Timer.class);
    timeout = mock(Timeout.class);
    span = mock(Span.class);
    
    when(context.getTimer()).thenReturn(timer);
    when(context.getTSDB()).thenReturn(tsdb);
    when(timer.newTimeout(any(TimerTask.class), anyLong(), 
        eq(TimeUnit.MILLISECONDS))).thenReturn(timeout);
    when(graph.tsdb()).thenReturn(tsdb);
    when(tsdb.getRegistry()).thenReturn(registry);
  }
  
  public static class MockExecutionGraph extends ExecutionGraph {

    protected MockExecutionGraph(final Builder builder) {
      super(builder);
    }
    
    public static class Builder extends ExecutionGraph.Builder {
      
    }
  }
}
