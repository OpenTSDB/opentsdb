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

import com.google.common.collect.Lists;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.opentracing.Span;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.stats.StatsCollector;

public class BaseExecutorTest {
  
  protected DefaultTSDB tsdb;
  protected QueryContext context;
  protected QueryPipelineContext pcontext;
  protected DefaultRegistry registry;
  protected ExecutionGraph graph;
  protected Timer timer;
  protected Timeout timeout;
  protected Span span;
  protected QueryNode upstream;
  protected QueryNode downstream;
  protected TimeSeriesDataSource source;
  
  protected TimeSeriesQuery query;
  protected ExecutionGraphNode node;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(DefaultTSDB.class);
    context = mock(QueryContext.class);
    pcontext = mock(QueryPipelineContext.class);
    registry = mock(DefaultRegistry.class);
    graph = mock(ExecutionGraph.class);
    timer = mock(Timer.class);
    timeout = mock(Timeout.class);
    span = mock(Span.class);
    upstream = mock(QueryNode.class);
    downstream = mock(QueryNode.class);
    source = mock(TimeSeriesDataSource.class);
    
    when(context.getTimer()).thenReturn(timer);
    when(context.getTSDB()).thenReturn(tsdb);
    when(timer.newTimeout(any(TimerTask.class), anyLong(), 
        eq(TimeUnit.MILLISECONDS))).thenReturn(timeout);
    when(tsdb.getRegistry()).thenReturn(registry);
    when(tsdb.getStatsCollector()).thenReturn(mock(StatsCollector.class));
    when(pcontext.query()).thenReturn(query);
    when(pcontext.downstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(downstream));
    when(pcontext.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
    when(pcontext.downstreamSources(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(source));
  }
  
  public static class MockExecutionGraph extends ExecutionGraph {

    protected MockExecutionGraph(final Builder builder) {
      super(builder);
    }
    
    public static class Builder extends ExecutionGraph.Builder {
      
    }
  }
}
