//This file is part of OpenTSDB.
//Copyright (C) 2018-2019  The OpenTSDB Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
package net.opentsdb.query.processor.summarizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.stats.Span;

public class TestSummarizerFactory {

  private static MockTSDB TSDB;
  private static TimeSeriesDataSource SRC_MOCK;
  private QueryPipelineContext context;
  private QueryNode ctx_node;
  private SummarizerConfig config;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    TSDB.registry = spy(new DefaultRegistry(TSDB));
    SRC_MOCK = mock(TimeSeriesDataSource.class);

    ((DefaultRegistry) TSDB.registry).initialize(true).join(60_000);
    TimeSeriesDataSourceFactory ts_factory = mock(TimeSeriesDataSourceFactory.class);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, null, ts_factory);
    when(ts_factory.newNode(any(QueryPipelineContext.class), any(QueryNodeConfig.class)))
      .thenReturn(SRC_MOCK);
    
    QueryNodeConfig config = mock(QueryNodeConfig.class);
    when(config.getId()).thenReturn("mock");
    when(SRC_MOCK.config()).thenReturn(config);
    when(SRC_MOCK.initialize(any(Span.class))).thenReturn(Deferred.fromResult(null));
  }
  
  @Before
  public void before() throws Exception {
    config = (SummarizerConfig) 
        SummarizerConfig.newBuilder()
        .setSummaries(Lists.newArrayList("sum", "avg"))
        .setInfectiousNan(true)
        .setId("summarizer")
        .build();
    context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    ctx_node = mock(QueryNode.class);
  }
  
  @Test
  public void ctor() throws Exception {
    SummarizerFactory factory = new SummarizerFactory();
    assertEquals(3, factory.types().size());
    assertTrue(factory.types().contains(NumericArrayType.TYPE));
    assertTrue(factory.types().contains(NumericType.TYPE));
    assertTrue(factory.types().contains(NumericSummaryType.TYPE));
    factory.initialize(mock(TSDB.class), null).join(1);
    assertEquals(SummarizerFactory.TYPE, factory.id());
  }
  
  @Test
  public void newIterator() throws Exception {
    final QueryNode node = mock(QueryNode.class);
    when(node.config()).thenReturn(config);
    final QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(node.pipelineContext()).thenReturn(context);
    
    SummarizerFactory factory = new SummarizerFactory();
    
    QueryNode summarizer = factory.newNode(context, config);
    assertTrue(summarizer instanceof Summarizer);
  }
  
  @Test
  public void setupNoPassThrough() throws Exception {
    SummarizerFactory factory = new SummarizerFactory();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(SummarizerConfig.newBuilder()
            .setSummaries(Lists.newArrayList("sum"))
            .addSource("m1")
            .setId("summary")
            .build())
        .build();
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);
    
    assertEquals(3, planner.graph().nodes().size());
    QueryNode node = planner.nodeForId("summary");
    assertFalse(((SummarizerConfig) node.config()).passThrough());
    assertEquals(1, planner.serializationSources().size());
    assertTrue(planner.serializationSources().contains("summary:m1"));
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
        planner.nodeForId("summary")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("summary"),
        planner.nodeForId("mock")));
  }
  
  @Test
  public void setupPassThrough() throws Exception {
    SummarizerFactory factory = new SummarizerFactory();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(SummarizerConfig.newBuilder()
            .setSummaries(Lists.newArrayList("sum"))
            .addSource("m1")
            .setId("summary")
            .build())
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("m1", "summary")))
        .build();
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);
    
    assertEquals(3, planner.graph().nodes().size());
    QueryNode node = planner.nodeForId("summary");
    assertTrue(((SummarizerConfig) node.config()).passThrough());
    assertEquals(2, planner.serializationSources().size());
    assertTrue(planner.serializationSources().contains("summary:m1"));
    assertTrue(planner.serializationSources().contains("m1:m1"));
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
        planner.nodeForId("summary")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("summary"),
        planner.nodeForId("mock")));
  }
  
  private SerdesOptions serdesConfigs(final List<String> filter) {
    final SerdesOptions config = mock(SerdesOptions.class);
    when(config.getFilter()).thenReturn(filter);
    return config;
  }
}
