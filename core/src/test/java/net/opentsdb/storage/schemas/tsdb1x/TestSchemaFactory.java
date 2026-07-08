// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.storage.schemas.tsdb1x;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.*;

import java.util.List;


import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.query.*;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.processor.timeshift.TimeShiftConfig;
import net.opentsdb.query.processor.timeshift.TimeShiftFactory;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.DefaultRollupInterval;
import net.opentsdb.stats.Span;
import net.opentsdb.uid.UniqueIdType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

public class TestSchemaFactory extends SchemaBase {
  
  private Tsdb1xDataStore store;
  private Tsdb1xQueryNode node;
  private MockedConstruction<Schema> mockedSchema;
  
  @Before
  public void before() throws Exception {
    store = mock(Tsdb1xDataStore.class);
    node = mock(Tsdb1xQueryNode.class);
    
    when(store.newNode(any(QueryPipelineContext.class), any(TimeSeriesDataSourceConfig.class)))
      .thenAnswer(new Answer<QueryNode>() {
        @Override
        public Tsdb1xQueryNode answer(InvocationOnMock invocation) throws Throwable {
          when(node.config()).thenReturn((TimeSeriesDataSourceConfig) invocation.getArguments()[1]);
          when(node.initialize(nullable(Span.class))).thenReturn(Deferred.fromResult(null));
          return node;
        }
      });
    
    mockedSchema = Mockito.mockConstruction(Schema.class,
        (mock, context) -> {
            when(mock.dataStore()).thenReturn(store);
        });
  }

  @After
  public void after() {
      if (mockedSchema != null) {
          mockedSchema.close();
      }
  }
  
  @Test
  public void ctor() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    assertNull(factory.id());
    assertEquals(SchemaFactory.TYPE, factory.type());
    assertEquals(0, mockedSchema.constructed().size());
    
    assertNull(factory.initialize(tsdb, null).join(1));
    assertEquals(1, mockedSchema.constructed().size());
  }
  
  @Test
  public void newNode() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    factory.initialize(tsdb, null).join(1);
    
    assertSame(node, factory.newNode(mock(QueryPipelineContext.class), 
        mock(TimeSeriesDataSourceConfig.class)));
  }
  
  @Test
  public void newNodePadding() throws Exception {
    TimeSeriesDataSourceConfig config =
        (TimeSeriesDataSourceConfig) DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setSummaryInterval("1h")
        .addSummaryAggregation("sum")
        .setId("m1")
        .build();
    
    SchemaFactory factory = new SchemaFactory();
    factory.initialize(tsdb, null).join(1);
    factory.newNode(mock(QueryPipelineContext.class), config);
    TimeSeriesDataSourceConfig new_config = (TimeSeriesDataSourceConfig) node.config();
    assertEquals("1h", new_config.getSummaryInterval());
    assertEquals(1, new_config.getSummaryAggregations().size());
    assertTrue(new_config.getSummaryAggregations().contains("sum"));
  }
  
  @Test
  public void newNodeRollups() throws Exception {
    TimeSeriesDataSourceConfig config =
        (TimeSeriesDataSourceConfig) DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setSummaryInterval("1h")
        .addSummaryAggregation("sum")
        .setId("m1")
        .build();

    // Build real DefaultRollupConfig. No need for a spy/mock.
    final DefaultRollupConfig rollup_config = DefaultRollupConfig.newBuilder()
        .addAggregationId("sum", 0)
        .addInterval(DefaultRollupInterval.builder()
            .setTable("tsdb-rollup-1h")
            .setPreAggregationTable("tsdb-rollup-preagg-1h")
            .setInterval("1h")
            .setRowSpan("1d"))
        .addInterval(DefaultRollupInterval.builder()
            .setTable("tsdb-rollup-30m")
            .setPreAggregationTable("tsdb-rollup-preagg-30m")
            .setInterval("30m")
            .setRowSpan("1d"))
        .build();
    
    SchemaFactory factory = new SchemaFactory();
    factory.registerConfigs(tsdb);
    tsdb.config.override(factory.getConfigKey(
        SchemaFactory.ROLLUP_ENABLED_KEY), true);
    tsdb.config.override(factory.getConfigKey(
        SchemaFactory.ROLLUP_KEY), rollup_config);
    
    factory.initialize(tsdb, null).join(1);
    factory.newNode(mock(QueryPipelineContext.class), config);

    TimeSeriesDataSourceConfig new_config = (TimeSeriesDataSourceConfig) node.config();
    assertEquals("1h", new_config.getSummaryInterval());
    assertEquals(1, new_config.getSummaryAggregations().size());
    assertTrue(new_config.getSummaryAggregations().contains("sum"));
    assertEquals(2, new_config.getRollupIntervals().size());
    assertTrue(new_config.getRollupIntervals().contains("1h"));
    assertTrue(new_config.getRollupIntervals().contains("30m"));
  }
  
  @Test
  public void resolveByteId() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    factory.initialize(tsdb, null).join(1);
    
    factory.resolveByteId(mock(TimeSeriesByteId.class), null);
    verify(factory.schema, times(1)).resolveByteId(
        any(TimeSeriesByteId.class), nullable(Span.class));
  }
  
  @Test
  public void encodeJoinKeys() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    factory.initialize(tsdb, null).join(1);
    
    factory.encodeJoinKeys(Lists.newArrayList(), null);
    verify(factory.schema, times(1)).getIds(
        eq(UniqueIdType.TAGK), any(List.class), nullable(Span.class));
  }
  
  @Test
  public void encodeJoinMetrics() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    factory.initialize(tsdb, null).join(1);
    
    factory.encodeJoinMetrics(Lists.newArrayList(), null);
    verify(factory.schema, times(1)).getIds(
        eq(UniqueIdType.METRIC), any(List.class), nullable(Span.class));
  }

  @Test
  public void setupWithOutOffsets() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    factory.initialize(tsdb, null).join(1);

    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig)
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .setId("m1")
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .addExecutionGraphNode(config)
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .build();
    
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.query()).thenReturn(query);
    when(context.tsdb()).thenReturn(tsdb);
    when(tsdb.getRegistry().getQueryNodeFactory(null)).thenReturn(factory);
    when(tsdb.getRegistry().getQueryNodeFactory("timeshift"))
      .thenReturn(new TimeShiftFactory());
    QueryNode sink = mock(QueryNode.class);
    DefaultQueryPlanner plan = new DefaultQueryPlanner(context, sink);
    plan.plan(null).join();
    
    assertEquals(3, plan.configGraph().nodes().size());
  }
  
  @Test
  public void setupWithOffsets() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    factory.initialize(tsdb, null).join(1);

    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig)
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .setTimeShiftInterval("1d")
        .setId("m1")
        .build();

    SemanticQuery query = SemanticQuery.newBuilder()
        .addExecutionGraphNode(config)
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .build();

    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.query()).thenReturn(query);
    when(context.tsdb()).thenReturn(tsdb);
    when(tsdb.getRegistry().getQueryNodeFactory(null)).thenReturn(factory);
    when(tsdb.getRegistry().getQueryNodeFactory("timeshift"))
      .thenReturn(new TimeShiftFactory());
    QueryNode sink = mock(QueryNode.class);
    DefaultQueryPlanner plan = new DefaultQueryPlanner(context, sink);
    plan.plan(null).join();

    QueryNodeConfig shift = plan.configNodeForId("m1_timeShift");
    assertTrue(shift instanceof TimeShiftConfig);

    assertEquals(4, plan.configGraph().nodes().size());
    QueryNodeConfig node = plan.configNodeForId("m1");
    assertTrue(plan.configGraph().hasEdgeConnecting(shift, node));
  }
}
