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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.List;

import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.uid.UniqueIdType;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SchemaFactory.class })
public class TestSchemaFactory extends SchemaBase {

  private MockTSDB tsdb;
  private Tsdb1xDataStore store;
  private QueryNode node;
  
  @Before
  public void before() throws Exception {
    tsdb = new MockTSDB();
    store = mock(Tsdb1xDataStore.class);
    node = mock(QueryNode.class);
    
    when(store.newNode(any(QueryPipelineContext.class), 
        any(QueryNodeConfig.class)))
      .thenReturn(node);
    
    PowerMockito.whenNew(Schema.class).withAnyArguments()
      .thenAnswer(new Answer<Schema>() {
      @Override
      public Schema answer(InvocationOnMock invocation) throws Throwable {
        final Schema schema = mock(Schema.class);
        when(schema.dataStore()).thenReturn(store);
        return schema;
      }
    });
  }
  
  @Test
  public void ctor() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    assertNull(factory.id());
    assertEquals(SchemaFactory.TYPE, factory.type());
    PowerMockito.verifyNew(Schema.class, never());
    
    assertNull(factory.initialize(tsdb, null).join(1));
    PowerMockito.verifyNew(Schema.class);
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
    
    QueryNodeConfig[] configs = new QueryNodeConfig[1];
    when(store.newNode(any(QueryPipelineContext.class), 
        any(QueryNodeConfig.class)))
      .thenAnswer(new Answer<QueryNode>() {
        @Override
        public QueryNode answer(InvocationOnMock invocation) throws Throwable {
          configs[0] = (QueryNodeConfig) invocation.getArguments()[1];
          return null;
        }
      });
    
    SchemaFactory factory = new SchemaFactory();
    factory.initialize(tsdb, null).join(1);
    factory.newNode(mock(QueryPipelineContext.class), config);
    TimeSeriesDataSourceConfig new_config = (TimeSeriesDataSourceConfig) configs[0];
    assertEquals("1h", new_config.getPrePadding());
    assertEquals("30m", new_config.getPostPadding());
    assertEquals("1h", new_config.getSummaryInterval());
    assertEquals(1, new_config.getSummaryAggregations().size());
    assertTrue(new_config.getSummaryAggregations().contains("sum"));
    assertTrue(new_config.getRollupIntervals().isEmpty());
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
    
    DefaultRollupConfig rollup_config = mock(DefaultRollupConfig.class);
    when(rollup_config.getPossibleIntervals("1h"))
      .thenReturn(Lists.newArrayList("1h", "30m"));
    
    QueryNodeConfig[] configs = new QueryNodeConfig[1];
    when(store.newNode(any(QueryPipelineContext.class), 
        any(QueryNodeConfig.class)))
      .thenAnswer(new Answer<QueryNode>() {
        @Override
        public QueryNode answer(InvocationOnMock invocation) throws Throwable {
          configs[0] = (QueryNodeConfig) invocation.getArguments()[1];
          return null;
        }
      });
    
    SchemaFactory factory = new SchemaFactory();
    factory.registerConfigs(tsdb);
    tsdb.config.override(factory.getConfigKey(
        SchemaFactory.ROLLUP_ENABLED_KEY), true);
    tsdb.config.override(factory.getConfigKey(
        SchemaFactory.ROLLUP_KEY), rollup_config);
    
    factory.initialize(tsdb, null).join(1);
    factory.newNode(mock(QueryPipelineContext.class), config);
    
    TimeSeriesDataSourceConfig new_config = (TimeSeriesDataSourceConfig) configs[0];
    assertEquals("1h", new_config.getPrePadding());
    assertEquals("30m", new_config.getPostPadding());
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
        any(TimeSeriesByteId.class), any(Span.class));
  }
  
  @Test
  public void encodeJoinKeys() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    factory.initialize(tsdb, null).join(1);
    
    factory.encodeJoinKeys(Lists.newArrayList(), null);
    verify(factory.schema, times(1)).getIds(
        eq(UniqueIdType.TAGK), any(List.class), any(Span.class));
  }
  
  @Test
  public void encodeJoinMetrics() throws Exception {
    SchemaFactory factory = new SchemaFactory();
    factory.initialize(tsdb, null).join(1);
    
    factory.encodeJoinMetrics(Lists.newArrayList(), null);
    verify(factory.schema, times(1)).getIds(
        eq(UniqueIdType.METRIC), any(List.class), any(Span.class));
  }
}
