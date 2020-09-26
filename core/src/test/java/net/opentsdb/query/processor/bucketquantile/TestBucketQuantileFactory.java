// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.bucketquantile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.BaseTimeSeriesDataSourceConfig;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;

public class TestBucketQuantileFactory {

  private static MockTSDB TSDB;
  private static TimeSeriesDataSourceFactory STORE_FACTORY;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static QueryNode SINK;
  private static List<TimeSeriesDataSource> STORE_NODES;
  private static TimeSeriesDataSourceFactory S1;
  
  private QueryPipelineContext context;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    STORE_FACTORY = mock(TimeSeriesDataSourceFactory.class);
    SINK = mock(QueryNode.class);
    STORE_NODES = Lists.newArrayList();
    S1 = mock(TimeSeriesDataSourceFactory.class);
    
    TSDB.registry = new DefaultRegistry(TSDB);
    ((DefaultRegistry) TSDB.registry).initialize(true);
    ((DefaultRegistry) TSDB.registry).registerPlugin(
        TimeSeriesDataSourceFactory.class, null, (TSDBPlugin) STORE_FACTORY);
    ((DefaultRegistry) TSDB.registry).registerPlugin(
        TimeSeriesDataSourceFactory.class, "s1", (TSDBPlugin) S1);
    
    when(S1.newNode(any(QueryPipelineContext.class), 
        any(QueryNodeConfig.class)))
      .thenAnswer(new Answer<QueryNode>() {
        @Override
        public QueryNode answer(InvocationOnMock invocation) throws Throwable {
          final TimeSeriesDataSource node = mock(TimeSeriesDataSource.class);
          when(node.initialize(null)).thenReturn(Deferred.fromResult(null));
          when(node.config()).thenReturn((QueryNodeConfig) invocation.getArguments()[1]);
          STORE_NODES.add(node);
          return node;
        }
      });
    when(STORE_FACTORY.newNode(any(QueryPipelineContext.class), 
        any(QueryNodeConfig.class)))
      .thenAnswer(new Answer<QueryNode>() {
        @Override
        public QueryNode answer(InvocationOnMock invocation) throws Throwable {
          final TimeSeriesDataSource node = mock(TimeSeriesDataSource.class);
          when(node.initialize(null)).thenReturn(Deferred.fromResult(null));
          when(node.config()).thenReturn((QueryNodeConfig) invocation.getArguments()[1]);
          STORE_NODES.add(node);
          return node;
        }
      });
    when(STORE_FACTORY.idType()).thenAnswer(new Answer<TypeToken<? extends TimeSeriesId>>() {
      @Override
      public TypeToken<? extends TimeSeriesId> answer(
          InvocationOnMock invocation) throws Throwable {
        return Const.TS_STRING_ID;
      }
    });
    when(S1.idType()).thenAnswer(new Answer<TypeToken<? extends TimeSeriesId>>() {
      @Override
      public TypeToken<? extends TimeSeriesId> answer(
          InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(S1.id()).thenReturn("s1");
    
    when(STORE_FACTORY.parseConfig(any(ObjectMapper.class), any(TSDB.class), any(JsonNode.class)))
    .thenAnswer(new Answer<QueryNodeConfig>() {
      @Override
      public QueryNodeConfig answer(InvocationOnMock invocation)
          throws Throwable {
        DefaultTimeSeriesDataSourceConfig.Builder builder = DefaultTimeSeriesDataSourceConfig.newBuilder();
        
        DefaultTimeSeriesDataSourceConfig.parseConfig
            ((ObjectMapper) invocation.getArguments()[0], 
                invocation.getArgumentAt(1, TSDB.class), 
                (JsonNode) invocation.getArguments()[2],
                (BaseTimeSeriesDataSourceConfig.Builder) builder);
        return builder.build();
      }
    });
    
    NUMERIC_CONFIG = (NumericInterpolatorConfig) 
        NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
    
    QueryNodeConfig sink_config = mock(QueryNodeConfig.class);
    when(sink_config.getId()).thenReturn("SINK");
    when(SINK.config()).thenReturn(sink_config);
  }
  
  @Test
  public void setupGraphDirect() throws Exception {
    BucketQuantileConfig config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setOverflow("m4")
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .setUnderflow("m5")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("q")
        .addSource("m1")
        .addSource("m2")
        .addSource("m3")
        .addSource("m4")
        .addSource("m5")
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(config)
        .addExecutionGraphNode(metricNode("m1", "m_0_250"))
        .addExecutionGraphNode(metricNode("m2", "m_250_500"))
        .addExecutionGraphNode(metricNode("m3", "m_500_1000"))
        .addExecutionGraphNode(metricNode("m4", "m_over"))
        .addExecutionGraphNode(metricNode("m5", "m_under"))
        .setStart("1h-ago")
        .build();
    context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.queryContext()).thenReturn(mock(QueryContext.class));
    when(context.query()).thenReturn(query);
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    config = (BucketQuantileConfig) planner.configNodeForId("q");
    assertTrue(config.histogramMetrics().contains("m_0_250"));
    assertTrue(config.histogramIds().contains(new DefaultQueryResultId("m1", "m1")));
    assertTrue(config.histogramMetrics().contains("m_250_500"));
    assertTrue(config.histogramIds().contains(new DefaultQueryResultId("m2", "m2")));
    assertTrue(config.histogramMetrics().contains("m_500_1000"));
    assertTrue(config.histogramIds().contains(new DefaultQueryResultId("m3", "m3")));
    assertEquals("m_over", config.overflowMetric());
    assertEquals(new DefaultQueryResultId("m4", "m4"), config.overflowId());
    assertEquals("m_under", config.underflowMetric());
    assertEquals(new DefaultQueryResultId("m5", "m5"), config.underflowId());
  }
  
  @Test
  public void setupGraphThroughNodes() throws Exception {
    BucketQuantileConfig config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setOverflow("m4")
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .setUnderflow("m5")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("q")
        .addSource("gb_m1")
        .addSource("gb_m2")
        .addSource("gb_m3")
        .addSource("gb_m4")
        .addSource("gb_m5")
        .build();
    
    SemanticQuery.Builder builder = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(config)
        .setStart("1h-ago");
    addGBandDS(builder, "m1", "m_0_250");
    addGBandDS(builder, "m2", "m_250_500");
    addGBandDS(builder, "m3", "m_500_1000");
    addGBandDS(builder, "m4", "m_over");
    addGBandDS(builder, "m5", "m_under");
    
    SemanticQuery query = builder.build();
    context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.queryContext()).thenReturn(mock(QueryContext.class));
    when(context.query()).thenReturn(query);
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    config = (BucketQuantileConfig) planner.configNodeForId("q");
    assertTrue(config.histogramMetrics().contains("m_0_250"));
    assertTrue(config.histogramIds().contains(new DefaultQueryResultId("gb_m1", "m1")));
    assertTrue(config.histogramMetrics().contains("m_250_500"));
    assertTrue(config.histogramIds().contains(new DefaultQueryResultId("gb_m2", "m2")));
    assertTrue(config.histogramMetrics().contains("m_500_1000"));
    assertTrue(config.histogramIds().contains(new DefaultQueryResultId("gb_m3", "m3")));
    assertEquals("m_over", config.overflowMetric());
    assertEquals(new DefaultQueryResultId("gb_m4", "m4"), config.overflowId());
    assertEquals("m_under", config.underflowMetric());
    assertEquals(new DefaultQueryResultId("gb_m5", "m5"), config.underflowId());
  }
  
  @Test
  public void setupGraphMissingNodes() throws Exception {
    BucketQuantileConfig config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setOverflow("m4")
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .setUnderflow("m5")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("q")
        //.addSource("gb_m1")
        .addSource("gb_m2")
        .addSource("gb_m3")
        .addSource("gb_m4")
        .addSource("gb_m5")
        .build();
    
    SemanticQuery.Builder builder = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(config)
        .setStart("1h-ago");
    addGBandDS(builder, "m1", "m_0_250");
    addGBandDS(builder, "m2", "m_250_500");
    addGBandDS(builder, "m3", "m_500_1000");
    addGBandDS(builder, "m4", "m_over");
    addGBandDS(builder, "m5", "m_under");
    
    SemanticQuery query = builder.build();
    context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.queryContext()).thenReturn(mock(QueryContext.class));
    when(context.query()).thenReturn(query);
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    try {
      planner.plan(null).join();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    
    // overflow
    config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setOverflow("m4")
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .setUnderflow("m5")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("q")
        .addSource("gb_m1")
        .addSource("gb_m2")
        .addSource("gb_m3")
        //.addSource("gb_m4")
        .addSource("gb_m5")
        .build();
    
    builder = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(config)
        .setStart("1h-ago");
    addGBandDS(builder, "m1", "m_0_250");
    addGBandDS(builder, "m2", "m_250_500");
    addGBandDS(builder, "m3", "m_500_1000");
    addGBandDS(builder, "m4", "m_over");
    addGBandDS(builder, "m5", "m_under");
    
    query = builder.build();
    context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.queryContext()).thenReturn(mock(QueryContext.class));
    when(context.query()).thenReturn(query);
    planner = new DefaultQueryPlanner(context, SINK);
    try {
      planner.plan(null).join();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    
    // underflow
    config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setOverflow("m4")
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .setUnderflow("m5")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("q")
        .addSource("gb_m1")
        .addSource("gb_m2")
        .addSource("gb_m3")
        .addSource("gb_m4")
        //.addSource("gb_m5")
        .build();
    
    builder = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(config)
        .setStart("1h-ago");
    addGBandDS(builder, "m1", "m_0_250");
    addGBandDS(builder, "m2", "m_250_500");
    addGBandDS(builder, "m3", "m_500_1000");
    addGBandDS(builder, "m4", "m_over");
    addGBandDS(builder, "m5", "m_under");
    
    query = builder.build();
    context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.queryContext()).thenReturn(mock(QueryContext.class));
    when(context.query()).thenReturn(query);
    planner = new DefaultQueryPlanner(context, SINK);
    try {
      planner.plan(null).join();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
  }
  
  QueryNodeConfig metricNode(final String id, final String metric) {
    return DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(metric)
            .build())
        .setId(id)
        .build();
  }
  
  void addGBandDS(final SemanticQuery.Builder builder, 
                  final String id, 
                  final String metric) {
    builder.addExecutionGraphNode(metricNode(id, metric));
    builder.addExecutionGraphNode(DownsampleConfig.newBuilder()
        .setAggregator("avg")
        .setInterval("1m")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .addSource(id)
        .setId("ds_" + id)
        .build());
    builder.addExecutionGraphNode(GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .addSource("ds_" + id)
        .setId("gb_" + id)
        .build());
  }
}
