// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultQueryContextFilter.PreAggConfig;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.DefaultNamedFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;

public class TestDefaultQueryContextFilter {
  private static final int BASE_TIMESTAMP = 1546300800;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private MockTSDB tsdb;
  private TimeSeriesQuery query;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    NUMERIC_CONFIG = (NumericInterpolatorConfig) 
        NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
  }
  
  @Before
  public void before() throws Exception {
    tsdb = new MockTSDB();
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP))
        .setEnd(Integer.toString(BASE_TIMESTAMP + 3600))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setQueryFilter(ChainFilter.newBuilder()
                .addFilter(TagValueLiteralOrFilter.newBuilder()
                    .setFilter("myapp")
                    .setKey("app")
                    .build())
                .addFilter(TagValueLiteralOrFilter.newBuilder()
                    .setFilter("den")
                    .setKey("colo")
                    .build())
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .setId("ds")
            .build())
        .addExecutionGraphNode(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("app")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("ds")
            .setId("gb")
            .build())
        .build();
    
    tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
        .setKey(DefaultQueryContextFilter.PREAGG_KEY)
        .setDefaultValue(Maps.newHashMap())
        .setDescription("TODO")
        .setType(DefaultQueryContextFilter.PREAGG_FILTERS)
        .setSource(this.getClass().toString())
        .isDynamic()
        .build());
  }
  
  @Test
  public void noConfigNoChange() throws Exception {
    DefaultQueryContextFilter filter = new DefaultQueryContextFilter();
    filter.initialize(tsdb, null).join();
    
    assertEquals(filter.filter(query, null, null), query);
  }
  
  @Test
  public void preAggSingleChainNested() throws Exception {
    PreAggConfig preagg = new PreAggConfig();
    preagg.startEpoch = BASE_TIMESTAMP;
    preagg.excludedAggTags = Lists.newArrayList("host");
    Map<String, PreAggConfig> config = Maps.newHashMap();
    config.put("sys", preagg);
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, config);
    
    DefaultQueryContextFilter filter = new DefaultQueryContextFilter();
    filter.initialize(tsdb, null).join();
    
    TimeSeriesQuery new_query = filter.filter(query, null, null);
    assertNotEquals(new_query, query);
    QueryFilter qf = null;
    for (final QueryNodeConfig node : new_query.getExecutionGraph()) {
      if (node.getId().equals("m1")) {
        qf = ((TimeSeriesDataSourceConfig) node).getFilter();
        break;
      }
    }
    assertEquals(3, ((ChainFilter) qf).getFilters().size());
    boolean found_preagg = false;
    for (final QueryFilter f : ((ChainFilter) qf).getFilters()) {
      TagValueLiteralOrFilter tf = (TagValueLiteralOrFilter) f;
      if (tf.getTagKey().equals("_aggregate")) {
        assertEquals("SUM", tf.getFilter());
        found_preagg = true;
      } else if (tf.getTagKey().equals("app")) {
        assertEquals("myapp", tf.getFilter());
      } else {
        assertEquals("den", tf.getFilter());
      }
    }
    assertTrue(found_preagg);
  }
  
  @Test
  public void preAggSingleChainNestedWithExclude() throws Exception {
    PreAggConfig preagg = new PreAggConfig();
    preagg.startEpoch = BASE_TIMESTAMP;
    preagg.excludedAggTags = Lists.newArrayList("host");
    Map<String, PreAggConfig> config = Maps.newHashMap();
    config.put("sys", preagg);
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, config);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP - 3600))
        .setEnd(Integer.toString(BASE_TIMESTAMP))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setQueryFilter(ChainFilter.newBuilder()
                .addFilter(TagValueLiteralOrFilter.newBuilder()
                    .setFilter("web01")
                    .setKey("host")
                    .build())
                .addFilter(TagValueLiteralOrFilter.newBuilder()
                    .setFilter("den")
                    .setKey("colo")
                    .build())
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .setId("ds")
            .build())
        .addExecutionGraphNode(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("app")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("ds")
            .setId("gb")
            .build())
        .build();
    
    DefaultQueryContextFilter filter = new DefaultQueryContextFilter();
    filter.initialize(tsdb, null).join();
    
    assertEquals(filter.filter(query, null, null), query);
  }
  
  @Test
  public void preAggTooEarly() throws Exception {
    PreAggConfig preagg = new PreAggConfig();
    preagg.startEpoch = BASE_TIMESTAMP;
    preagg.excludedAggTags = Lists.newArrayList("host");
    Map<String, PreAggConfig> config = Maps.newHashMap();
    config.put("sys", preagg);
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, config);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP - 3600))
        .setEnd(Integer.toString(BASE_TIMESTAMP))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setQueryFilter(ChainFilter.newBuilder()
                .addFilter(TagValueLiteralOrFilter.newBuilder()
                    .setFilter("myapp")
                    .setKey("app")
                    .build())
                .addFilter(TagValueLiteralOrFilter.newBuilder()
                    .setFilter("den")
                    .setKey("colo")
                    .build())
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .setId("ds")
            .build())
        .addExecutionGraphNode(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("app")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("ds")
            .setId("gb")
            .build())
        .build();
    
    DefaultQueryContextFilter filter = new DefaultQueryContextFilter();
    filter.initialize(tsdb, null).join();
    
    assertEquals(filter.filter(query, null, null), query);
  }

  @Test
  public void preAggSingleChainNamedFilter() throws Exception {
    PreAggConfig preagg = new PreAggConfig();
    preagg.startEpoch = BASE_TIMESTAMP;
    preagg.excludedAggTags = Lists.newArrayList("host");
    Map<String, PreAggConfig> config = Maps.newHashMap();
    config.put("sys", preagg);
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, config);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP))
        .setEnd(Integer.toString(BASE_TIMESTAMP + 3600))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .setId("ds")
            .build())
        .addExecutionGraphNode(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("app")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("ds")
            .setId("gb")
            .build())
        .addFilter(DefaultNamedFilter.newBuilder()
            .setId("f1")
            .setFilter(ChainFilter.newBuilder()
                .addFilter(TagValueLiteralOrFilter.newBuilder()
                    .setFilter("myapp")
                    .setKey("app")
                    .build())
                .addFilter(TagValueLiteralOrFilter.newBuilder()
                    .setFilter("den")
                    .setKey("colo")
                    .build())
                .build())
            .build())
        .build();
    
    DefaultQueryContextFilter filter = new DefaultQueryContextFilter();
    filter.initialize(tsdb, null).join();
    
    TimeSeriesQuery new_query = filter.filter(query, null, null);
    assertNotEquals(new_query, query);
    QueryFilter qf = null;
    for (final QueryNodeConfig node : new_query.getExecutionGraph()) {
      if (node.getId().equals("m1")) {
        qf = ((TimeSeriesDataSourceConfig) node).getFilter();
        assertNull(((TimeSeriesDataSourceConfig) node).getFilterId());
        break;
      }
    }
    assertEquals(3, ((ChainFilter) qf).getFilters().size());
    boolean found_preagg = false;
    for (final QueryFilter f : ((ChainFilter) qf).getFilters()) {
      TagValueLiteralOrFilter tf = (TagValueLiteralOrFilter) f;
      if (tf.getTagKey().equals("_aggregate")) {
        assertEquals("SUM", tf.getFilter());
        found_preagg = true;
      } else if (tf.getTagKey().equals("app")) {
        assertEquals("myapp", tf.getFilter());
      } else {
        assertEquals("den", tf.getFilter());
      }
    }
    assertTrue(found_preagg);
  }
}
