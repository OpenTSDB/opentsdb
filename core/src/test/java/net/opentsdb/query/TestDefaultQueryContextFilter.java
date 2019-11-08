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

import com.google.common.collect.Maps;

import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.PreAggConfig.MetricPattern;
import net.opentsdb.query.PreAggConfig.TagsAndAggs;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.DefaultNamedFilter;
import net.opentsdb.query.filter.ExplicitTagsFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.filter.TagValueWildcardFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;

public class TestDefaultQueryContextFilter {
  private static final int BASE_TIMESTAMP = 1546300800;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static Map<String, PreAggConfig> PRE_AGG_CONFIG;
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
    
    PRE_AGG_CONFIG = Maps.newHashMap();
    
    PreAggConfig config = PreAggConfig.newBuilder()
        .addMetric(MetricPattern.newBuilder()
              .setMetric("sys.*")
              .addAggs(TagsAndAggs.newBuilder()
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("app")
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("colo")
                  .addTag("app")
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .build())
        .addMetric(MetricPattern.newBuilder()
              .setMetric("net.*")
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("app")
                  .addAgg("SUM", BASE_TIMESTAMP + 3600)
                  .addAgg("COUNT", BASE_TIMESTAMP + 3600)
                  .build())
              .addAggs(TagsAndAggs.newBuilder()
                  .addTag("colo")
                  .addTag("app")
                  .addTag("dept")
                  .addAgg("SUM", BASE_TIMESTAMP)
                  .addAgg("COUNT", BASE_TIMESTAMP)
                  .build())
              .build())
        .build();
    PRE_AGG_CONFIG.put("MyNS", config);
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
                .setMetric("MyNS.sys.cpu.user")
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
        .setType(PreAggConfig.TYPE_REF)
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
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, PRE_AGG_CONFIG);
    
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
    
    ExplicitTagsFilter ef = (ExplicitTagsFilter) qf;
    assertEquals(3, ((ChainFilter) ef.getFilter()).getFilters().size());
    boolean found_preagg = false;
    for (final QueryFilter f : ((ChainFilter) ef.getFilter()).getFilters()) {
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
  public void preAggTagNotAgged() throws Exception {
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, PRE_AGG_CONFIG);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP - 3600))
        .setEnd(Integer.toString(BASE_TIMESTAMP))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("MyNS.sys.cpu.user")
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
        assertEquals("raw", tf.getFilter());
        found_preagg = true;
      } else if (tf.getTagKey().equals("host")) {
        assertEquals("web01", tf.getFilter());
      } else {
        assertEquals("den", tf.getFilter());
      }
    }
    assertTrue(found_preagg);
  }
  
  @Test
  public void preAggTooEarly() throws Exception {
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, PRE_AGG_CONFIG);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP - 3600))
        .setEnd(Integer.toString(BASE_TIMESTAMP))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("MyNS.sys.cpu.user")
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
        assertEquals("raw", tf.getFilter());
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
  public void preAggNoSuchAgg() throws Exception {
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, PRE_AGG_CONFIG);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP))
        .setEnd(Integer.toString(BASE_TIMESTAMP + 3600))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("MyNS.sys.cpu.user")
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
            .setAggregator("p95")
            .addTagKey("app")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("ds")
            .setId("gb")
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
        break;
      }
    }
    assertEquals(3, ((ChainFilter) qf).getFilters().size());
    boolean found_preagg = false;
    for (final QueryFilter f : ((ChainFilter) qf).getFilters()) {
      TagValueLiteralOrFilter tf = (TagValueLiteralOrFilter) f;
      if (tf.getTagKey().equals("_aggregate")) {
        assertEquals("raw", tf.getFilter());
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
  public void preAggSingleChainNamedFilter() throws Exception {
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, PRE_AGG_CONFIG);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP))
        .setEnd(Integer.toString(BASE_TIMESTAMP + 3600))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("MyNS.sys.cpu.user")
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
    ExplicitTagsFilter ef = (ExplicitTagsFilter) qf;
    assertEquals(3, ((ChainFilter) ef.getFilter()).getFilters().size());
    boolean found_preagg = false;
    for (final QueryFilter f : ((ChainFilter) ef.getFilter()).getFilters()) {
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
  public void preAggNoRulesForNamespace() throws Exception {
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, PRE_AGG_CONFIG);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP - 3600))
        .setEnd(Integer.toString(BASE_TIMESTAMP))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("MyNS2.sys.cpu.user")
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
  public void preAggNoRulesForMetric() throws Exception {
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, PRE_AGG_CONFIG);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP - 3600))
        .setEnd(Integer.toString(BASE_TIMESTAMP))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("MyNS.anApp.latency")
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
  public void preAggOnlyGroupBy() throws Exception {
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, PRE_AGG_CONFIG);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP))
        .setEnd(Integer.toString(BASE_TIMESTAMP + 3600))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("MyNS.sys.cpu.user")
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
    ExplicitTagsFilter ef = (ExplicitTagsFilter) qf;
    TagValueLiteralOrFilter tf = (TagValueLiteralOrFilter) ef.getFilter();
    assertEquals("_aggregate", tf.getTagKey());
    assertEquals("SUM", tf.getFilter());
  }
  
  @Test
  public void preAggOnlyGroupByNoTagInRule() throws Exception {
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, PRE_AGG_CONFIG);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP))
        .setEnd(Integer.toString(BASE_TIMESTAMP + 3600))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("MyNS.sys.cpu.user")
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
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("ds")
            .setId("gb")
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
    TagValueLiteralOrFilter tf = (TagValueLiteralOrFilter) qf;
    assertEquals("_aggregate", tf.getTagKey());
    assertEquals("raw", tf.getFilter());
  }
  
  @Test
  public void preAggOnlyGroupByAll() throws Exception {
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, PRE_AGG_CONFIG);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP))
        .setEnd(Integer.toString(BASE_TIMESTAMP + 3600))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("MyNS.sys.cpu.user")
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
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("ds")
            .setId("gb")
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
    
    ExplicitTagsFilter ef = (ExplicitTagsFilter) qf;
    TagValueLiteralOrFilter tf = (TagValueLiteralOrFilter) ef.getFilter();
    assertEquals("_aggregate", tf.getTagKey());
    assertEquals("SUM", tf.getFilter());
  }
  
  @Test
  public void preAggOnlyGroupByAllNoAgg() throws Exception {
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, PRE_AGG_CONFIG);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP))
        .setEnd(Integer.toString(BASE_TIMESTAMP + 3600))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("MyNS.net.if.out")
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
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("ds")
            .setId("gb")
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
    TagValueLiteralOrFilter tf = (TagValueLiteralOrFilter) qf;
    assertEquals("_aggregate", tf.getTagKey());
    assertEquals("raw", tf.getFilter());
  }

  @Test
  public void preAggNoGroupBy() throws Exception {
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, PRE_AGG_CONFIG);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP))
        .setEnd(Integer.toString(BASE_TIMESTAMP + 3600))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("MyNS.sys.cpu.user")
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
    TagValueLiteralOrFilter tf = (TagValueLiteralOrFilter) qf;
    assertEquals("_aggregate", tf.getTagKey());
    assertEquals("raw", tf.getFilter());
  }

  @Test
  public void preAggAddsTag() throws Exception {
    tsdb.config.addOverride(DefaultQueryContextFilter.PREAGG_KEY, PRE_AGG_CONFIG);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(BASE_TIMESTAMP))
        .setEnd(Integer.toString(BASE_TIMESTAMP + 3600))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("MyNS.net.if.out")
                .build())
            .setQueryFilter(ChainFilter.newBuilder()
                .addFilter(TagValueLiteralOrFilter.newBuilder()
                    .setFilter("myapp")
                    .setKey("app")
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
            .addTagKey("dept")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("ds")
            .setId("gb")
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
        break;
      }
    }
    
    ExplicitTagsFilter ef = (ExplicitTagsFilter) qf;
    ChainFilter cf = (ChainFilter) ef.getFilter();
    assertEquals(2, cf.getFilters().size());
    
    ChainFilter cf2 = (ChainFilter) cf.getFilters().get(0);
    assertEquals(2, cf2.getFilters().size());
    
    TagValueLiteralOrFilter tf = (TagValueLiteralOrFilter) cf2.getFilters().get(0);
    assertEquals("SUM", tf.getFilter());
    assertEquals("_aggregate", tf.getTagKey());
    
    tf = (TagValueLiteralOrFilter) cf2.getFilters().get(1);
    assertEquals("myapp", tf.getFilter());
    assertEquals("app", tf.getTagKey());
    
    TagValueWildcardFilter wf = (TagValueWildcardFilter) cf.getFilters().get(1);
    assertEquals("*", wf.getFilter());
    assertEquals("colo", wf.getTagKey());
  }
  
}
